"""One embedded Escapement owner and Arrow transport helpers.

All durable timeseries state and execution live in Escapement. This module has
no stream registry, SQL connections, revision ledger, or computation scheduler.
"""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
from threading import Condition, Event, Thread

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from escapement_next import Closed, Engine, EscapementError, NotFound, Receipt, Refused

from acquirium.internals.models import MAX_REQUEST_ROWS
from acquirium.Storage.values import split_values

SCHEMA = pa.schema(
    [
        ("ts", pa.timestamp("ns", tz="UTC")),
        ("value", pa.float64()),
        ("uri", pa.string()),
    ]
)
# Text and coalesced reads return the value column as strings.
TEXT_SCHEMA = SCHEMA.set(1, pa.field("value", pa.string()))
logger = logging.getLogger(__name__)


def schema(mode: str) -> pa.Schema:
    return SCHEMA if mode == "numeric" else TEXT_SCHEMA


def _selected(times, values, texts, mode: str, stream: str) -> pa.RecordBatch:
    """The readings a read in ``mode`` returns, from an Escapement batch:
    numbers only, text only, or every reading as text ("coalesce")."""
    if mode == "coalesce":
        # Python's str() keeps numbers as they read elsewhere ("20.0", not "20").
        values = pa.array(
            [str(n) if s is None else s for n, s in zip(values.to_pylist(), texts.to_pylist())],
            type=pa.string(),
        )
    else:
        if mode == "text":
            values = texts
        keep = pc.is_valid(values)
        times, values = pc.filter(times, keep), pc.filter(values, keep)
    return pa.RecordBatch.from_arrays(
        [
            times.cast(SCHEMA.field("ts").type),
            values,
            pa.repeat(pa.scalar(stream, pa.string()), len(times)),
        ],
        schema=schema(mode),
    )


def time_ns(value: int | datetime | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(
            "timestamps must be UTC nanoseconds or timezone-aware datetimes"
        )
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("timestamp exceeds signed UTC nanoseconds")
        return value
    if isinstance(value, str):
        if datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        series = pl.Series([value]).str.to_datetime(
            time_unit="ns", time_zone="UTC", strict=True
        )
        return series.cast(pl.Int64).item()
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return (
        pc.cast(
            pa.array([value], type=pa.timestamp("us", tz="UTC")),
            pa.timestamp("ns", tz="UTC"),
            safe=True,
        )
        .cast(pa.int64())[0]
        .as_py()
    )


def _stop(stop, drain, failure):
    try:
        stop(drain=drain)
    except BaseException as error:
        failure.append(error)
    finally:
        del stop  # The error's traceback must not keep the owner alive.


def _read_range(start, end, limit, order, batch_size, mode):
    if mode not in {"numeric", "text", "coalesce"}:
        raise ValueError("mode must be numeric, text or coalesce")
    if order not in {"asc", "desc"}:
        raise ValueError("order must be asc or desc")
    if limit is not None and (isinstance(limit, bool) or limit < 0):
        raise ValueError("limit must be nonnegative")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    lo, hi = time_ns(start), time_ns(end)
    if lo is not None and hi is not None and lo > hi:
        raise ValueError("start must not exceed end")
    return lo, hi


def _iso(value):
    if value is None:
        return None
    return (
        pl.Series([value], dtype=pl.Int64)
        .cast(pl.Datetime("ns", "UTC"))
        .dt.to_string("%Y-%m-%dT%H:%M:%S%.9f%:z")
        .item()
    )


class Core:
    """Acquirium's Arrow transport for Escapement."""

    def __init__(self, path: str | Path, *, history_epochs: int | None = None, **settings):
        if history_epochs is not None and (type(history_epochs) is not int or history_epochs < 1):
            raise ValueError("history_epochs must be a positive integer")
        self.engine = Engine.open(path, **settings)
        self._stopping = None
        self._stop_failure = []
        self.pending_at_close = 0
        # Orders submissions against close. Submissions do not exclude each
        # other: Escapement admits concurrent ones in one transaction.
        self._state = Condition()
        self._submitting = 0
        self._closing = False
        self._retention_stop = Event()
        self._retention_thread = None
        if history_epochs is not None:
            self._retention_thread = Thread(
                target=self._prune_history, args=(history_epochs,), name="escapement-retention", daemon=True,
            )
            self._retention_thread.start()

    def _prune_history(self, history_epochs):
        while not self._retention_stop.wait(1):
            try:
                status = self.engine.status()
                before = status["published"] - history_epochs + 1
                if before > status["retention"]:
                    self.engine.prune(before)
            except Closed:
                return
            except EscapementError:
                logger.exception("could not advance Escapement retention")

    def ingest(self, source: str, request_id: str, frame: pl.DataFrame, *, replace=()):
        """Submit ``ref_uri``, timezone-aware ``ts`` and ``value`` rows, or rows
        with ``operation`` ``"delete"``. Each value is stored as its row's
        ``value_kind`` (numeric when absent; see ``split_values``).

        Each stream in ``replace`` first loses all its readings, in the same
        request, so it holds exactly the rows supplied for it."""
        replace = sorted(set(replace))
        if frame.height + len(replace) > MAX_REQUEST_ROWS:
            raise ValueError(
                f"a request may contain at most {MAX_REQUEST_ROWS:,} rows"
            )
        dtype = frame.schema["ts"]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise ValueError("ts must be a timezone-aware datetime column")
        if "operation" not in frame.columns:
            frame = frame.with_columns(pl.lit("upsert").alias("operation"))
        if "value_kind" not in frame.columns:
            frame = frame.with_columns(pl.lit("numeric").alias("value_kind"))
        # Splitting deletes from values must not change which mutation wins
        # when the request addresses the same key more than once.
        frame = frame.select(
            "ref_uri", "ts", "operation", "value", "value_kind"
        ).with_row_index("_row")
        deleted = frame["operation"].eq("delete")
        rows = pl.concat([
            frame.filter(deleted).select(
                "_row", "ref_uri", "ts", "operation",
                pl.lit(None, pl.Float64).alias("number"), pl.lit(None, pl.String).alias("text"),
            ),
            split_values(frame.filter(~deleted).select(
                "_row", "ref_uri", "ts", "operation", "value", "value_kind"
            )),
        ]).sort("_row").drop("_row")
        return self._write(source, request_id, rows, replace)

    def _submit(self, submit):
        """Keep close from overtaking a running submission."""
        with self._state:
            if self._closing:
                raise Closed("engine is closed")
            self._submitting += 1
        try:
            return submit()
        finally:
            with self._state:
                self._submitting -= 1
                if not self._submitting:
                    self._state.notify_all()

    def activate(self, request_id: str, plan):
        return self._submit(lambda: self.engine.activate(request_id, plan))


    def _write(self, source, request_id, frame, replace):
        # Keep the last surviving operation at each stream and time.
        # A row with neither a number nor text deletes the key.
        rows = (
            frame.unique(subset=["ref_uri", "ts"], keep="last")
            .sort(["ref_uri", "ts"])
            .select(
                pl.col("ref_uri").alias("stream"),
                pl.col("ts").alias("time"),
                "number",
                "text",
            )
            .to_arrow()
        )
        return self._submit(
            lambda: self.engine.write(
                source, request_id, rows, deletes=[(stream, None, None) for stream in replace]
            )
        )

    def clear(self, source: str, request_id: str, streams):
        """Delete every reading of ``streams`` in one request."""
        deletes = [(stream, None, None) for stream in sorted(set(streams))]
        if not deletes:
            raise ValueError("clear needs at least one stream")
        return self._submit(lambda: self.engine.write(source, request_id, deletes=deletes))

    def wait(self, receipt, timeout: float = 30):
        return self.engine.snapshot(after=receipt, timeout=timeout)

    def wait_for(self, sequence: int, timeout: float = 30):
        return self.wait(Receipt(sequence), timeout=timeout)

    def epochs_of(self, sequences):
        return self.engine.epochs_of([Receipt(sequence) for sequence in sequences])

    def plan_at(self, epoch=None):
        return self.engine.snapshot(epoch).plan()

    def applied_at(self, epoch):
        """Last request published through an epoch, including refresh epochs."""
        while epoch > 0:
            after = max(0, epoch - 128)
            for record in reversed(self.engine.epochs(after=after, limit=epoch - after)):
                if record["epoch"] <= epoch and record["requests"] is not None:
                    return record["requests"][1]
            epoch = after
        return 0

    def batches(
        self,
        stream: str,
        *,
        start=None,
        end=None,
        limit=None,
        order="asc",
        batch_size=10_000,
        epoch=None,
        mode="numeric",
    ):
        lo, hi = _read_range(start, end, limit, order, batch_size, mode)
        try:
            snapshot = self.engine.snapshot(epoch)
        except NotFound as error:
            raise ValueError("epoch is not published") from error

        newest = order == "desc"

        def read(start, end, count):
            try:
                data = snapshot.read([stream], start=start, end=end, limit=count, newest_first=newest)
            except NotFound:
                return
            with pa.RecordBatchReader.from_stream(data) as reader:
                yield from reader

        def raw_batches():
            # Try the indexed first/last readings for limited requests. If
            # the public value mode filters some out, continue with one lazy
            # range read over the rest of the same snapshot.
            count, last = 0, None
            for rows in read(lo, hi, limit):
                yield rows
                count += rows.num_rows
                if rows.num_rows:
                    last = rows["time"].cast(pa.int64())[-1].as_py()
            if limit is not None and count == limit and mode != "coalesce":
                if newest:
                    yield from read(lo, last, None)
                elif last < 2**63 - 1:
                    yield from read(last + 1, hi, None)

        def pages():
            remaining = limit
            found = False
            if remaining != 0:
                for rows in raw_batches():
                    batch = _selected(rows["time"], rows["number"], rows["text"], mode, stream)
                    count = batch.num_rows
                    if remaining is not None:
                        count = min(count, remaining)
                    for offset in range(0, count, batch_size):
                        found = True
                        yield batch.slice(offset, min(batch_size, count - offset))
                    if remaining is not None:
                        remaining -= count
                        if remaining == 0:
                            break
            if not found:
                yield pa.RecordBatch.from_pylist([], schema=schema(mode))

        return pages()

    def info(self, streams, *, snapshot=None):
        """Read the count and time bounds at one epoch."""
        snapshot = self.engine.snapshot() if snapshot is None else snapshot
        streams = list(dict.fromkeys(streams))
        try:
            extents = snapshot.extent(streams)
        except NotFound:
            known = {item["stream"] for item in snapshot.describe()}
            extents = snapshot.extent([stream for stream in streams if stream in known])
        bounds = {row["stream"]: row for row in extents}
        result = {}
        for stream in streams:
            row = bounds.get(stream, {})
            result[stream] = dict(
                row_count=row.get("count", 0),
                earliest=_iso(row.get("first")),
                latest=_iso(row.get("last")),
            )
        return result

    def versions(self, *, after=None, streams=None):
        """Return the pinned feed frontier and a lazy Arrow reader."""
        snapshot = self.engine.snapshot()
        return snapshot.epoch, pa.RecordBatchReader.from_stream(
            snapshot.versions(streams, after=after)
        )

    def changes(self, *, epoch=0, ordinal=0, limit=1000):
        """Page signed changes derived from the engine's version rows."""
        if any(type(value) is not int or not 0 <= value < 2**64 for value in (epoch, ordinal)):
            raise ValueError("epoch and ordinal must be unsigned 64-bit integers")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10,000")
        status = self.engine.status()
        frontier, retention = status["published"], status["retention"]
        if epoch > frontier or (epoch == 0 and ordinal != 0):
            raise ValueError("cursor must refer to published history")
        if epoch < retention:
            raise Refused(f"cursor epoch {epoch} is before retention epoch {retention}")
        if retention and epoch == retention and ordinal != 2**64 - 1:
            raise Refused(f"change ordinals at retention epoch {retention} are unavailable")
        cursor = {"epoch": epoch, "ordinal": ordinal}
        page = []
        complete = frontier
        first = max(1, retention + 1, epoch)
        for through in range(first, frontier + 1):
            snapshot = self.engine.snapshot(through)
            with pa.RecordBatchReader.from_stream(snapshot.versions(after=through - 1)) as reader:
                position = -1
                for batch in reader:
                    times = batch["time"].cast(pa.int64())
                    for stream, time, number, text, since, until in zip(
                        batch["stream"], times, batch["number"], batch["text"], batch["since"], batch["until"]
                    ):
                        added = since.as_py() == through
                        if not added and until.as_py() != through:
                            continue
                        position += 1
                        if through == epoch and position <= ordinal:
                            continue
                        if len(page) == limit:
                            complete = through - 1
                            break
                        cursor = {"epoch": through, "ordinal": position}
                        page.append({
                            **cursor, "weight": 1 if added else -1,
                            "row": {
                                "key": {"stream": stream.as_py(), "time": time.as_py()},
                                "data": {"value": number.as_py() if number.is_valid else text.as_py()},
                                "revision": since.as_py(),
                            },
                        })
                    if complete != frontier:
                        break
            if complete != frontier:
                break
        return {
            "changes": page, "cursor": cursor, "published_epoch": frontier,
            "complete_through": complete, "has_more": complete != frontier,
        }

    def close(self, *, drain=True, timeout: float | None = None) -> bool:
        """Stop processing requests, after any running submissions finish."""
        with self._state:
            self._closing = True
            self._state.wait_for(lambda: not self._submitting)
            if self._stopping is None:
                self.pending_at_close = self.engine.status()["pending"]
                self._stopping = Thread(
                    target=_stop, args=(self._close, drain, self._stop_failure),
                    name="escapement-epoch-stop", daemon=True,
                )
                self._stopping.start()
            stopping = self._stopping
        stopping.join(timeout)
        if stopping.is_alive():
            return False
        if self._stop_failure:
            raise self._stop_failure[0]
        return True

    def _close(self, *, drain):
        self._retention_stop.set()
        if self._retention_thread is not None:
            self._retention_thread.join()
        self.engine.close(drain=drain)
