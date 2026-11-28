"""One embedded Escapement owner and Arrow transport helpers.

All durable timeseries state and execution live in Escapement. This module has
no stream registry, SQL connections, revision ledger, or computation scheduler.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
from threading import Condition, Event, Lock, Thread

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from escapement import Closed, Engine, EscapementError, NotFound, Receipt, Refused

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
END_OF_EPOCH = 2**64 - 1
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestResult:
    receipt: Receipt
    rows_inserted: int


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
    """Normalize an optional timestamp to signed UTC nanoseconds.

    Integers already use nanoseconds. Datetimes and ISO strings without a
    time zone are interpreted as UTC.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, datetime, str)):
        raise ValueError("timestamps must be UTC nanoseconds or ISO datetimes")
    if isinstance(value, str):
        try:
            parser = pl.Series([value]).str
            micros = parser.to_datetime(time_unit="us", time_zone="UTC", strict=True).cast(pl.Int64).item()
            nanos = parser.to_datetime(time_unit="ns", time_zone="UTC", strict=True).cast(pl.Int64).item()
        except pl.exceptions.PolarsError as error:
            raise ValueError(f"invalid timestamp {value!r}") from error
        # The nanosecond parser can wrap. The wider microsecond range lets
        # us detect it while retaining all nine fractional digits.
        if nanos // 1000 != micros:
            raise ValueError("timestamp exceeds signed UTC nanoseconds")
        return nanos
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        micros = pa.array([value], type=pa.timestamp("us", tz="UTC")).cast(pa.int64())[0].as_py()
        # Timestamp subclasses such as pandas.Timestamp can carry sub-microsecond precision.
        value = micros * 1000 + getattr(value, "nanosecond", 0)
    if not -(2**63) <= value < 2**63:
        raise ValueError("timestamp exceeds signed UTC nanoseconds")
    return value


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
        self._snapshot_lock = Lock()
        self._snapshot_pins: Counter[int] = Counter()
        self._retention_thread = None
        if history_epochs is not None:
            self._retention_thread = Thread(
                target=self._prune_history, args=(history_epochs,), name="escapement-retention", daemon=True,
            )
            self._retention_thread.start()

    def _prune_history(self, history_epochs):
        """Advance retention while preserving the oldest pinned snapshot."""
        while not self._retention_stop.wait(1):
            try:
                with self._snapshot_lock:
                    status = self.engine.status()
                    before = status["published"] - history_epochs + 1
                    if self._snapshot_pins:
                        before = min(before, min(self._snapshot_pins))
                    if before > status["retention"]:
                        self.engine.prune(before)
            except Closed:
                return
            except EscapementError:
                logger.exception("could not advance Escapement retention")

    @contextmanager
    def pinned_snapshot(self):
        """Keep a published epoch readable throughout a handler's work."""
        with self._snapshot_lock:
            snapshot = self.engine.snapshot()
            epoch = snapshot.epoch
            self._snapshot_pins[epoch] += 1
        try:
            yield snapshot
        finally:
            with self._snapshot_lock:
                self._snapshot_pins[epoch] -= 1
                if not self._snapshot_pins[epoch]:
                    del self._snapshot_pins[epoch]

    def ingest(self, source: str, request_id: str, frame: pl.DataFrame, *, replace=()) -> IngestResult:
        """Submit ``stream_uri``, timezone-aware ``ts`` and ``value`` rows, or rows
        with ``operation`` ``"delete"``. Each value is stored as its row's
        ``value_kind`` (numeric when absent; see ``split_values``).

        Each stream in ``replace`` first loses all its readings, in the same
        request, so it holds exactly the rows supplied for it. The result
        includes the receipt and the normalized upsert count."""
        replace = sorted(set(replace))
        self._validate_write_frame(frame, len(replace))
        if "operation" not in frame.columns:
            frame = frame.with_columns(pl.lit("upsert").alias("operation"))
        if "value_kind" not in frame.columns:
            frame = frame.with_columns(pl.lit("numeric").alias("value_kind"))
        # Splitting deletes from values must not change which mutation wins
        # when the request addresses the same key more than once.
        frame = frame.select(
            "stream_uri", "ts", "operation", "value", "value_kind"
        ).with_row_index("_row")
        deleted = frame["operation"].eq("delete")
        rows = pl.concat([
            frame.filter(deleted).select(
                "_row", "stream_uri", "ts", "operation",
                pl.lit(None, pl.Float64).alias("number"), pl.lit(None, pl.String).alias("text"),
            ),
            split_values(frame.filter(~deleted).select(
                "_row", "stream_uri", "ts", "operation", "value", "value_kind"
            )),
        ]).sort("_row").drop("_row")
        return self._write_readings(source, request_id, rows, replace)

    def ingest_stored(self, source: str, request_id: str, frame: pl.DataFrame) -> IngestResult:
        """Replay a typed reading, preserving its stored number or text verbatim."""
        self._validate_write_frame(frame, 0)
        if frame.schema["number"] not in (pl.Float64, pl.Null) or frame.schema["text"] not in (pl.String, pl.Null):
            raise ValueError("stored readings require Float64 number and string text columns")
        if frame.select((pl.col("number").is_null() == pl.col("text").is_null()).any()).item():
            raise ValueError("each stored reading needs exactly one of number or text")
        rows = frame.with_columns(
            pl.lit("upsert").alias("operation"), pl.col("number").cast(pl.Float64), pl.col("text").cast(pl.String),
        )
        return self._write_readings(source, request_id, rows, ())

    @staticmethod
    def _validate_write_frame(frame: pl.DataFrame, replacements: int) -> None:
        if frame.height + replacements > MAX_REQUEST_ROWS:
            raise ValueError(f"a request may contain at most {MAX_REQUEST_ROWS:,} rows")
        dtype = frame.schema["ts"]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise ValueError("ts must be a timezone-aware datetime column")

    def _write_readings(self, source, request_id, rows, replace):
        # Keep the last surviving operation at each stream and time.
        # A row with neither a number nor text deletes the key.
        rows = (
            rows.unique(subset=["stream_uri", "ts"], keep="last")
            .sort(["stream_uri", "ts"])
        )
        count = rows.height - rows["operation"].eq("delete").sum()
        payload = rows.select(
            pl.col("stream_uri").alias("stream"), pl.col("ts").alias("time"), "number", "text"
        ).to_arrow()
        deletes = [(stream, None, None) for stream in replace]
        receipt = self._submit(lambda: self.engine.write(source, request_id, payload, deletes=deletes))
        return IngestResult(receipt, count)

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


    def clear(self, source: str, request_id: str, streams):
        """Delete every reading of ``streams`` in one request."""
        deletes = [(stream, None, None) for stream in sorted(set(streams))]
        if not deletes:
            raise ValueError("clear needs at least one stream")
        return self._submit(lambda: self.engine.write(source, request_id, deletes=deletes))

    def wait(self, receipt, timeout: float = 30):
        """Return the snapshot that publishes the accepted request."""
        return self.engine.snapshot(after=receipt, timeout=timeout)

    def wait_for(self, sequence: int, timeout: float = 30):
        """Wait until request ``sequence`` is published. Sequence 0 names the
        empty store, which epoch 0 already holds."""
        if sequence == 0:
            return self.engine.snapshot()
        return self.wait(Receipt(sequence), timeout=timeout)

    def epochs_of(self, sequences):
        """The epoch that published each sequence; 0 for sequence 0."""
        sequences = list(sequences)
        requests = [sequence for sequence in sequences if sequence]
        epochs = iter(self.engine.epochs_of([Receipt(sequence) for sequence in requests]))
        return [next(epochs) if sequence else 0 for sequence in sequences]

    def failures(self, snapshot, streams=None):
        """The described streams whose maintenance is behind at ``snapshot``."""
        return [item for item in snapshot.describe(streams) if item["behind"] is not None]

    def plan_at(self, epoch=None):
        return self.engine.snapshot(epoch).plan()

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
        """Stream a half-open time range from one published epoch.

        The row limit applies after value-mode filtering. Even an empty
        result yields a batch so Arrow readers receive the requested schema.
        """
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
                    last = rows["time"][-1].value
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
        # A stream never written, or written but empty, has no extent row.
        bounds = {row["stream"]: row for row in snapshot.extent(streams)}
        result = {}
        for stream in streams:
            row = bounds.get(stream, {})
            result[stream] = dict(
                row_count=row.get("count", 0),
                earliest=_iso(row.get("first")),
                latest=_iso(row.get("last")),
            )
        return result

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
        if retention and epoch == retention and ordinal != END_OF_EPOCH:
            raise Refused(f"change ordinals at retention epoch {retention} are unavailable")
        first = max(1, retention + 1, epoch)
        # Resuming inside the cursor's epoch skips the changes it has seen.
        skip = min(ordinal + 1, END_OF_EPOCH) if first == epoch else 0
        feed = self.engine.snapshot(frontier).changes(first - 1, skip=skip, limit=limit + 1)
        with pa.RecordBatchReader.from_stream(feed) as reader:
            table = reader.read_all()
        columns = {name: table[name].to_pylist() for name in
                   ("stream", "number", "text", "since", "epoch", "ordinal", "weight")}
        columns["time"] = table["time"].cast(pa.int64()).to_pylist()
        page = [
            {
                "epoch": row["epoch"], "ordinal": row["ordinal"], "weight": row["weight"],
                "row": {
                    "key": {"stream": row["stream"], "time": row["time"]},
                    "data": {"value": row["text"] if row["number"] is None else row["number"]},
                    "revision": row["since"],
                },
            }
            for row in (dict(zip(columns, values)) for values in zip(*columns.values()))
        ]
        # One change past the page tells where the complete epochs end.
        complete = frontier if len(page) <= limit else page.pop()["epoch"] - 1
        last = page[-1] if page else {"epoch": epoch, "ordinal": ordinal}
        if complete and last["epoch"] <= complete:
            last = {"epoch": complete, "ordinal": END_OF_EPOCH}
        return {
            "changes": page, "cursor": {"epoch": last["epoch"], "ordinal": last["ordinal"]},
            "published_epoch": frontier, "complete_through": complete, "has_more": complete != frontier,
        }

    def close(self, *, drain=True, timeout: float | None = None) -> bool:
        """Stop processing requests after any running submissions finish.

        Return false if the shutdown thread is still running after waiting
        for it. A subsequent call waits on that same shutdown.
        """
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
