"""One embedded Escapement owner and Arrow transport helpers.

All durable timeseries state and execution live in Escapement. This module has
no stream registry, SQL connections, revision ledger, or computation scheduler.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import RLock, Thread

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from escapement import Engine, Mutation, Receipt, Snapshot

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


def schema(mode: str) -> pa.Schema:
    return SCHEMA if mode == "numeric" else TEXT_SCHEMA


def _selected(rows: pa.RecordBatch, mode: str, stream: str) -> pa.RecordBatch:
    """The readings a read in ``mode`` returns, from an Escapement batch:
    numbers only, text only, or every reading as text ("coalesce")."""
    times, values = rows["event_time_ns"], rows["value"]
    if mode == "coalesce":
        # Python's str() keeps numbers as they read elsewhere ("20.0", not "20").
        values = pa.array(
            [str(n) if s is None else s for n, s in zip(values.to_pylist(), rows["text"].to_pylist())],
            type=pa.string(),
        )
    else:
        if mode == "text":
            values = rows["text"]
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


def _stop(processor, drain, failure):
    try:
        processor.stop(drain=drain)
    except BaseException as error:
        failure.append(error)
    finally:
        del processor  # The error's traceback must not keep it alive.


class Core:
    def __init__(self, path: str | Path, **limits):
        self._gate = RLock()
        self.engine = Engine(path, **limits)
        try:
            self.processor = self.engine.start()
        except BaseException:
            self.engine.close()
            raise

    def ingest(self, source: str, request_id: str, frame: pl.DataFrame) -> Receipt:
        """Submit ``ref_uri``, timezone-aware ``ts`` and ``value`` rows, or rows
        with ``operation`` ``"delete"``. Each value is stored as its row's
        ``value_kind`` (numeric when absent; see ``split_values``)."""
        if frame.height > MAX_REQUEST_ROWS:
            raise ValueError(
                f"a request may contain at most {MAX_REQUEST_ROWS:,} rows; chunk imports explicitly"
            )
        dtype = frame.schema["ts"]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise ValueError("ts must be a timezone-aware datetime column")
        if "operation" not in frame.columns:
            frame = frame.with_columns(pl.lit("upsert").alias("operation"))
        if "value_kind" not in frame.columns:
            frame = frame.with_columns(pl.lit("numeric").alias("value_kind"))
        deleted = frame["operation"].eq("delete")
        frame = pl.concat([
            frame.filter(deleted).select(
                "ref_uri", "ts", "operation",
                pl.lit(None, pl.Float64).alias("number"), pl.lit(None, pl.String).alias("text"),
            ),
            split_values(frame.filter(~deleted).select(
                "ref_uri", "ts", "operation", "value", "value_kind"
            )),
        ])
        times = (
            pc.cast(frame["ts"].to_arrow(), pa.timestamp("ns", tz="UTC"), safe=True)
            .cast(pa.int64())
            .to_pylist()
        )
        deletes = frame["operation"].eq("delete").to_list()
        normalized = {}
        for (stream, number, text), timestamp, delete in zip(
            frame.select("ref_uri", "number", "text").iter_rows(), times, deletes
        ):
            normalized[(stream, timestamp)] = (
                Mutation.delete(stream, timestamp)
                if delete
                else Mutation.put(stream, timestamp, text if number is None else number)
            )
        with self._gate:
            receipt = self.engine.ingest(
                source, request_id, [normalized[key] for key in sorted(normalized)]
            )
            self.processor.wake()
            return receipt

    def clear(self, source: str, request_id: str, streams) -> Receipt:
        """Delete every reading of ``streams`` in one request."""
        mutations = [Mutation.delete_range(stream) for stream in sorted(set(streams))]
        if not mutations:
            raise ValueError("clear needs at least one stream")
        with self._gate:
            receipt = self.engine.ingest(source, request_id, mutations)
            self.processor.wake()
            return receipt

    def activate(self, request_id: str, plan) -> Receipt:
        with self._gate:
            receipt = self.engine.activate(request_id, plan)
            self.processor.wake()
            return receipt

    def wait(self, receipt: Receipt, timeout: float = 30) -> Snapshot:
        return self.processor.read_after(receipt, timeout=timeout)

    def changes(self, *, epoch=0, ordinal=0, limit=1000):
        """Page the signed native feed at a captured publication frontier.

        Cursors are exclusive and database-scoped. A page may split an epoch;
        complete_through tells consumers which epochs can become visible.
        """
        for name, value in (("epoch", epoch), ("ordinal", ordinal)):
            if type(value) is not int or not 0 <= value < 2**64:
                raise ValueError(f"{name} must be an unsigned 64-bit integer")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10,000")
        with self._gate:
            frontier = self.engine.snapshot().epoch
            if epoch > frontier or (epoch == 0 and ordinal != 0):
                raise ValueError("cursor must refer to published history")
            rows = self.engine.changes(epoch=epoch, ordinal=ordinal, limit=limit + 1)
        rows = [row for row in rows if row["epoch"] <= frontier]
        more = len(rows) > limit
        complete = rows[limit]["epoch"] - 1 if more else frontier
        page = rows[:limit]
        cursor = {"epoch": epoch, "ordinal": ordinal}
        if page:
            cursor = {key: page[-1][key] for key in ("epoch", "ordinal")}
        return dict(
            changes=page,
            cursor=cursor,
            published_epoch=frontier,
            complete_through=complete,
            has_more=more,
        )

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
        snapshot=None,
        mode="numeric",
    ):
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
        # Capture before returning the iterator: all pages belong to one epoch.
        if snapshot is not None and epoch is not None:
            raise ValueError("choose snapshot or epoch")
        snapshot = snapshot or (self.engine.snapshot() if epoch is None else None)
        if epoch is not None and not 0 <= epoch <= self.engine.status()["epoch"]:
            raise ValueError("epoch is not published")

        def pages():
            nonlocal lo, hi
            remaining = limit
            yielded = False
            while remaining is None or remaining > 0:
                count = (
                    min(batch_size, remaining) if remaining is not None else batch_size
                )
                kwargs = dict(start=lo, end=hi, limit=count, descending=order == "desc")
                rows = (
                    snapshot.query_arrow(stream, **kwargs)
                    if snapshot is not None
                    else self.engine.query_arrow(stream, epoch=epoch, **kwargs)
                )
                if not rows.num_rows:
                    break
                if pc.any(pc.not_equal(rows["sample_key"], 0)).as_py():
                    raise ValueError("Acquirium streams require sample_key=0")
                selected = _selected(rows, mode, stream)
                if selected.num_rows:
                    yielded = True
                    yield selected
                if remaining is not None:
                    remaining -= selected.num_rows
                if rows.num_rows < count:
                    break
                last = rows["event_time_ns"][-1].as_py()
                if order == "desc":
                    hi = last
                else:
                    if last == 2**63 - 1:
                        break
                    lo = last + 1
            if not yielded:
                # An empty read still carries the mode's schema.
                yield pa.RecordBatch.from_pylist([], schema=schema(mode))

        return pages()

    def info(self, streams):
        """Reading count and earliest and latest times per stream, from one
        snapshot, without reading the rows."""

        def iso(value):
            if value is None:
                return None
            return (
                pl.Series([value], dtype=pl.Int64)
                .cast(pl.Datetime("ns", "UTC"))
                .dt.to_string("%Y-%m-%dT%H:%M:%S%.9f%:z")
                .item()
            )

        snapshot = self.engine.snapshot()
        result = {}
        for stream in streams:
            stats = snapshot.stats(stream) or {"count": 0, "first_ns": None, "last_ns": None}
            result[stream] = dict(
                row_count=stats["count"],
                earliest=iso(stats["first_ns"]),
                latest=iso(stats["last_ns"]),
            )
        return result

    def close(self, *, drain=True, timeout: float | None = None) -> bool:
        """Stop the processor, then close the engine. Return whether it closed.

        Escapement cannot interrupt a running function, so with ``timeout`` the
        stop runs in a daemon thread and is abandoned when it expires: the
        engine stays open until the process exits, and accepted work not yet
        published is processed when the engine next opens.
        """
        with self._gate:
            if self.processor is None:
                return True
            failure = []
            # The thread drops its arguments when it finishes, releasing the
            # processor's native shared ownership before the engine closes.
            stopping = Thread(
                target=_stop, args=(self.processor, drain, failure),
                name="escapement-stop", daemon=True,
            )
            self.processor = None
            stopping.start()
            stopping.join(timeout)
            if stopping.is_alive():
                return False
            self.engine.close()
            if failure:
                raise failure[0]
            return True
