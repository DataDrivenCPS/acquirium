"""One embedded Escapement owner and Arrow transport helpers.

All durable timeseries state and execution live in Escapement. This module has
no stream registry, SQL connections, revision ledger, or computation scheduler.
"""

from __future__ import annotations

from datetime import datetime
import math
from pathlib import Path
from threading import RLock

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from escapement import Engine, Mutation, Receipt, Snapshot

SCHEMA = pa.schema(
    [
        ("ts", pa.timestamp("ns", tz="UTC")),
        ("value", pa.float64()),
        ("uri", pa.string()),
    ]
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
        if not source or not request_id:
            raise ValueError("source and request_id must be nonempty")
        if frame.height > 10_000:
            raise ValueError(
                "a request may contain at most 10,000 rows; chunk imports explicitly"
            )
        if not {"ref_uri", "ts", "value"}.issubset(frame.columns):
            raise ValueError("observations require ref_uri, ts, and value")
        dtype = frame.schema["ts"]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise ValueError("ts must be a timezone-aware datetime column")
        times = (
            pc.cast(frame["ts"].to_arrow(), pa.timestamp("ns", tz="UTC"), safe=True)
            .cast(pa.int64())
            .to_pylist()
        )
        operations = (
            frame["operation"].to_list()
            if "operation" in frame.columns
            else ["upsert"] * len(frame)
        )
        normalized = {}
        for (stream, value), timestamp, operation in zip(
            frame.select("ref_uri", "value").iter_rows(), times, operations
        ):
            if not isinstance(stream, str) or not stream or timestamp is None:
                raise ValueError("stream IDs and timestamps must be non-null")
            if operation == "delete":
                mutation = Mutation.delete(stream, timestamp)
            elif operation == "upsert":
                if value is None or isinstance(value, (bool, str)):
                    raise ValueError("observations must be finite non-null numbers")
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("observations must be finite non-null numbers")
                mutation = Mutation.put(stream, timestamp, number)
            else:
                raise ValueError("operation must be upsert or delete")
            normalized[(stream, timestamp)] = mutation
        with self._gate:
            receipt = self.engine.ingest(
                source, request_id, [normalized[key] for key in sorted(normalized)]
            )
            self.processor.wake()
            return receipt

    def activate(self, request_id: str, plan) -> Receipt:
        with self._gate:
            receipt = self.engine.activate(request_id, plan)
            self.processor.wake()
            return receipt

    def wait(self, receipt: Receipt, timeout: float = 30) -> Snapshot:
        return self.processor.read_after(receipt, timeout=timeout)

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
    ):
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
            while remaining is None or remaining > 0:
                count = (
                    min(batch_size, remaining) if remaining is not None else batch_size
                )
                kwargs = dict(start=lo, end=hi, limit=count, descending=order == "desc")
                rows = (
                    snapshot.query(stream, **kwargs)
                    if snapshot is not None
                    else self.engine.query(stream, epoch=epoch, **kwargs)
                )
                if not rows:
                    break
                if any(row.sample_key != 0 for row in rows):
                    raise ValueError("Acquirium streams require sample_key=0")
                yield pa.RecordBatch.from_arrays(
                    [
                        pa.array(
                            [row.event_time_ns for row in rows],
                            type=SCHEMA.field("ts").type,
                        ),
                        pa.array([row.value for row in rows], type=pa.float64()),
                        pa.array([stream] * len(rows), type=pa.string()),
                    ],
                    schema=SCHEMA,
                )
                if remaining is not None:
                    remaining -= len(rows)
                if len(rows) < count:
                    break
                if order == "desc":
                    hi = rows[-1].event_time_ns
                else:
                    if rows[-1].event_time_ns == 2**63 - 1:
                        break
                    lo = rows[-1].event_time_ns + 1

        return pages()

    def info(self, streams):
        snapshot = self.engine.snapshot()
        result = {}
        for stream in streams:
            count, earliest, latest = 0, None, None
            for batch in self.batches(stream, snapshot=snapshot):
                count += batch.num_rows
                earliest = earliest if earliest is not None else batch["ts"][0].value
                latest = batch["ts"][-1].value

            def iso(value):
                if value is None:
                    return None
                return (
                    pl.Series([value], dtype=pl.Int64)
                    .cast(pl.Datetime("ns", "UTC"))
                    .dt.to_string("%Y-%m-%dT%H:%M:%S%.9f%:z")
                    .item()
                )

            result[stream] = dict(
                table="escapement",
                row_count=count,
                earliest=iso(earliest),
                latest=iso(latest),
            )
        return result

    def close(self, *, drain=True):
        with self._gate:
            if self.processor is None:
                return
            try:
                self.processor.stop(drain=drain)
            finally:
                self.processor = None  # Release native shared ownership, too.
                self.engine.close()
