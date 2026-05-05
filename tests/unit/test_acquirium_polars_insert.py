from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
from acquirium.Client.acquirium import Acquirium
from acquirium.Server.direct_client import DirectAcquirium


def test_insert_timeseries_polars_default_delegates_with_correct_column_order():
    aq = Acquirium.__new__(Acquirium)
    captured: dict[str, object] = {}

    def insert_timeseries_batch(source_id, streams):
        captured["source_id"] = source_id
        captured["streams"] = streams
        return {"ok": True, "rows_inserted": sum(len(rows) for rows in streams.values())}

    aq.insert_timeseries_batch = insert_timeseries_batch
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1)],
            "ref_name": ["temp", "state/value"],
            "value": ["72.4", "OK"],
            "value_kind": ["numeric", "text"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
            "value_kind": pl.Utf8,
        },
    )

    result = aq.insert_timeseries_polars("source/file.csv", df)

    assert result == {"ok": True, "rows_inserted": 2}
    assert captured == {
        "source_id": "source/file.csv",
        "streams": {
            "temp": [(ts, "72.4")],
            "state/value": [(ts.replace(hour=1), "OK")],
        },
    }


def test_insert_timeseries_polars_ignores_value_kind_column():
    aq = Acquirium.__new__(Acquirium)
    captured: dict[str, object] = {}
    aq.insert_timeseries_batch = lambda source_id, streams: captured.update(streams=streams) or {
        "ok": True,
        "rows_inserted": sum(len(rows) for rows in streams.values()),
    }
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1)],
            "ref_name": ["state", "state"],
            "value": ["1", "ON"],
            "value_kind": ["numeric", "text"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
            "value_kind": pl.Utf8,
        },
    )

    assert aq.insert_timeseries_polars("source/file.csv", df) == {"ok": True, "rows_inserted": 2}
    assert captured["streams"] == {"state": [(ts, "1"), (ts.replace(hour=1), "ON")]}


def test_insert_timeseries_polars_converts_nan_for_json_transport():
    aq = Acquirium.__new__(Acquirium)
    captured: dict[str, object] = {}
    aq.insert_timeseries_batch = lambda source_id, streams: captured.update(streams=streams) or {
        "ok": True,
        "rows_inserted": sum(len(rows) for rows in streams.values()),
    }
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts],
            "ref_name": ["temp"],
            "value": [float("nan")],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Float64,
        },
    )

    assert aq.insert_timeseries_polars("source/file.csv", df) == {"ok": True, "rows_inserted": 1}
    assert captured["streams"] == {"temp": [(ts, None)]}


def test_direct_acquirium_polars_insert_uses_configured_batching():
    class _Manager:
        def __init__(self):
            self.batches = []

        def insert_timeseries_batch(self, source_id, streams):
            self.batches.append((source_id, streams))
            return sum(len(rows) for rows in streams.values())

    manager = _Manager()
    aq = DirectAcquirium(manager, insert_batch_rows=2)
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1), ts.replace(hour=2)],
            "ref_name": ["temp", "temp", "state"],
            "value": [1.0, 2.0, "OK"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Object,
        },
    )

    result = aq.insert_timeseries_polars("source/file.csv", df)

    assert result == {"ok": True, "rows_inserted": 3, "batches": 2}
    assert len(manager.batches) == 2
    assert sum(
        len(rows)
        for _, streams in manager.batches
        for rows in streams.values()
    ) == 3
