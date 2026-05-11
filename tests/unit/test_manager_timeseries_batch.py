from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_ref_uri


class _BulkStore:
    def __init__(self) -> None:
        self.frames = []
        self.refs = []

    def ensure_stream_ref(self, point_uri, source_id, ref_name, ref_uri=None, value_kind="text"):
        self.refs.append(
            {
                "point_uri": point_uri,
                "source_id": source_id,
                "ref_name": ref_name,
                "ref_uri": str(ref_uri),
                "value_kind": value_kind,
            }
        )
        return str(ref_uri)

    def bulk_insert_polars(self, df):
        self.frames.append(df)
        return len(df)

    def stream_value_kind(self, ref_uri):
        if ref_uri == str(compute_ref_uri("source/file.csv", "state/value")):
            return "text"
        return "numeric"


class _FailingBulkStore(_BulkStore):
    def bulk_insert_polars(self, df):
        self.frames.append(df)
        raise RuntimeError("bulk insert failed")


def test_insert_timeseries_batch_uses_computed_ref_uris_in_one_bulk_insert():
    mgr = Manager.__new__(Manager)
    store = _BulkStore()
    mgr.timescale = store

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    count = mgr.insert_timeseries_batch(
        "source/file.csv",
        {
            "temp": [(ts, 72.4)],
            "state/value": [(ts, "OK"), (ts.replace(hour=1), None)],
        },
    )

    assert count == 3
    assert len(store.frames) == 1
    rows = store.frames[0].sort(["ref_uri", "ts"]).to_dicts()
    assert store.frames[0].columns == ["ref_uri", "ts", "value", "value_kind"]
    assert {row["ref_uri"] for row in rows} == {
        str(compute_ref_uri("source/file.csv", "temp")),
        str(compute_ref_uri("source/file.csv", "state/value")),
    }
    assert {row["value"] for row in rows} == {72.4, "OK", None}
    assert store.frames[0].get_column("value_kind").to_list() == ["numeric", "text", "text"]
    assert store.refs == []


def test_insert_timeseries_uses_computed_ref_uris_in_one_bulk_insert():
    mgr = Manager.__new__(Manager)
    store = _BulkStore()
    mgr.timescale = store

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1), ts],
            "ref_name": ["temp", "state/value", "temp"],
            "value": ["72.4", "OK", "73.1"],
            "value_kind": ["numeric", "text", "numeric"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
            "value_kind": pl.Utf8,
        },
    )

    count = mgr.insert_timeseries("source/file.csv", df.to_arrow())

    assert count == 3
    assert len(store.frames) == 1
    rows = store.frames[0].sort(["ref_uri", "ts"]).to_dicts()
    assert store.frames[0].columns == ["ref_uri", "ts", "value", "value_kind"]
    assert {row["ref_uri"] for row in rows} == {
        str(compute_ref_uri("source/file.csv", "temp")),
        str(compute_ref_uri("source/file.csv", "state/value")),
    }
    assert {row["value"] for row in rows} == {"72.4", "73.1", "OK"}
    assert store.frames[0].sort(["ref_uri", "ts"]).get_column("value_kind").to_list() == ["text", "numeric", "numeric"]
    assert store.refs == []


def test_insert_timeseries_ignores_input_value_kind_column():
    mgr = Manager.__new__(Manager)
    store = _BulkStore()
    mgr.timescale = store

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

    assert mgr.insert_timeseries("source/file.csv", df.to_arrow()) == 2
    assert store.frames[0].get_column("value_kind").to_list() == ["numeric", "numeric"]


def test_insert_timeseries_batch_does_not_register_streams_when_bulk_insert_fails():
    mgr = Manager.__new__(Manager)
    store = _FailingBulkStore()
    mgr.timescale = store

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    with pytest.raises(RuntimeError, match="bulk insert failed"):
        mgr.insert_timeseries_batch("source/file.csv", {"temp": [(ts, 72.4)]})

    assert len(store.frames) == 1
    assert store.refs == []
