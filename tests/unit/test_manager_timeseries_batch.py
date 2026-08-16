from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from acquirium.Server.manager import Manager
from acquirium.Storage.values import prepare_value_columns
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
    # A batch mixing scalar types is stringified so the split can vectorize;
    # the values still survive intact through prepare_value_columns.
    assert {row["value"] for row in rows} == {"72.4", "OK", None}
    split = prepare_value_columns(store.frames[0]).sort("ts")
    assert split.get_column("numeric_value").to_list() == [72.4, None, None]
    assert split.get_column("text_value").to_list() == [None, "OK", None]
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

    count = mgr.insert_timeseries_arrow("source/file.csv", df.to_arrow())

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

    assert mgr.insert_timeseries_arrow("source/file.csv", df.to_arrow()) == 2
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


# ─────────────────────── change hook ───────────────────────


class _HookStore(_BulkStore):
    def upsert_rows(self, ref_uri, rows, value_kind="numeric"):
        return len(rows)

    def replace_rows(self, ref_uri, rows, value_kind="numeric"):
        return len(rows)


def _hooked_manager():
    mgr = Manager.__new__(Manager)
    mgr.timescale = _HookStore()
    calls: list[tuple] = []
    mgr.change_hook = lambda source_id, refs, cascade: calls.append((source_id, sorted(refs), cascade))
    return mgr, calls


def test_change_hook_fires_from_all_three_insert_paths():
    mgr, calls = _hooked_manager()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    r = lambda name: str(compute_ref_uri("src", name))

    mgr.insert_timeseries(source_id="src", ref_name="a", rows=[(ts, 1.0)])
    mgr.insert_timeseries_batch("src", {"b": [(ts, 1.0)], "c": [(ts, 2.0), (ts, 3.0)]})
    import pyarrow as pa
    table = pa.table({"ts": [ts, ts], "ref_name": ["d", "d"], "value": [1.0, 2.0]})
    mgr.insert_timeseries_arrow("src", table)

    assert calls == [
        ("src", [r("a")], False),
        ("src", sorted([r("b"), r("c")]), False),          # de-duplicated per stream
        ("src", [r("d")], False),
    ]


def test_change_hook_carries_cascade_and_survives_failure():
    mgr, calls = _hooked_manager()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mgr.insert_timeseries(source_id="app:x", ref_name="out", rows=[(ts, 1.0)], cascade=True)
    assert calls[-1][0] == "app:x" and calls[-1][2] is True

    mgr.change_hook = lambda *a: (_ for _ in ()).throw(RuntimeError("feed down"))
    # A broken hook must never fail the insert.
    assert mgr.insert_timeseries(source_id="src", ref_name="a", rows=[(ts, 1.0)]) == 1


def test_no_hook_is_a_noop():
    mgr = Manager.__new__(Manager)
    mgr.timescale = _HookStore()
    mgr.change_hook = None
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert mgr.insert_timeseries(source_id="src", ref_name="a", rows=[(ts, 1.0)]) == 1
