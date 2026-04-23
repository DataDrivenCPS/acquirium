"""Unit tests for DuckDBStore — no Docker required.

Run with:  uv run pytest tests/test_duckdb_store.py -m unit
"""
from __future__ import annotations

import pytest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import polars as pl

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.base import TimeseriesStore
from acquirium.internals.models import LogEntry, TimeIntervalModel


# ---- fixtures ----

@pytest.fixture(scope="module")
def store(tmp_path_factory):
    p = tmp_path_factory.mktemp("duckdb") / "test.duckdb"
    s = DuckDBStore(db_path=p, recreate=True)
    yield s
    s.close()


def _utc(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ---- protocol conformance ----

@pytest.mark.unit
def test_protocol_conformance(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "proto.duckdb")
    assert isinstance(s, TimeseriesStore)
    s.close()


# ---- upsert_rows ----

@pytest.mark.unit
def test_upsert_rows_basic(store):
    uri = "urn:test:duck:upsert"
    rows = [(_utc(2024, 1, 1), "10.5"), (_utc(2024, 1, 2), "11.0")]
    n = store.upsert_rows(uri, rows)
    assert n == 2


@pytest.mark.unit
def test_upsert_rows_conflict_updates(store):
    uri = "urn:test:duck:conflict"
    store.upsert_rows(uri, [(_utc(2024, 1, 1), "1.0")])
    store.upsert_rows(uri, [(_utc(2024, 1, 1), "2.0")])  # conflict → update
    batches = list(store.timeseries(uri))
    rows = [r for b in batches for r in b.to_pydict()["value"]]
    assert rows == ["2.0"]


@pytest.mark.unit
def test_upsert_rows_empty(store):
    n = store.upsert_rows("urn:test:duck:empty", [])
    assert n == 0


# ---- replace_rows ----

@pytest.mark.unit
def test_replace_rows(store):
    uri = "urn:test:duck:replace"
    store.upsert_rows(uri, [(_utc(2024, 1, 1), "old"), (_utc(2024, 1, 2), "old2")])
    store.replace_rows(uri, [(_utc(2024, 1, 3), "new")])
    batches = list(store.timeseries(uri))
    all_vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert all_vals == ["new"]


# ---- bulk_insert_polars ----

@pytest.mark.unit
def test_bulk_insert_polars(store):
    uri = "urn:test:duck:bulk"
    df = pl.DataFrame({
        "point_uri": [uri, uri, uri],
        "ts": [_utc(2024, 2, 1), _utc(2024, 2, 2), _utc(2024, 2, 3)],
        "value": ["a", "b", "c"],
    })
    n = store.bulk_insert_polars(df)
    assert n == 3
    info = store.timeseries_info(uri)
    assert info.row_count == 3


# ---- timeseries query ----

@pytest.mark.unit
def test_timeseries_basic(store):
    uri = "urn:test:duck:ts_basic"
    store.upsert_rows(uri, [
        (_utc(2024, 3, 1), "1"),
        (_utc(2024, 3, 2), "2"),
        (_utc(2024, 3, 3), "3"),
    ])
    batches = list(store.timeseries(uri))
    assert len(batches) >= 1
    batch = batches[0]
    assert batch.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert batch.schema.field("value").type == pa.string()
    assert batch.schema.field("uri").type == pa.string()
    vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert vals == ["1", "2", "3"]


@pytest.mark.unit
def test_timeseries_with_start_end(store):
    uri = "urn:test:duck:ts_range"
    store.upsert_rows(uri, [
        (_utc(2024, 4, 1), "A"),
        (_utc(2024, 4, 5), "B"),
        (_utc(2024, 4, 10), "C"),
    ])
    batches = list(store.timeseries(uri, start=_utc(2024, 4, 4), end=_utc(2024, 4, 6)))
    vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert vals == ["B"]


@pytest.mark.unit
def test_timeseries_order_desc(store):
    uri = "urn:test:duck:ts_desc"
    store.upsert_rows(uri, [(_utc(2024, 5, i), str(i)) for i in range(1, 4)])
    batches = list(store.timeseries(uri, order="desc"))
    vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert vals == ["3", "2", "1"]


@pytest.mark.unit
def test_timeseries_limit(store):
    uri = "urn:test:duck:ts_limit"
    store.upsert_rows(uri, [(_utc(2024, 6, i), str(i)) for i in range(1, 6)])
    batches = list(store.timeseries(uri, limit=2))
    vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert len(vals) == 2


# ---- timeseries_info ----

@pytest.mark.unit
def test_timeseries_info(store):
    uri = "urn:test:duck:info"
    store.upsert_rows(uri, [
        (_utc(2024, 7, 1), "x"),
        (_utc(2024, 7, 31), "y"),
    ])
    info = store.timeseries_info(uri)
    assert info.row_count == 2
    assert info.earliest is not None
    assert info.latest is not None
    assert info.earliest <= info.latest


@pytest.mark.unit
def test_timeseries_info_missing(store):
    info = store.timeseries_info("urn:test:duck:nonexistent")
    assert info.row_count == 0


@pytest.mark.unit
def test_timeseries_info_batch(store):
    uris = ["urn:test:duck:batch_a", "urn:test:duck:batch_b"]
    store.upsert_rows(uris[0], [(_utc(2024, 8, 1), "1")])
    store.upsert_rows(uris[1], [(_utc(2024, 8, 2), "2"), (_utc(2024, 8, 3), "3")])
    result = store.timeseries_info_batch(uris)
    assert result[uris[0]].row_count == 1
    assert result[uris[1]].row_count == 2


# ---- stream handles ----

@pytest.mark.unit
def test_ensure_stream_handle_returns_handle(store):
    h = store.ensure_stream_handle("urn:test:duck:sh1", "src1", "ref1")
    assert isinstance(h, str) and len(h) > 0


@pytest.mark.unit
def test_ensure_stream_handle_deterministic(store):
    h1 = store.ensure_stream_handle("urn:test:duck:sh2", "src2", "ref2")
    h2 = store.ensure_stream_handle("urn:test:duck:sh2", "src2", "ref2")
    assert h1 == h2


@pytest.mark.unit
def test_resolve_handle(store):
    uri = "urn:test:duck:resolve_handle"
    h = store.ensure_stream_handle(uri, "srcX", "refX")
    point, src, ref = store.resolve_handle(h)
    assert point == uri
    assert src == "srcX"
    assert ref == "refX"


@pytest.mark.unit
def test_resolve_handle_missing(store):
    point, src, ref = store.resolve_handle("no-such-handle")
    assert (point, src, ref) == (None, None, None)


@pytest.mark.unit
def test_resolve_storage_key(store):
    uri = "urn:test:duck:rsk"
    h = store.ensure_stream_handle(uri, "s", "r")
    assert store.resolve_storage_key(uri) == h


@pytest.mark.unit
def test_resolve_storage_key_unregistered(store):
    uri = "urn:test:duck:unregistered"
    assert store.resolve_storage_key(uri) == uri


@pytest.mark.unit
def test_resolve_storage_keys_batch(store):
    uris = ["urn:test:duck:rsk_batch_a", "urn:test:duck:rsk_batch_b"]
    ha = store.ensure_stream_handle(uris[0], "sa", "ra")
    hb = store.ensure_stream_handle(uris[1], "sb", "rb")
    result = store.resolve_storage_keys(uris)
    assert result[uris[0]] == ha
    assert result[uris[1]] == hb


# ---- logs ----

@pytest.mark.unit
def test_insert_and_query_log(store):
    uri = "urn:test:duck:log1"
    entry = LogEntry(
        point_uri=uri,
        timestamp=_utc(2024, 9, 1),
        period=TimeIntervalModel(start=_utc(2024, 9, 1), end=_utc(2024, 9, 30)),
        message="test log",
    )
    store.insert_log(entry)
    results = store.query_logs(uri)
    assert len(results) == 1
    assert results[0].message == "test log"
    assert results[0].period.start is not None
    assert results[0].period.end is not None


@pytest.mark.unit
def test_log_time_filter(store):
    uri = "urn:test:duck:log_time"
    for day in [1, 5, 10]:
        store.insert_log(LogEntry(
            point_uri=uri,
            timestamp=_utc(2024, 10, day),
            period=TimeIntervalModel(),
            message=f"day{day}",
        ))
    results = store.query_logs(
        uri,
        log_time_interval=TimeIntervalModel(start=_utc(2024, 10, 4), end=_utc(2024, 10, 6)),
    )
    assert len(results) == 1
    assert results[0].message == "day5"


@pytest.mark.unit
def test_log_observation_overlap(store):
    uri = "urn:test:duck:log_obs"
    store.insert_log(LogEntry(
        point_uri=uri,
        timestamp=_utc(2024, 11, 1),
        period=TimeIntervalModel(start=_utc(2024, 11, 1), end=_utc(2024, 11, 10)),
        message="overlapping",
    ))
    store.insert_log(LogEntry(
        point_uri=uri,
        timestamp=_utc(2024, 11, 2),
        period=TimeIntervalModel(start=_utc(2024, 11, 20), end=_utc(2024, 11, 30)),
        message="non-overlapping",
    ))
    results = store.query_logs(
        uri,
        obs_time_interval=TimeIntervalModel(start=_utc(2024, 11, 5), end=_utc(2024, 11, 15)),
    )
    assert len(results) == 1
    assert results[0].message == "overlapping"


@pytest.mark.unit
def test_delete_logs(store):
    uri = "urn:test:duck:log_delete"
    store.insert_log(LogEntry(
        point_uri=uri,
        timestamp=_utc(2024, 12, 1),
        period=TimeIntervalModel(),
        message="to delete",
    ))
    assert len(store.query_logs(uri)) == 1
    store.delete_logs(uri)
    assert len(store.query_logs(uri)) == 0


# ---- transactions ----

@pytest.mark.unit
def test_commit(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "tx_commit.duckdb")
    uri = "urn:test:duck:tx_commit"
    s.begin()
    s.upsert_rows(uri, [(_utc(2024, 1, 1), "committed")])
    s.commit()
    info = s.timeseries_info(uri)
    assert info.row_count == 1
    s.close()


@pytest.mark.unit
def test_rollback(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "tx_rollback.duckdb")
    uri = "urn:test:duck:tx_rollback"
    s.begin()
    s.upsert_rows(uri, [(_utc(2024, 1, 1), "should_vanish")])
    s.rollback()
    info = s.timeseries_info(uri)
    assert info.row_count == 0
    s.close()


# ---- sql_query ----

@pytest.mark.unit
def test_sql_query(store):
    result = store.sql_query("SELECT 1 AS x")
    assert result["columns"] == ["x"]
    assert result["rows"] == [[1]]


# ---- recreate ----

@pytest.mark.unit
def test_recreate_wipes_data(tmp_path):
    p = tmp_path / "recreate.duckdb"
    s = DuckDBStore(db_path=p)
    s.upsert_rows("urn:x", [(_utc(2024, 1, 1), "v")])
    s.close()

    s2 = DuckDBStore(db_path=p, recreate=True)
    info = s2.timeseries_info("urn:x")
    assert info.row_count == 0
    s2.close()


# ---- factory ----

@pytest.mark.unit
def test_factory_creates_duckdb(tmp_path):
    from acquirium.Storage import create_timeseries_store
    s = create_timeseries_store("duckdb", duckdb_path=tmp_path / "factory.duckdb")
    assert isinstance(s, TimeseriesStore)
    s.close()


@pytest.mark.unit
def test_factory_unknown_backend():
    from acquirium.Storage import create_timeseries_store
    with pytest.raises(ValueError, match="Unknown"):
        create_timeseries_store("sqlite")
