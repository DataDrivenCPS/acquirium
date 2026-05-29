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


@pytest.mark.unit
def test_fresh_store_does_not_create_timeseries_indexes(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "schema.duckdb", recreate=True)
    try:
        result = s.sql_query(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'timeseries'"
        )
    finally:
        s.close()

    assert result["rows"] == []


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
    store.upsert_rows(uri, [(_utc(2024, 1, 1), "old"), (_utc(2024, 1, 2), "old2")], value_kind="text")
    store.replace_rows(uri, [(_utc(2024, 1, 3), "new")], value_kind="text")
    batches = list(store.timeseries(uri))
    all_vals = [v for b in batches for v in b.to_pydict()["value"]]
    assert all_vals == ["new"]


# ---- bulk_insert_polars ----

@pytest.mark.unit
def test_bulk_insert_polars(store):
    uri = "urn:test:duck:bulk"
    df = pl.DataFrame({
        "ref_uri": [uri, uri, uri],
        "ts": [_utc(2024, 2, 1), _utc(2024, 2, 2), _utc(2024, 2, 3)],
        "value": ["a", "b", "c"],
        "value_kind": ["text", "text", "text"],
    })
    n = store.bulk_insert_polars(df)
    assert n == 3
    info = store.timeseries_info(uri)
    assert info.row_count == 3


@pytest.mark.unit
def test_bulk_insert_polars_splits_numeric_and_text_values(store):
    numeric_uri = "urn:test:duck:bulk_numeric"
    text_uri = "urn:test:duck:bulk_text"
    df = pl.DataFrame(
        {
            "ref_uri": [numeric_uri, numeric_uri, text_uri, text_uri],
            "ts": [_utc(2024, 2, 4), _utc(2024, 2, 5), _utc(2024, 2, 4), _utc(2024, 2, 5)],
            "value": [1.5, 2.5, "1.5", "ok"],
            "value_kind": ["numeric", "numeric", "text", "text"],
        },
        schema={
            "ref_uri": pl.Utf8,
            "ts": pl.Datetime("us", "UTC"),
            "value": pl.Object,
            "value_kind": pl.Utf8,
        },
    )

    assert store.bulk_insert_polars(df) == 4
    stored = store.sql_query(
        f"""
        SELECT ref_uri, numeric_value, text_value
        FROM timeseries
        WHERE ref_uri IN ('{numeric_uri}', '{text_uri}')
        ORDER BY ref_uri, ts
        """
    )["rows"]
    assert stored == [
        [numeric_uri, 1.5, None],
        [numeric_uri, 2.5, None],
        [text_uri, None, "1.5"],
        [text_uri, None, "ok"],
    ]


@pytest.mark.unit
def test_bulk_insert_polars_appends_rows_in_stream_time_order(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "ordered_insert.duckdb", recreate=True)
    try:
        df = pl.DataFrame(
            {
                "ref_uri": ["urn:test:duck:z", "urn:test:duck:a", "urn:test:duck:z", "urn:test:duck:a"],
                "ts": [_utc(2024, 2, 2), _utc(2024, 2, 2), _utc(2024, 2, 1), _utc(2024, 2, 1)],
                "value": [4.0, 2.0, 3.0, 1.0],
                "value_kind": ["numeric", "numeric", "numeric", "numeric"],
            }
        )

        assert s.bulk_insert_polars(df) == 4

        stored = s.sql_query(
            """
            SELECT ref_uri, ts, numeric_value
            FROM timeseries
            ORDER BY rowid
            """
        )["rows"]
    finally:
        s.close()

    assert stored == [
        ["urn:test:duck:a", _utc(2024, 2, 1).replace(tzinfo=None), 1.0],
        ["urn:test:duck:a", _utc(2024, 2, 2).replace(tzinfo=None), 2.0],
        ["urn:test:duck:z", _utc(2024, 2, 1).replace(tzinfo=None), 3.0],
        ["urn:test:duck:z", _utc(2024, 2, 2).replace(tzinfo=None), 4.0],
    ]


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
def test_timeseries_uses_registered_numeric_kind_for_all_null_values(store):
    uri = "urn:test:duck:ts_null_numeric"
    store.ensure_stream_ref(None, "src-null", "numeric-null", ref_uri=uri, value_kind="numeric")
    store.upsert_rows(uri, [(_utc(2024, 3, 4), None)], value_kind="numeric")

    batch = list(store.timeseries(uri))[0]

    assert batch.schema.field("value").type == pa.float64()
    assert batch.to_pydict()["value"] == [None]


@pytest.mark.unit
def test_timeseries_uses_registered_text_kind_for_parseable_text(store):
    uri = "urn:test:duck:ts_parseable_text"
    store.ensure_stream_ref(None, "src-text", "text-number", ref_uri=uri, value_kind="text")
    store.upsert_rows(uri, [(_utc(2024, 3, 5), "1.5")], value_kind="text")

    batch = list(store.timeseries(uri))[0]

    assert batch.schema.field("value").type == pa.string()
    assert batch.to_pydict()["value"] == ["1.5"]


@pytest.mark.unit
def test_timeseries_with_start_end(store):
    uri = "urn:test:duck:ts_range"
    store.upsert_rows(uri, [
        (_utc(2024, 4, 1), "A"),
        (_utc(2024, 4, 5), "B"),
        (_utc(2024, 4, 10), "C"),
    ], value_kind="text")
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


@pytest.mark.unit
def test_timeseries_iterator_survives_other_queries(store):
    uri = "urn:test:duck:ts_lifecycle"
    store.upsert_rows(uri, [(_utc(2024, 6, 1), "1"), (_utc(2024, 6, 2), "2")])
    batches_iter = store.timeseries(uri)
    info = store.timeseries_info(uri)
    assert info.row_count == 2
    vals = [v for b in batches_iter for v in b.to_pydict()["value"]]
    assert vals == ["1", "2"]


# ---- timeseries_info ----

@pytest.mark.unit
def test_timeseries_info(store):
    uri = "urn:test:duck:info"
    store.upsert_rows(uri, [
        (_utc(2024, 7, 1), "x"),
        (_utc(2024, 7, 31), "y"),
    ], value_kind="text")
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


# ---- stream refs ----

@pytest.mark.unit
def test_ensure_stream_ref_returns_ref_uri(store):
    ref_uri = store.ensure_stream_ref("urn:test:duck:sh1", "src1", "ref1")
    assert isinstance(ref_uri, str) and len(ref_uri) > 0


@pytest.mark.unit
def test_ensure_stream_ref_defaults_value_kind_to_text(store):
    ref_uri = store.ensure_stream_ref("urn:test:duck:sh_text_default", "src-text-default", "ref")
    rows = store.sql_query(
        f"SELECT value_kind FROM streams WHERE ref_uri = '{ref_uri}'"
    )["rows"]
    assert rows == [["text"]]


@pytest.mark.unit
def test_ensure_stream_ref_deterministic(store):
    ref_uri1 = store.ensure_stream_ref("urn:test:duck:sh2", "src2", "ref2")
    ref_uri2 = store.ensure_stream_ref("urn:test:duck:sh2", "src2", "ref2")
    assert ref_uri1 == ref_uri2


@pytest.mark.unit
def test_resolve_storage_key(store):
    uri = "urn:test:duck:rsk"
    ref_uri = store.ensure_stream_ref(uri, "s", "r")
    assert store.resolve_storage_key(uri) == ref_uri


@pytest.mark.unit
def test_resolve_storage_key_unregistered(store):
    uri = "urn:test:duck:unregistered"
    assert store.resolve_storage_key(uri) == uri


@pytest.mark.unit
def test_resolve_storage_keys_batch(store):
    uris = ["urn:test:duck:rsk_batch_a", "urn:test:duck:rsk_batch_b"]
    ref_uri_a = store.ensure_stream_ref(uris[0], "sa", "ra")
    ref_uri_b = store.ensure_stream_ref(uris[1], "sb", "rb")
    result = store.resolve_storage_keys(uris)
    assert result[uris[0]] == ref_uri_a
    assert result[uris[1]] == ref_uri_b


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
    s.upsert_rows(uri, [(_utc(2024, 1, 1), "committed")], value_kind="text")
    s.commit()
    info = s.timeseries_info(uri)
    assert info.row_count == 1
    s.close()


@pytest.mark.unit
def test_rollback(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "tx_rollback.duckdb")
    uri = "urn:test:duck:tx_rollback"
    s.begin()
    s.upsert_rows(uri, [(_utc(2024, 1, 1), "should_vanish")], value_kind="text")
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
    s.upsert_rows("urn:x", [(_utc(2024, 1, 1), "v")], value_kind="text")
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
