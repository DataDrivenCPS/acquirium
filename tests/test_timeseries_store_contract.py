"""Shared contract tests for TimeseriesStore implementations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import polars as pl
import psycopg
import pytest

from acquirium.Storage.base import TimeseriesStore
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.timescale_store import TimescaleStore
from acquirium.internals.models import LogEntry, TimeIntervalModel


def _utc(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


@pytest.fixture(params=["duckdb", "timescale"])
def contract_store(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "contract.duckdb", recreate=True)
    else:
        try:
            store = TimescaleStore(dsn=pg_dsn, connect_timeout=2, recreate=False)
        except psycopg.OperationalError as exc:
            pytest.skip(f"TimescaleDB is not available: {exc}")

    assert isinstance(store, TimeseriesStore)
    yield store
    store.close()


@pytest.fixture
def contract_uri_prefix() -> str:
    return f"urn:test:contract:{uuid4()}"


def _values(store: TimeseriesStore, ref_uri: str, **kwargs):
    return [
        value
        for batch in store.timeseries(ref_uri, **kwargs)
        for value in batch.column("value").to_pylist()
    ]


def test_timeseries_mutation_and_query_contract(contract_store, contract_uri_prefix):
    store = contract_store
    ref_uri = f"{contract_uri_prefix}:numeric"

    assert isinstance(store.ensure_table(), str)

    assert store.upsert_rows(
        ref_uri,
        [(_utc(2026, 1, 1), 1.0), (_utc(2026, 1, 2), 2.0)],
        value_kind="numeric",
    ) == 2
    assert store.upsert_rows(ref_uri, [(_utc(2026, 1, 2), 20.0)], value_kind="numeric") == 1

    batches = list(store.timeseries(ref_uri, batch_size=1))
    assert sum(batch.num_rows for batch in batches) == 2
    assert batches[0].schema.names == ["ts", "value", "uri"]
    assert _values(store, ref_uri, order="asc") == [1.0, 20.0]
    assert _values(store, ref_uri, order="desc", limit=1) == [20.0]
    assert _values(store, ref_uri, start=_utc(2026, 1, 2), end=_utc(2026, 1, 2)) == [20.0]

    info = store.timeseries_info(ref_uri)
    assert info.row_count == 2
    assert info.earliest == _utc(2026, 1, 1)
    assert info.latest == _utc(2026, 1, 2)

    missing_ref = f"{contract_uri_prefix}:missing"
    batch_info = store.timeseries_info_batch([ref_uri, missing_ref])
    assert batch_info[ref_uri].row_count == 2
    assert batch_info[missing_ref].row_count == 0
    assert list(store.timeseries(missing_ref)) == []

    assert store.replace_rows(ref_uri, [(_utc(2026, 1, 3), 3.0)], value_kind="numeric") == 1
    assert _values(store, ref_uri) == [3.0]


def test_numeric_stream_can_store_and_query_text_fallback_rows(contract_store, contract_uri_prefix):
    store = contract_store
    ref_uri = f"{contract_uri_prefix}:mixed"
    store.ensure_stream_ref(None, contract_uri_prefix, "mixed", ref_uri=ref_uri, value_kind="numeric")

    assert store.upsert_rows(
        ref_uri,
        [
            (_utc(2026, 2, 1), "1.0"),
            (_utc(2026, 2, 2), "Manual Control"),
            (_utc(2026, 2, 3), 2.5),
        ],
        value_kind="numeric",
    ) == 3

    assert _values(store, ref_uri, order="asc") == [1.0, None, 2.5]
    assert _values(store, ref_uri, order="asc", value_mode="numeric") == [1.0, 2.5]
    assert _values(store, ref_uri, order="asc", value_mode="text") == ["Manual Control"]
    assert _values(store, ref_uri, order="asc", value_mode="coalesce") == [
        "1.0",
        "Manual Control",
        "2.5",
    ]


def test_bulk_insert_polars_contract(contract_store, contract_uri_prefix):
    store = contract_store
    ref_uri = f"{contract_uri_prefix}:bulk"
    ts = _utc(2026, 2, 1)
    df = pl.DataFrame(
        {
            "ref_uri": [ref_uri, ref_uri, ref_uri],
            "ts": [ts, ts, ts + timedelta(hours=1)],
            "value": [1.0, 2.0, 3.0],
            "value_kind": ["numeric", "numeric", "numeric"],
        }
    )

    assert store.bulk_insert_polars(df) == 2
    assert _values(store, ref_uri) == [2.0, 3.0]

    empty = pl.DataFrame(
        {"ref_uri": [], "ts": [], "value": [], "value_kind": []},
        schema={
            "ref_uri": pl.Utf8,
            "ts": pl.Datetime("us", "UTC"),
            "value": pl.Float64,
            "value_kind": pl.Utf8,
        },
    )
    assert store.bulk_insert_polars(empty) == 0


def test_stream_registry_contract(contract_store, contract_uri_prefix):
    store = contract_store
    point_uri = f"{contract_uri_prefix}:point"

    ref_uri = str(store.ensure_stream_ref(point_uri, contract_uri_prefix, "temperature", value_kind="numeric"))
    assert ref_uri
    assert store.stream_value_kind(ref_uri) == "numeric"
    assert str(store.ensure_stream_ref(
        point_uri,
        contract_uri_prefix,
        "temperature",
        value_kind="numeric",
    )) == ref_uri
    assert str(store.ensure_stream_ref(
        f"{contract_uri_prefix}:other-point",
        f"{contract_uri_prefix}:other",
        "temperature",
    )) != ref_uri
    assert store.resolve_storage_key(point_uri) == ref_uri
    assert store.resolve_storage_key(f"{contract_uri_prefix}:unregistered") == f"{contract_uri_prefix}:unregistered"

    other_point_uri = f"{contract_uri_prefix}:point2"
    other_ref_uri = str(store.ensure_stream_ref(other_point_uri, contract_uri_prefix, "humidity"))
    assert store.stream_value_kind(other_ref_uri) == "text"
    assert store.resolve_storage_keys([point_uri, other_point_uri, "urn:test:contract:unregistered"]) == {
        point_uri: ref_uri,
        other_point_uri: other_ref_uri,
        "urn:test:contract:unregistered": "urn:test:contract:unregistered",
    }


def test_list_streams_contract(contract_store, contract_uri_prefix):
    store = contract_store
    sid = f"{contract_uri_prefix}:lsrc"

    # Empty table → empty result.
    assert store.list_streams(bound=False) == []

    # Two unbound, one bound.
    ref_a = str(store.ensure_stream_ref(None, sid, "alpha"))
    ref_b = str(store.ensure_stream_ref(None, sid, "beta"))
    ref_c = str(store.ensure_stream_ref(f"{contract_uri_prefix}:p-c", sid, "gamma"))

    all_rows = store.list_streams()
    refs = {r["ref_uri"] for r in all_rows}
    assert {ref_a, ref_b, ref_c} <= refs
    assert all(set(r.keys()) >= {"ref_uri", "point_uri", "source_id", "ref_name", "value_kind"} for r in all_rows)

    unbound = store.list_streams(bound=False)
    assert {r["ref_uri"] for r in unbound if r["source_id"] == sid} == {ref_a, ref_b}
    assert all(r["point_uri"] is None for r in unbound)

    bound = store.list_streams(bound=True)
    assert ref_c in {r["ref_uri"] for r in bound}
    assert all(r["point_uri"] is not None for r in bound)

    # Pagination — results are ordered by (source_id, ref_name) so within our
    # source ('alpha', 'beta', 'gamma') we can pick a deterministic slice.
    paged = [r for r in store.list_streams(limit=2) if r["source_id"] == sid]
    assert len(paged) <= 2
    paged_offset = [r for r in store.list_streams(limit=2, offset=1) if r["source_id"] == sid]
    # offset+limit slicing should not return the first row of the source twice.
    if paged and paged_offset:
        assert paged[0]["ref_uri"] != paged_offset[0]["ref_uri"]


def test_logs_contract(contract_store, contract_uri_prefix):
    store = contract_store
    point_uri = f"{contract_uri_prefix}:logged"

    store.insert_log(
        LogEntry(
            point_uri=point_uri,
            timestamp=_utc(2026, 3, 1),
            period=TimeIntervalModel(),
            message="no observation period",
        )
    )
    store.insert_log(
        LogEntry(
            point_uri=point_uri,
            timestamp=_utc(2026, 3, 2),
            period=TimeIntervalModel(start=_utc(2026, 3, 1, 12), end=_utc(2026, 3, 3)),
            message="observed",
        )
    )

    logs = store.query_logs(point_uri)
    assert [log.message for log in logs] == ["no observation period", "observed"]
    assert logs[0].period.start is None

    time_filtered = store.query_logs(
        point_uri,
        log_time_interval=TimeIntervalModel(start=_utc(2026, 3, 2), end=_utc(2026, 3, 2)),
    )
    assert [log.message for log in time_filtered] == ["observed"]

    obs_filtered = store.query_logs(
        point_uri,
        obs_time_interval=TimeIntervalModel(start=_utc(2026, 3, 2), end=_utc(2026, 3, 2, 1)),
    )
    assert [log.message for log in obs_filtered] == ["observed"]

    assert store.delete_logs(point_uri) is True
    assert store.query_logs(point_uri) == []


def test_transaction_and_sql_query_contract(contract_store, contract_uri_prefix):
    store = contract_store
    ref_uri = f"{contract_uri_prefix}:tx"

    store.begin()
    store.upsert_rows(ref_uri, [(_utc(2026, 4, 1), "rolled back")], value_kind="text")
    store.rollback()
    assert store.timeseries_info(ref_uri).row_count == 0

    store.begin()
    store.upsert_rows(ref_uri, [(_utc(2026, 4, 1), "committed")], value_kind="text")
    store.commit()
    assert _values(store, ref_uri) == ["committed"]

    result = store.sql_query("SELECT 1 AS contract_value")
    assert result["columns"] == ["contract_value"]
    assert result["rows"] == [[1]] or result["rows"] == [(1,)]
