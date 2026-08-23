from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from acquirium.Server.manager import Manager
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import PublicationConflict
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri


@pytest.fixture
def mgr(tmp_path):
    """A real Manager-shaped object with only the timeseries/publication
    layers wired -- enough to exercise insert_timeseries* end to end without
    the full Manager.__init__ (graph store, embedding indexes, ...)."""
    store = DuckDBStore(tmp_path / "mgr.duckdb", recreate=True)
    m = Manager.__new__(Manager)
    m.timescale = store
    m.publication = PublicationDuckDB(store)
    m.epoch_materialization = MagicMock()
    m.notify_service_changes = lambda *args, **kwargs: None
    yield m
    store.close()


def _register(mgr, source_id: str, ref_name: str, value_kind: str) -> str:
    ref_uri = str(compute_ref_uri(source_id, ref_name))
    mgr.timescale.ensure_stream_ref(None, source_id, ref_name, ref_uri, value_kind=value_kind)
    return ref_uri


def test_insert_timeseries_batch_publishes_one_atomic_mutation_set(mgr):
    temp_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    state_uri = _register(mgr, "source/file.csv", "state/value", "text")

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    receipt = mgr.insert_timeseries_batch(
        "source/file.csv",
        {
            "temp": [(ts, 72.4)],
            "state/value": [(ts, "OK"), (ts.replace(hour=1), None)],
        },
    )

    assert receipt.row_count == 3
    assert set(receipt.versions) == {temp_uri, state_uri}

    temp_values = [v for b in mgr.timescale.timeseries(temp_uri) for v in b.column("value").to_pylist()]
    assert temp_values == [72.4]
    state_values = [v for b in mgr.timescale.timeseries(state_uri) for v in b.column("value").to_pylist()]
    assert sorted(state_values, key=lambda v: v or "") == [None, "OK"]


def test_insert_timeseries_arrow_publishes_computed_ref_uris(mgr):
    import polars as pl

    temp_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    state_uri = _register(mgr, "source/file.csv", "state/value", "text")

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1), ts.replace(hour=2)],
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

    receipt = mgr.insert_timeseries_arrow("source/file.csv", df.to_arrow())

    assert receipt.row_count == 3
    assert set(receipt.versions) == {temp_uri, state_uri}
    temp_values = {v for b in mgr.timescale.timeseries(temp_uri) for v in b.column("value").to_pylist()}
    assert temp_values == {72.4, 73.1}


def test_insert_timeseries_arrow_ignores_input_value_kind_column(mgr):
    """The stream's *registered* value_kind wins over an incoming value_kind
    column -- a writer cannot silently retype an already-registered stream."""
    import polars as pl

    state_uri = _register(mgr, "source/file.csv", "state", "numeric")

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

    receipt = mgr.insert_timeseries_arrow("source/file.csv", df.to_arrow())
    assert receipt.row_count == 2
    # "ON" is not parseable as numeric, so it falls back to text storage even
    # under the registered "numeric" kind; value_mode="coalesce" surfaces
    # both the numeric and the text-fallback column on one read.
    values = {
        v for b in mgr.timescale.timeseries(state_uri, value_mode="coalesce")
        for v in b.column("value").to_pylist()
    }
    assert values == {"1.0", "ON"}


def test_insert_timeseries_replace_tombstones_stale_rows_atomically(mgr):
    ref_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    t0, t1, t2 = (
        datetime(2026, 4, 28, hour=h, tzinfo=timezone.utc) for h in (0, 1, 2)
    )
    mgr.insert_timeseries(source_id="source/file.csv", ref_name="temp", rows=[(t0, 1.0), (t1, 2.0)])

    receipt = mgr.insert_timeseries(
        source_id="source/file.csv", ref_name="temp", rows=[(t1, 20.0), (t2, 3.0)], replace=True,
    )
    # One atomic publication for the whole replace: 2 upserts + 1 tombstone.
    assert receipt.row_count == 3

    rows = sorted(
        (ts, v)
        for b in mgr.timescale.timeseries(ref_uri)
        for ts, v in zip(b.column("ts").to_pylist(), b.column("value").to_pylist())
    )
    assert rows == [(t1, 20.0), (t2, 3.0)]


def test_delete_timeseries_explicit_timestamps(mgr):
    ref_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    t0, t1 = (datetime(2026, 4, 28, hour=h, tzinfo=timezone.utc) for h in (0, 1))
    mgr.insert_timeseries(source_id="source/file.csv", ref_name="temp", rows=[(t0, 1.0), (t1, 2.0)])

    receipt = mgr.delete_timeseries(ref_uri, timestamps=[t0])
    assert receipt.row_count == 1

    remaining = [
        ts for b in mgr.timescale.timeseries(ref_uri) for ts in b.column("ts").to_pylist()
    ]
    assert remaining == [t1]


def test_delete_timeseries_range(mgr):
    ref_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    t0, t1, t2 = (
        datetime(2026, 4, 28, hour=h, tzinfo=timezone.utc) for h in (0, 1, 2)
    )
    mgr.insert_timeseries(
        source_id="source/file.csv", ref_name="temp", rows=[(t0, 1.0), (t1, 2.0), (t2, 3.0)],
    )

    receipt = mgr.delete_timeseries(ref_uri, start=t0, end=t1)
    assert receipt.row_count == 2

    remaining = [
        ts for b in mgr.timescale.timeseries(ref_uri) for ts in b.column("ts").to_pylist()
    ]
    assert remaining == [t2]


def test_insert_timeseries_batch_publication_conflict_leaves_no_partial_state(mgr):
    """Reusing a publication_id with a different payload rejects the whole
    write; a fresh publish for the same stream afterwards is unaffected."""
    ref_uri = _register(mgr, "source/file.csv", "temp", "numeric")
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)

    mgr.insert_timeseries_batch(
        "source/file.csv", {"temp": [(ts, 1.0)]}, publication_id="dup-id",
    )
    with pytest.raises(PublicationConflict):
        mgr.insert_timeseries_batch(
            "source/file.csv", {"temp": [(ts, 2.0)]}, publication_id="dup-id",
        )

    values = [v for b in mgr.timescale.timeseries(ref_uri) for v in b.column("value").to_pylist()]
    assert values == [1.0]
