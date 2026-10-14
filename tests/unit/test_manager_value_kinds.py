"""Stream value kinds are resolved for a whole batch with one store query."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from acquirium.Server.manager import Manager
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_ref_uri


class _CountingStore:
    def __init__(self, kinds):
        self.kinds = kinds
        self.calls = []

    def stream_value_kinds(self, ref_uris):
        uris = list(ref_uris)
        self.calls.append(uris)
        return {uri: self.kinds[uri] for uri in uris if uri in self.kinds}

    def bulk_insert_polars(self, df):
        self.frame = df
        return len(df)


class _Graph:
    def __init__(self, published=1):
        self.published = published

    def graph_status(self):
        return {"published_version": self.published}


def _manager(store, graph=None):
    mgr = Manager.__new__(Manager)
    mgr.timeseries_store = store
    mgr.graph_store = graph or _Graph()
    mgr._refs_synced_revision = None
    mgr.resyncs = 0

    def resync():
        mgr.resyncs += 1
        return 0

    mgr._sync_stream_refs_from_graph = resync
    return mgr


def test_arrow_insert_resolves_all_streams_with_one_lookup():
    source = "plant"
    names = [f"p{i}" for i in range(50)]
    kinds = {str(compute_ref_uri(source, n)): "numeric" for n in names}
    store = _CountingStore(kinds)
    mgr = _manager(store)
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    table = pa.table({"ts": [ts] * 50, "ref_name": names, "value": [float(i) for i in range(50)]})

    mgr.insert_timeseries_arrow(source, table)

    assert len(store.calls) == 1
    assert sorted(store.calls[0]) == sorted(kinds)
    assert set(store.frame["value_kind"].to_list()) == {"numeric"}


def test_missing_stream_triggers_one_resync_then_rejects():
    store = _CountingStore({"urn:known": "numeric"})
    mgr = _manager(store)

    with pytest.raises(ValueError, match="urn:unknown is not registered"):
        mgr._registered_value_kinds(["urn:known", "urn:unknown"])

    assert mgr.resyncs == 1
    assert store.calls == [["urn:known", "urn:unknown"], ["urn:unknown"]]
    # The graph has not advanced since the resync, so a repeat does not rebuild again.
    with pytest.raises(ValueError):
        mgr._registered_value_kinds(["urn:unknown"])
    assert mgr.resyncs == 1


def test_single_lookup_delegates_to_the_batch_form():
    store = _CountingStore({"urn:a": "TEXT"})
    assert _manager(store)._registered_value_kind("urn:a") == "text"


def test_duckdb_store_batch_lookup(tmp_path):
    store = DuckDBStore(tmp_path / "kinds.duckdb")
    refs = [(None, "plant", f"p{i}", f"urn:plant/p{i}", "numeric" if i % 2 else "text") for i in range(10)]
    store.ensure_stream_refs(refs)

    kinds = store.stream_value_kinds(["urn:plant/p1", "urn:plant/p2", "urn:plant/p1", "urn:missing"])

    assert kinds == {"urn:plant/p1": "numeric", "urn:plant/p2": "text"}
    assert store.stream_value_kinds([]) == {}
    assert store.stream_value_kind("urn:plant/p3") == "numeric"
