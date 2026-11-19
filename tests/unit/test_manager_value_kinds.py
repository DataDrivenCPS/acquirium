"""Stream value kinds are resolved for a whole batch with one store query."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from acquirium.Server.manager import Manager
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.internals.models import compute_stream_uri


class _CountingStore:
    def __init__(self, kinds):
        self.kinds = kinds
        self.calls = []

    def stream_value_kinds(self, stream_uris):
        uris = list(stream_uris)
        self.calls.append(uris)
        return {uri: self.kinds[uri] for uri in uris if uri in self.kinds}

    def mark_streams_numeric(self, stream_uris):
        self.marked = list(stream_uris)
        for uri in self.marked:
            if self.kinds.get(uri) == "undeclared":
                self.kinds[uri] = "numeric"

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
    kinds = {str(compute_stream_uri(source, n)): "numeric" for n in names}
    store = _CountingStore(kinds)
    mgr = _manager(store)
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    table = pa.table({"ts": [ts] * 50, "stream_name": names, "value": [float(i) for i in range(50)]})

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


def _insert(mgr, source, rows):
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    names, values = zip(*rows)
    mgr.insert_timeseries_arrow(
        source, pa.table({"ts": [ts] * len(rows), "stream_name": list(names), "value": list(values)})
    )
    frame = mgr.timeseries_store.frame
    return dict(zip(frame["stream_uri"].to_list(), frame["value_kind"].to_list()))


def test_undeclared_stream_becomes_numeric_with_its_first_numeric_value():
    uri = {n: str(compute_stream_uri("s", n)) for n in ("num", "csv", "status", "forced")}
    store = _CountingStore({
        uri["num"]: "undeclared", uri["csv"]: "undeclared",
        uri["status"]: "undeclared", uri["forced"]: "text",
    })
    mgr = _manager(store)

    written = _insert(mgr, "s", [("num", "21.5"), ("csv", "Bad"), ("csv", " 12.5 "),
                                 ("status", "ON"), ("forced", "1")])

    assert written == {uri["num"]: "numeric", uri["csv"]: "numeric",
                       uri["status"]: "text", uri["forced"]: "text"}
    assert sorted(store.marked) == sorted([uri["num"], uri["csv"]])
    assert store.kinds[uri["status"]] == "undeclared"
    assert store.kinds[uri["forced"]] == "text"


def test_undeclared_text_stream_switches_to_numeric_later():
    uri = str(compute_stream_uri("s", "late"))
    store = _CountingStore({uri: "undeclared"})
    mgr = _manager(store)

    assert _insert(mgr, "s", [("late", "Bad")]) == {uri: "text"}
    assert store.kinds[uri] == "undeclared"
    assert _insert(mgr, "s", [("late", "3.0")]) == {uri: "numeric"}
    assert store.kinds[uri] == "numeric"


def test_store_keeps_an_inferred_kind_when_registered_again_without_one(tmp_path):
    store = DuckDBStore(str(tmp_path / "ts.duckdb"))
    uri = str(compute_stream_uri("s", "a"))
    store.ensure_stream_refs([(None, "s", "a", uri, None)])
    assert store.stream_value_kind(uri) == "undeclared"

    store.mark_streams_numeric([uri])
    store.ensure_stream_refs([(None, "s", "a", uri, None)])
    assert store.stream_value_kind(uri) == "numeric"

    store.ensure_stream_refs([(None, "s", "a", uri, "text")])
    store.mark_streams_numeric([uri])
    assert store.stream_value_kind(uri) == "text"
