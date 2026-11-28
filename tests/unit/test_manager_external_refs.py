from __future__ import annotations

from types import SimpleNamespace

from rdflib import URIRef

import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_stream_uri


class _StubGraphStore:
    def __init__(self, rows):
        self._rows = rows

    def graph_status(self):
        return {"source_version": 1}

    def sparql_query(self, query: str, include_dependencies: bool = True, **kwargs) -> dict:
        return {"rows": self._rows}


class _StubEngine:
    """An engine with no published plan, so references have no replacements."""

    def snapshot(self):
        return SimpleNamespace(plan=None)

    def plan_at(self, epoch):
        return None


def _bare_manager(rows):
    manager = Manager.__new__(Manager)
    manager.graph_store = _StubGraphStore(rows)
    manager.core = SimpleNamespace(engine=_StubEngine())
    manager.apps = SimpleNamespace(plan_at=lambda epoch=None: {"instances": []})
    return manager


def test_sync_stream_refs_accepts_canonical_reference_uri():
    property_uri = "urn:test:point"
    source_id = "demo-source"
    stream_name = "cpu_percent"
    stream_uri = compute_stream_uri(source_id, stream_name)

    mgr = _bare_manager([(property_uri, stream_uri, source_id, stream_name, "numeric")])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert mgr._stream_refs[str(stream_uri)]["source_id"] == source_id
    assert mgr._point_refs[property_uri] == {str(stream_uri)}


def test_sync_stream_refs_accepts_standalone_reference_without_property_uri():
    source_id = "demo-source"
    stream_name = "cpu_percent"
    stream_uri = compute_stream_uri(source_id, stream_name)

    mgr = _bare_manager([(None, stream_uri, source_id, stream_name, None)])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert mgr._stream_refs[str(stream_uri)]["value_kind"] == "undeclared"
    assert mgr._point_refs == {}


def test_sync_stream_refs_rejects_noncanonical_reference_uri():
    property_uri = "urn:test:point"
    source_id = "demo-source"
    stream_name = "cpu_percent"
    bad_stream_uri = URIRef("urn:acquirium#not-the-canonical-stream-uri")

    mgr = _bare_manager([(property_uri, bad_stream_uri, source_id, stream_name, "numeric")])

    with pytest.raises(ValueError, match="Managed reference URI mismatch"):
        mgr._sync_stream_refs_from_graph()


def test_reads_resync_references_after_a_graph_change():
    property_uri = "urn:test:point"
    old_ref, new_ref = compute_stream_uri("demo", "old"), compute_stream_uri("demo", "new")
    mgr = _bare_manager([(property_uri, old_ref, "demo", "old", None)])
    mgr._refs_revision = -1
    assert mgr.resolve_storage_keys([property_uri]) == {property_uri: str(old_ref)}

    # The point now names another stream, in a newer graph revision.
    mgr.graph_store._rows = [(property_uri, new_ref, "demo", "new", None)]
    assert mgr.resolve_storage_keys([property_uri]) == {property_uri: str(old_ref)}
    mgr.graph_store.graph_status = lambda: {"source_version": 2}
    assert mgr.resolve_storage_keys([property_uri]) == {property_uri: str(new_ref)}
