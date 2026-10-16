from __future__ import annotations

from rdflib import URIRef

import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_ref_uri


class _StubGraphStore:
    def __init__(self, rows):
        self._rows = rows

    def graph_status(self):
        return {"source_version": 1}

    def sparql_query(self, query: str, include_dependencies: bool = True, **kwargs) -> dict:
        return {"rows": self._rows}


def _bare_manager(rows):
    manager = Manager.__new__(Manager)
    manager.graph_store = _StubGraphStore(rows)
    return manager


def test_sync_stream_refs_accepts_canonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    ref_uri = compute_ref_uri(source_id, ref_name)

    mgr = _bare_manager([(point_uri, ref_uri, source_id, ref_name, "numeric")])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert mgr._stream_refs[str(ref_uri)]["source_id"] == source_id
    assert mgr._point_refs[point_uri] == {str(ref_uri)}


def test_sync_stream_refs_accepts_standalone_reference_without_point_uri():
    source_id = "demo-source"
    ref_name = "cpu_percent"
    ref_uri = compute_ref_uri(source_id, ref_name)

    mgr = _bare_manager([(None, ref_uri, source_id, ref_name, None)])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert mgr._stream_refs[str(ref_uri)]["value_kind"] == "numeric"
    assert mgr._point_refs == {}


def test_sync_stream_refs_rejects_noncanonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    bad_ref_uri = URIRef("urn:acquirium#not-the-canonical-ref_uri")

    mgr = _bare_manager([(point_uri, bad_ref_uri, source_id, ref_name, "numeric")])

    with pytest.raises(ValueError, match="Managed reference URI mismatch"):
        mgr._sync_stream_refs_from_graph()
