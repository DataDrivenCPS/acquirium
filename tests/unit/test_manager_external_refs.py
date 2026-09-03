from __future__ import annotations

from rdflib import URIRef

import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_ref_uri


class _StubGraphStore:
    def __init__(self, rows):
        self._rows = rows

    def sparql_query(self, query: str, include_dependencies: bool = True, **kwargs) -> dict:
        return {"rows": self._rows}


class _StubTimescale:
    def __init__(self) -> None:
        self.calls = []

    def ensure_stream_refs(self, refs):
        out = []
        for point_uri, source_id, ref_name, ref_uri, value_kind in refs:
            self.calls.append(
                {
                    "point_uri": point_uri,
                    "source_id": source_id,
                    "ref_name": ref_name,
                    "ref_uri": ref_uri,
                    "value_kind": value_kind,
                }
            )
            out.append(str(ref_uri) if ref_uri is not None else "")
        return out


def _bare_manager(rows) -> tuple[Manager, _StubTimescale]:
    mgr = Manager.__new__(Manager)
    mgr.graph_store = _StubGraphStore(rows)
    ts = _StubTimescale()
    mgr.timeseries_store = ts
    return mgr, ts


def test_sync_stream_refs_accepts_canonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    ref_uri = compute_ref_uri(source_id, ref_name)

    mgr, ts = _bare_manager([(point_uri, ref_uri, source_id, ref_name, "numeric")])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert len(ts.calls) == 1
    assert ts.calls[0]["ref_uri"] == URIRef(str(ref_uri))
    assert ts.calls[0]["point_uri"] == point_uri


def test_sync_stream_refs_accepts_standalone_reference_without_point_uri():
    source_id = "demo-source"
    ref_name = "cpu_percent"
    ref_uri = compute_ref_uri(source_id, ref_name)

    mgr, ts = _bare_manager([(None, ref_uri, source_id, ref_name, None)])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert ts.calls == [
        {
            "point_uri": None,
            "source_id": source_id,
            "ref_name": ref_name,
            "ref_uri": URIRef(str(ref_uri)),
            "value_kind": "text",
        }
    ]


def test_sync_stream_refs_rejects_noncanonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    bad_ref_uri = URIRef("urn:acquirium#not-the-canonical-ref_uri")

    mgr, ts = _bare_manager([(point_uri, bad_ref_uri, source_id, ref_name, "numeric")])

    with pytest.raises(ValueError, match="Managed reference URI mismatch"):
        mgr._sync_stream_refs_from_graph()

    assert ts.calls == []
