from __future__ import annotations

from rdflib import URIRef

import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_stream_uri


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
        for property_uri, source_id, stream_name, stream_uri, value_kind in refs:
            self.calls.append(
                {
                    "property_uri": property_uri,
                    "source_id": source_id,
                    "stream_name": stream_name,
                    "stream_uri": stream_uri,
                    "value_kind": value_kind,
                }
            )
            out.append(str(stream_uri) if stream_uri is not None else "")
        return out


def _bare_manager(rows) -> tuple[Manager, _StubTimescale]:
    mgr = Manager.__new__(Manager)
    mgr.graph_store = _StubGraphStore(rows)
    ts = _StubTimescale()
    mgr.timeseries_store = ts
    return mgr, ts


def test_sync_stream_refs_accepts_canonical_reference_uri():
    property_uri = "urn:test:point"
    source_id = "demo-source"
    stream_name = "cpu_percent"
    stream_uri = compute_stream_uri(source_id, stream_name)

    mgr, ts = _bare_manager([(property_uri, stream_uri, source_id, stream_name, "numeric")])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert len(ts.calls) == 1
    assert ts.calls[0]["stream_uri"] == URIRef(str(stream_uri))
    assert ts.calls[0]["property_uri"] == property_uri


def test_sync_stream_refs_accepts_standalone_reference_without_property_uri():
    source_id = "demo-source"
    stream_name = "cpu_percent"
    stream_uri = compute_stream_uri(source_id, stream_name)

    mgr, ts = _bare_manager([(None, stream_uri, source_id, stream_name, None)])

    count = mgr._sync_stream_refs_from_graph()

    assert count == 1
    assert ts.calls == [
        {
            "property_uri": None,
            "source_id": source_id,
            "stream_name": stream_name,
            "stream_uri": URIRef(str(stream_uri)),
            "value_kind": None,
        }
    ]


def test_sync_stream_refs_rejects_noncanonical_reference_uri():
    property_uri = "urn:test:point"
    source_id = "demo-source"
    stream_name = "cpu_percent"
    bad_stream_uri = URIRef("urn:acquirium#not-the-canonical-stream_uri")

    mgr, ts = _bare_manager([(property_uri, bad_stream_uri, source_id, stream_name, "numeric")])

    with pytest.raises(ValueError, match="Managed reference URI mismatch"):
        mgr._sync_stream_refs_from_graph()

    assert ts.calls == []
