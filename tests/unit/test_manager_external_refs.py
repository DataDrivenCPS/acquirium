from __future__ import annotations

from rdflib import URIRef

import pytest

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_handle


class _StubGraphStore:
    def __init__(self, rows):
        self._rows = rows

    def sparql_query(self, query: str, use_union: bool = True) -> dict:
        return {"rows": self._rows}


class _StubTimescale:
    def __init__(self) -> None:
        self.calls = []

    def ensure_stream_handle(self, point_uri: str, source_id: str, ref_name: str, handle=None):
        self.calls.append(
            {
                "point_uri": point_uri,
                "source_id": source_id,
                "ref_name": ref_name,
                "handle": handle,
            }
        )
        return str(handle) if handle is not None else ""


def _bare_manager(rows) -> tuple[Manager, _StubTimescale]:
    mgr = Manager.__new__(Manager)
    mgr.graph_store = _StubGraphStore(rows)
    ts = _StubTimescale()
    mgr.timescale = ts
    return mgr, ts


def test_sync_stream_handles_accepts_canonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    ref_uri = compute_handle(source_id, ref_name)

    mgr, ts = _bare_manager([(point_uri, ref_uri, source_id, ref_name)])

    count = mgr._sync_stream_handles_from_graph()

    assert count == 1
    assert len(ts.calls) == 1
    assert ts.calls[0]["handle"] == URIRef(str(ref_uri))


def test_sync_stream_handles_rejects_noncanonical_reference_uri():
    point_uri = "urn:test:point"
    source_id = "demo-source"
    ref_name = "cpu_percent"
    bad_ref_uri = URIRef("urn:acquirium#not-the-canonical-handle")

    mgr, ts = _bare_manager([(point_uri, bad_ref_uri, source_id, ref_name)])

    with pytest.raises(ValueError, match="Managed reference URI mismatch"):
        mgr._sync_stream_handles_from_graph()

    assert ts.calls == []
