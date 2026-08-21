"""Server-side stream registration: resolution, reconciliation, writing.

Built on a bare Manager with stubbed collaborators, in the style of
``test_manager_external_refs.py`` — no server, no graph store, no QUDT load.
"""
from __future__ import annotations

import pytest
from rdflib import Graph, Literal, URIRef

from acquirium.internals.internals_namespaces import (
    HAS_UNIT,
    UNIT_MISMATCH_ALLOWED,
)
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.reconcile import StreamMetadataConflict
from acquirium.Server.manager import Manager

DEG_C = "http://qudt.org/vocab/unit/DEG_C"
DEG_F = "http://qudt.org/vocab/unit/DEG_F"
MGL = "http://qudt.org/vocab/unit/MilliGM-PER-L"
POINT = "urn:test:point:temp"


class _StubGraphStore:
    """Answers the point-metadata query and records what got written."""

    def __init__(self, point_values: dict[str, dict[str, str]] | None = None):
        self._point_values = point_values or {}
        self.inserted: list[tuple[str, Graph]] = []

    def sparql_query(self, query: str, **kwargs) -> dict:
        fields = ["unit", "quantity_kind", "medium", "substance"]
        rows = []
        for point, values in self._point_values.items():
            if f"<{point}>" not in query:
                continue
            rows.append([point] + [values.get(f) for f in fields])
        return {"columns": ["point"] + fields, "rows": rows}


class _StubConverter:
    def __init__(self, verdict: str = "incompatible"):
        self._verdict = verdict
        self.calls: list[tuple[str, str]] = []

    def compatibility_verdict(self, a, b) -> str:
        self.calls.append((str(a), str(b)))
        return self._verdict


def _manager(*, point_values=None, verdict="incompatible", resolutions=None):
    mgr = Manager.__new__(Manager)
    mgr.graph_store = _StubGraphStore(point_values)
    converter = _StubConverter(verdict)
    mgr._ensure_qudt_converter = lambda: converter  # type: ignore[method-assign]
    mgr.resolve_record = lambda record, top_k=1, min_score=0.6: {  # type: ignore[method-assign]
        field: ([{"uri": (resolutions or {}).get(raw, raw)}]
                if (resolutions or {}).get(raw, None) is not None else [])
        for field, (raw, _kind) in record.items()
    }
    written: list[tuple[str, Graph]] = []

    def _insert_graph(graph, *, format="turtle", replace=False, source_id):
        written.append((source_id, graph))

    mgr.insert_graph = _insert_graph  # type: ignore[method-assign]
    mgr._written = written  # type: ignore[attr-defined]
    return mgr, converter


def _stream(**overrides) -> dict:
    return {"source_id": "svcw", "ref_name": "T-101", **overrides}


def _graph_for(mgr) -> Graph:
    assert len(mgr._written) == 1
    return mgr._written[0][1]


# --------------------------------------------------------------- basics

def test_empty_batch_writes_nothing():
    mgr, _ = _manager()
    assert mgr.register_streams([]) == {"ok": True, "registered": 0, "warnings": []}
    assert mgr._written == []


def test_writes_one_graph_per_source():
    mgr, _ = _manager()
    mgr.register_streams([
        _stream(source_id="a", ref_name="x"),
        _stream(source_id="a", ref_name="y"),
        _stream(source_id="b", ref_name="z"),
    ])
    assert sorted(source for source, _ in mgr._written) == ["a", "b"]


def test_missing_source_id_raises():
    mgr, _ = _manager()
    with pytest.raises(ValueError, match="non-empty source_id"):
        mgr.register_streams([{"ref_name": "x"}])


# ------------------------------------------------------------ resolution

def test_free_text_is_resolved_before_writing():
    mgr, _ = _manager(resolutions={"degC": DEG_C})
    mgr.register_streams([_stream(unit="degC")])
    assert (compute_ref_uri("svcw", "T-101"), HAS_UNIT, URIRef(DEG_C)) in _graph_for(mgr)


def test_uri_values_bypass_resolution():
    mgr, _ = _manager(resolutions={})
    mgr.register_streams([_stream(unit=DEG_C)])
    assert (compute_ref_uri("svcw", "T-101"), HAS_UNIT, URIRef(DEG_C)) in _graph_for(mgr)


def test_unresolvable_text_raises_rather_than_storing_a_literal():
    """Passing prose means "understand this"; a silent literal would only
    surface later as a query that mysteriously matches nothing."""
    mgr, _ = _manager(resolutions={})
    with pytest.raises(ValueError, match="could not resolve"):
        mgr.register_streams([_stream(unit="widgets per fortnight")])
    assert mgr._written == []


def test_unresolvable_error_names_the_stream_and_the_field():
    mgr, _ = _manager(resolutions={})
    with pytest.raises(ValueError) as excinfo:
        mgr.register_streams([_stream(unit="widgets")])
    message = str(excinfo.value)
    assert "'svcw'" in message and "'T-101'" in message and "widgets" in message


def test_streams_without_semantics_skip_resolution_entirely():
    mgr, _ = _manager(resolutions={})
    mgr.register_streams([_stream(value_kind="numeric")])
    assert len(mgr._written) == 1


# -------------------------------------------------------- reconciliation

def test_no_point_means_no_reconciliation():
    mgr, converter = _manager(verdict="incompatible")
    mgr.register_streams([_stream(unit=MGL)])
    assert converter.calls == []


def test_agreeing_point_is_accepted():
    mgr, converter = _manager(point_values={POINT: {"unit": DEG_C}})
    mgr.register_streams([_stream(unit=DEG_C, point_uri=POINT)])
    assert converter.calls == []  # equal values short-circuit before the verdict


def test_convertible_difference_is_accepted_and_nothing_extra_is_written():
    mgr, _ = _manager(point_values={POINT: {"unit": DEG_C}}, verdict="convertible")
    result = mgr.register_streams([_stream(unit=DEG_F, point_uri=POINT)])
    assert result["warnings"] == []
    graph = _graph_for(mgr)
    ref = compute_ref_uri("svcw", "T-101")
    # The reference keeps its own unit; the point is untouched.
    assert (ref, HAS_UNIT, URIRef(DEG_F)) in graph
    assert list(graph.objects(URIRef(POINT), HAS_UNIT)) == []


def test_incompatible_unit_raises_and_writes_nothing():
    mgr, _ = _manager(point_values={POINT: {"unit": DEG_C}}, verdict="incompatible")
    with pytest.raises(StreamMetadataConflict, match="allow_unit_mismatch"):
        mgr.register_streams([_stream(unit=MGL, point_uri=POINT)])
    assert mgr._written == []


def test_unknown_compatibility_raises():
    """The fail-open hazard — `are_compatible` would say yes here."""
    mgr, _ = _manager(point_values={POINT: {"unit": DEG_C}}, verdict="unknown")
    with pytest.raises(StreamMetadataConflict):
        mgr.register_streams([_stream(unit=MGL, point_uri=POINT)])


def test_allow_unit_mismatch_registers_and_records_the_override():
    mgr, _ = _manager(point_values={POINT: {"unit": DEG_C}}, verdict="incompatible")
    result = mgr.register_streams([
        _stream(unit=MGL, point_uri=POINT, allow_unit_mismatch=True)
    ])
    assert len(result["warnings"]) == 1
    ref = compute_ref_uri("svcw", "T-101")
    assert (ref, UNIT_MISMATCH_ALLOWED, Literal(True)) in _graph_for(mgr)


def test_a_conflicting_stream_rejects_the_whole_batch():
    """All-or-nothing: a partially applied batch would leave the graph in a
    state the caller never asked for."""
    mgr, _ = _manager(point_values={POINT: {"unit": DEG_C}}, verdict="incompatible")
    with pytest.raises(StreamMetadataConflict):
        mgr.register_streams([
            _stream(ref_name="fine"),
            _stream(ref_name="broken", unit=MGL, point_uri=POINT),
        ])
    assert mgr._written == []


def test_every_conflict_is_reported_together():
    mgr, _ = _manager(
        point_values={POINT: {"unit": DEG_C}, "urn:test:point:b": {"unit": DEG_C}},
        verdict="incompatible",
    )
    with pytest.raises(StreamMetadataConflict) as excinfo:
        mgr.register_streams([
            _stream(ref_name="a", unit=MGL, point_uri=POINT),
            _stream(ref_name="b", unit=MGL, point_uri="urn:test:point:b"),
        ])
    assert "2 stream registration(s)" in str(excinfo.value)


def test_medium_conflict_ignores_the_unit_flag():
    mgr, _ = _manager(point_values={POINT: {"medium": "urn:m:Air"}})
    with pytest.raises(StreamMetadataConflict, match="medium"):
        mgr.register_streams([
            _stream(medium="urn:m:Water", point_uri=POINT, allow_unit_mismatch=True)
        ])


def test_quantity_kind_difference_only_warns():
    mgr, _ = _manager(point_values={POINT: {"quantity_kind": "urn:qk:B"}})
    result = mgr.register_streams([
        _stream(quantity_kind="urn:qk:A", point_uri=POINT)
    ])
    assert len(result["warnings"]) == 1
    assert len(mgr._written) == 1


def test_point_with_no_metadata_is_not_a_conflict():
    mgr, converter = _manager(point_values={POINT: {}})
    mgr.register_streams([_stream(unit=MGL, point_uri=POINT)])
    assert converter.calls == []
    assert len(mgr._written) == 1
