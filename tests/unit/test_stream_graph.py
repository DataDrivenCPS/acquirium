"""The triples a stream registration produces.

The contract this pins: **semantics live on the external reference, never on
the point.** A point gets its type, its label and the link to the reference,
and nothing else — anything Acquirium wrote onto a point would be inside the
user's model, where a later insert_graph(replace=True) would drop it.
"""
from __future__ import annotations

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS

from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE,
    HAS_EXTERNAL_REFERENCE,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    OF_SUBSTANCE,
    STORED_AT,
    UNIT_MISMATCH_ALLOWED,
    VIRTUAL_POINT,
)
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.stream_graph import SEMANTIC_PREDICATES, build_stream_triples

SOURCE = "demo-source"
POINT = URIRef("urn:test:point:temp")
DEG_C = "http://qudt.org/vocab/unit/DEG_C"
TEMPERATURE = "http://qudt.org/vocab/quantitykind/Temperature"
WATER = "urn:nawi-water-ontology#Water"
CHLORINE = "urn:nawi-water-ontology#Chlorine"
_TEST_PROP = URIRef("urn:test:prop:fileLocation")

FULL_SEMANTICS = {
    "unit": DEG_C,
    "quantity_kind": TEMPERATURE,
    "medium": WATER,
    "substance": CHLORINE,
}


def build(stream: dict, resolved: dict | None = None) -> Graph:
    g = Graph()
    build_stream_triples(g, stream, resolved or {})
    return g


def ref_of(ref_name: str = "temp") -> URIRef:
    return compute_ref_uri(SOURCE, ref_name)


# ------------------------------------------------------------- identity

def test_reference_carries_identity():
    g = build({"source_id": SOURCE, "ref_name": "temp", "value_kind": "numeric"})
    ref = ref_of()
    assert (ref, ACQUIRIUM_SOURCE_ID, Literal(SOURCE)) in g
    assert (ref, ACQUIRIUM_REF_NAME, Literal("temp")) in g
    assert (ref, ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g
    assert (ref, STORED_AT, ACQUIRIUM_DB_URI) in g
    assert (ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")) in g


def test_value_kind_is_omitted_when_not_supplied():
    """A default would contradict a later data-derived kind on the same ref."""
    g = build({"source_id": SOURCE, "ref_name": "temp"})
    assert list(g.objects(ref_of(), ACQUIRIUM_VALUE_KIND)) == []


def test_value_kind_is_normalised():
    g = build({"source_id": SOURCE, "ref_name": "temp", "value_kind": "float"})
    assert (ref_of(), ACQUIRIUM_VALUE_KIND, Literal("numeric")) in g


# ----------------------------------------------- semantics live on the ref

@pytest.mark.parametrize("field,predicate", list(SEMANTIC_PREDICATES.items()))
def test_semantics_land_on_the_reference_without_a_point(field, predicate):
    g = build({"source_id": SOURCE, "ref_name": "temp", field: FULL_SEMANTICS[field]})
    assert (ref_of(), predicate, URIRef(FULL_SEMANTICS[field])) in g


@pytest.mark.parametrize("field,predicate", list(SEMANTIC_PREDICATES.items()))
def test_semantics_land_on_the_reference_even_with_a_point(field, predicate):
    """The change from earlier behaviour: a point no longer takes them."""
    g = build({
        "source_id": SOURCE, "ref_name": "temp", "point_uri": str(POINT),
        field: FULL_SEMANTICS[field],
    })
    assert (ref_of(), predicate, URIRef(FULL_SEMANTICS[field])) in g
    assert list(g.objects(POINT, predicate)) == []


def test_point_carries_only_type_label_and_the_link():
    g = build({
        "source_id": SOURCE, "ref_name": "temp", "point_uri": str(POINT),
        "label": "Basin 1 temperature", "data_source": "SCADA", **FULL_SEMANTICS,
    })
    assert set(g.predicate_objects(POINT)) == {
        (RDF.type, VIRTUAL_POINT),
        (RDFS.label, Literal("Basin 1 temperature")),
        (HAS_EXTERNAL_REFERENCE, ref_of()),
    }


def test_resolved_uri_wins_over_the_raw_text():
    g = build(
        {"source_id": SOURCE, "ref_name": "temp", "unit": "degC"},
        {"unit": DEG_C},
    )
    assert (ref_of(), HAS_UNIT, URIRef(DEG_C)) in g


def test_unresolved_text_is_kept_rather_than_dropped():
    """Registration refuses unresolvable text upstream; if a caller reaches
    here anyway the value must survive, not vanish."""
    g = build({"source_id": SOURCE, "ref_name": "temp", "unit": "widgets"}, {})
    assert (ref_of(), HAS_UNIT, Literal("widgets")) in g


def test_data_source_is_a_literal_on_the_reference():
    g = build({
        "source_id": SOURCE, "ref_name": "temp",
        "point_uri": str(POINT), "data_source": "SCADA",
    })
    assert (ref_of(), DATA_SOURCE, Literal("SCADA")) in g
    assert list(g.objects(POINT, DATA_SOURCE)) == []


# ---------------------------------------------------------------- label

def test_label_goes_to_the_point_when_there_is_one():
    g = build({
        "source_id": SOURCE, "ref_name": "temp",
        "point_uri": str(POINT), "label": "Basin 1",
    })
    assert (POINT, RDFS.label, Literal("Basin 1")) in g
    assert list(g.objects(ref_of(), RDFS.label)) == []


def test_label_falls_back_to_the_reference_without_a_point():
    g = build({"source_id": SOURCE, "ref_name": "temp", "label": "Basin 1"})
    assert (ref_of(), RDFS.label, Literal("Basin 1")) in g


# ------------------------------------------------------------ point link

def test_no_point_means_no_point_node_and_no_link():
    g = build({"source_id": SOURCE, "ref_name": "temp", **FULL_SEMANTICS})
    assert list(g.subjects(RDF.type, VIRTUAL_POINT)) == []
    assert list(g.subjects(HAS_EXTERNAL_REFERENCE, None)) == []


def test_point_is_linked_to_its_reference():
    g = build({"source_id": SOURCE, "ref_name": "temp", "point_uri": str(POINT)})
    assert (POINT, HAS_EXTERNAL_REFERENCE, ref_of()) in g


# ----------------------------------------------------- mismatch override

def test_mismatch_flag_is_recorded_on_the_reference():
    g = build({
        "source_id": SOURCE, "ref_name": "temp", "point_uri": str(POINT),
        "unit": DEG_C, "allow_unit_mismatch": True,
    })
    assert (ref_of(), UNIT_MISMATCH_ALLOWED, Literal(True)) in g


def test_mismatch_flag_absent_by_default():
    g = build({"source_id": SOURCE, "ref_name": "temp", "unit": DEG_C})
    assert list(g.objects(ref_of(), UNIT_MISMATCH_ALLOWED)) == []


# ------------------------------------------------------------ properties

def test_properties_go_on_the_reference():
    g = build({
        "source_id": SOURCE, "ref_name": "temp", "point_uri": str(POINT),
        "properties": {_TEST_PROP: "demo.csv"},
    })
    assert (ref_of(), _TEST_PROP, Literal("demo.csv")) in g


def test_properties_fall_back_to_the_point_when_there_is_no_reference():
    g = build({"point_uri": str(POINT), "properties": {_TEST_PROP: "demo.csv"}})
    assert (POINT, _TEST_PROP, Literal("demo.csv")) in g


def test_uri_shaped_property_values_become_iris():
    g = build({
        "source_id": SOURCE, "ref_name": "temp",
        "properties": {_TEST_PROP: "urn:test:thing"},
    })
    assert (ref_of(), _TEST_PROP, URIRef("urn:test:thing")) in g


# ------------------------------------------------------------- batching

def test_several_streams_accumulate_into_one_graph():
    g = Graph()
    for name in ("temp", "rh"):
        build_stream_triples(g, {"source_id": SOURCE, "ref_name": name,
                                 "value_kind": "numeric"}, {})
    for name in ("temp", "rh"):
        assert (ref_of(name), ACQUIRIUM_REF_NAME, Literal(name)) in g
