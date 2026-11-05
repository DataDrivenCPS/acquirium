"""Derived streams are discoverable S223 properties without physical sensors."""

import pytest
from rdflib import Graph, Literal, RDF, RDFS, URIRef
import shifty

from acquirium import App, Output, Selection
from acquirium.apps import output_reference
from acquirium._ontologies import S223_IRI, load_bundled_graph
from acquirium.internals.internals_namespaces import QUDT, S223
from acquirium.Server.manager import Manager

UNIT = "http://qudt.org/vocab/unit/DEG_C"
KIND = "http://qudt.org/vocab/quantitykind/Temperature"


@pytest.fixture(scope="module")
def manager(tmp_path_factory):
    owner = Manager(data_dir=tmp_path_factory.mktemp("output-properties"), exact_only=True)
    try:
        yield owner
    finally:
        owner.close()


@pytest.mark.parametrize("fields, quantitative", [
    ({"unit": UNIT, "quantity_kind": KIND}, True),
    ({"unit": UNIT}, False),
    ({"quantity_kind": KIND}, False),
    ({}, False),
    ({"value_kind": "text", "unit": UNIT, "quantity_kind": KIND}, False),
])
def test_output_property_types_and_discovery(manager, fields, quantitative):
    declaration = App(
        "property-types",
        [Selection("SELECT ?stream ?group WHERE { BIND(<urn:input> AS ?stream) BIND(<urn:plant> AS ?group) }")],
        {"result": Output(on_group=True, **fields)},
        {"kind": "mean", "port": "result", "per_stream": False},
    )
    manager.deploy_app(declaration)
    reference = output_reference(declaration.name, "urn:plant", "result")
    point = URIRef(reference + "__point")
    metadata = manager.graph_store.data_graph("derived:property-types")
    assert (point, RDF.type, S223.Property) in metadata
    assert ((point, RDF.type, S223.QuantifiableProperty) in metadata) is quantitative
    assert (point, RDF.type, S223.ObservableProperty) not in metadata
    matches = manager.graph_store.sparql_query(f"""
        SELECT ?point WHERE {{
            <urn:plant> <{S223.hasProperty}> ?point .
            ?point a <{S223.Property}> .
        }}
    """, wait_for_fresh=True)
    assert matches["rows"] == [[point]]
    manager.remove_app(declaration.name)


def test_quantifiable_property_validation_without_an_observing_sensor():
    shapes = load_bundled_graph("s223.ttl", S223_IRI)
    point, unit, kind = map(URIRef, ("urn:result", UNIT, KIND))
    data = Graph()
    data.add((point, RDF.type, S223.QuantifiableProperty))
    data.add((point, RDFS.label, Literal("Computed temperature")))
    data.add((point, QUDT.hasUnit, unit))
    data.add((point, QUDT.hasQuantityKind, kind))
    data.add((unit, RDF.type, QUDT.Unit))
    data.add((kind, RDF.type, QUDT.QuantityKind))
    selected = [str(S223.QuantifiableProperty), str(S223.ObservableProperty)]
    assert shifty.validate(data, shapes, shape_names=selected)[0]
    data.remove((point, QUDT.hasQuantityKind, kind))
    assert not shifty.validate(data, shapes, shape_names=selected)[0]
