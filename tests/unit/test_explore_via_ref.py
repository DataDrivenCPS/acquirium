"""Matching semantics on a point or on its external reference.

Stream registration writes unit/quantity_kind/medium/substance on the
external reference; a user's model may put them on the point. Queries have to
find either, and — where both are present and differ, which reconciliation
permits for a convertible pair — return one row taking the point's value.

The projection cases execute against a real rdflib graph rather than
asserting on query text, since row count is exactly what is at stake.
"""
from __future__ import annotations

import pytest
from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from acquirium.Client.explore.attributes import REGISTRY
from acquirium.Client.explore.compile import attr_path_groups, attr_pred_path
from acquirium.Client.explore.core import Query
from acquirium.internals.internals_namespaces import (
    HAS_EXTERNAL_REFERENCE,
    HAS_UNIT,
    S223,
)

DEG_C = URIRef("http://qudt.org/vocab/unit/DEG_C")
DEG_F = URIRef("http://qudt.org/vocab/unit/DEG_F")
CLS = URIRef("urn:t#Equipment")
EQ = URIRef("urn:t#eq")
REF_HOP = f"<{HAS_EXTERNAL_REFERENCE}>"


def q() -> Query:
    return Query(client=None)


@pytest.fixture
def graph() -> Graph:
    """One equipment with three measurements, covering each unit arrangement."""
    g = Graph()
    g.add((EQ, RDF.type, CLS))
    for name in ("both", "ref_only", "neither"):
        point, ref = URIRef(f"urn:t#p_{name}"), URIRef(f"urn:t#r_{name}")
        g.add((EQ, S223.hasProperty, point))
        g.add((point, HAS_EXTERNAL_REFERENCE, ref))
    # A convertible disagreement — accepted at registration, so the read path
    # must cope with it rather than treat it as impossible.
    g.add((URIRef("urn:t#p_both"), HAS_UNIT, DEG_C))
    g.add((URIRef("urn:t#r_both"), HAS_UNIT, DEG_F))
    g.add((URIRef("urn:t#r_ref_only"), HAS_UNIT, DEG_F))
    return g


def run(graph: Graph, query: Query) -> list[tuple]:
    """``(measurement, projected attribute)`` per row. Needs an include()."""
    return [
        (str(row[1]).split("#")[-1], str(row[2]).split("/")[-1] if row[2] else None)
        for row in graph.query(query.to_sparql())
    ]


def names(graph: Graph, query: Query) -> list[str]:
    """The matched measurements, for queries that project no attribute."""
    return [str(row[1]).split("#")[-1] for row in graph.query(query.to_sparql())]


# ------------------------------------------------------------ path building

@pytest.mark.parametrize("name", ["unit", "quantity_kind", "medium", "substance",
                                  "data_source"])
def test_via_ref_attrs_add_the_reference_hop_on_measurements(name):
    direct, through_ref = attr_path_groups(REGISTRY[name], "data")
    assert len(through_ref) == len(direct)
    assert all(path.startswith(REF_HOP + "/") for path in through_ref)


@pytest.mark.parametrize("name", ["unit", "medium"])
def test_no_reference_hop_on_entity_nodes(name):
    """An entity has no external reference; the extra alternative would be dead."""
    assert attr_path_groups(REGISTRY[name], "entity")[1] == []


@pytest.mark.parametrize("name", ["type", "process", "cp_type", "enumeration_kind"])
def test_non_via_ref_attrs_are_untouched(name):
    assert attr_path_groups(REGISTRY[name], "data")[1] == []


def test_point_side_paths_come_first():
    assert attr_pred_path(REGISTRY["unit"], "data").index(f"<{HAS_UNIT}>") == 0


# ---------------------------------------------------------------- filtering

def test_filter_matches_a_unit_on_the_point(graph):
    assert names(graph, q().entity(CLS).measurement(unit=str(DEG_C))) == ["p_both"]


def test_filter_matches_a_unit_on_the_reference(graph):
    """The case that motivated via_ref: a stream with no model behind it."""
    assert "p_ref_only" in names(graph, q().entity(CLS).measurement(unit=str(DEG_F)))


def test_filter_matching_either_side_does_not_duplicate_rows(graph):
    """p_both matches through its reference; SELECT DISTINCT must collapse it."""
    matched = names(graph, q().entity(CLS).measurement(unit=str(DEG_F)))
    assert sorted(matched) == ["p_both", "p_ref_only"]
    assert len(matched) == len(set(matched))


def test_filter_excludes_measurements_with_no_unit(graph):
    assert "p_neither" not in names(graph, q().entity(CLS).measurement(unit=str(DEG_C)))


# --------------------------------------------------------------- projection

def test_projection_returns_one_row_per_measurement(graph):
    """A plain alternation would bind twice for p_both and split it in two."""
    rows = run(graph, q().entity(CLS).measurement(alias="m").include("unit"))
    assert len(rows) == 3
    assert len({name for name, _ in rows}) == 3


def test_projection_prefers_the_point(graph):
    rows = dict(run(graph, q().entity(CLS).measurement(alias="m").include("unit")))
    assert rows["p_both"] == "DEG_C"


def test_projection_falls_back_to_the_reference(graph):
    rows = dict(run(graph, q().entity(CLS).measurement(alias="m").include("unit")))
    assert rows["p_ref_only"] == "DEG_F"


def test_projection_is_null_when_neither_side_has_one(graph):
    rows = dict(run(graph, q().entity(CLS).measurement(alias="m").include("unit")))
    assert rows["p_neither"] is None


def test_required_projection_drops_rows_with_no_value_either_side(graph):
    rows = run(graph, q().entity(CLS).measurement(alias="m").include("unit", required=True))
    assert {name for name, _ in rows} == {"p_both", "p_ref_only"}


def test_required_projection_still_prefers_the_point(graph):
    rows = dict(run(graph, q().entity(CLS).measurement(alias="m")
                    .include("unit", required=True)))
    assert rows["p_both"] == "DEG_C"


def test_projection_binds_a_single_output_variable(graph):
    """The per-side helpers stay internal; only the attr column is projected."""
    header = q().entity(CLS).measurement(alias="m").include("unit").to_sparql().splitlines()[0]
    assert "?attr1_unit" in header
    assert "__point" not in header and "__ref" not in header
