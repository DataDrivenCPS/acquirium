"""Round-trip tests for Query.to_dict / Query.from_dict.

Every query built here uses URI inputs, which bypass text resolution in the
builder, so no server/client is needed (same convention as
test_explore_core.py). Round-trips go through json.dumps/loads to exercise
exactly what persistence does — notably JSON stringifying int dict keys and
turning tuples into lists.
"""

import json
from dataclasses import replace

import pytest

from acquirium.Client.explore.attributes import Not
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.directions import UPSTREAM_EQUIPMENT

CLS_A = "urn:test#TypeA"
CLS_B = "urn:test#TypeB"
INST = "urn:test:instance#x1"
PRED_P = "urn:test#p"
MEDIUM = "urn:test#brine"


def q() -> Query:
    return Query(client=None)


def roundtrip(query: Query) -> Query:
    return Query.from_dict(json.loads(json.dumps(query.to_dict(strict=True))))


SAMPLES = {
    "entity": lambda: q().entity(CLS_A, alias="a"),
    "entity_uri": lambda: q().entity(uri=INST, alias="x"),
    "related_any": lambda: q().entity(CLS_A, alias="a").related(CLS_B, alias="b"),
    "related_preds": lambda: q().entity(CLS_A, alias="a").related(
        CLS_B, alias="b", via=[PRED_P], max_depth=2),
    "related_inverse": lambda: q().entity(CLS_A).related(CLS_B, via=[f"^{PRED_P}"]),
    "related_direction": lambda: q().entity(CLS_A, alias="a").related(
        CLS_B, alias="b", direction="downstream", max_depth=2),
    "related_nearest_preds": lambda: q().entity(CLS_A).related(
        CLS_B, via=[PRED_P], nearest=True),
    "related_program": lambda: q().entity(CLS_A).related(
        CLS_B, via=UPSTREAM_EQUIPMENT),
    "measurement": lambda: q().entity(CLS_A, alias="a").measurement(alias="m"),
    "measurement_no_cp": lambda: q().entity(CLS_A, alias="a").measurement(
        alias="m", include_connection_points=False),
    "measurement_root": lambda: q().measurement(),
    "measurement_directional": lambda: q().entity(CLS_A, alias="a").measurement(
        direction="upstream", max_depth=2),
    "measurement_nearest": lambda: q().entity(CLS_A, alias="a").measurement(
        direction="downstream", nearest=True),
    "filters_not": lambda: q().entity(CLS_A, alias="a").measurement(
        alias="m", medium=Not(MEDIUM), quantity_kind="urn:test#qk"),
    "entity_attrs": lambda: q().entity(CLS_A, alias="a", medium=[MEDIUM, "urn:test#x"]),
    "include": lambda: q().entity(CLS_A, alias="a").measurement(alias="m")
        .include("unit", required=True),
    "star": lambda: q().entity(CLS_A, alias="a").entity(CLS_B, alias="b")
        .measurement(frm="*"),
}


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_roundtrip_graph_and_sparql_identical(name):
    original = SAMPLES[name]()
    rebuilt = roundtrip(original)
    # Strongest check first: the reconstructed graph is *equal*, which the
    # frozen dataclasses only grant when every list/tuple/int-key detail
    # survived. SPARQL equality then guards against eq/compile drift.
    assert rebuilt.query_graph == original.query_graph
    assert rebuilt.to_sparql() == original.to_sparql()


def test_aliases_reverse_keys_are_ints_again():
    rebuilt = roundtrip(q().entity(CLS_A, alias="a").measurement(alias="m"))
    g = rebuilt.query_graph
    assert all(isinstance(k, int) for k in g.aliases_reverse)
    assert g.aliases_reverse[0] == "a"
    # The lookups consumers actually do:
    assert g.aliases_reverse.get(g.aliases["m"]) == "m"


def test_selects_are_tuples_and_dedup_still_works():
    b = q().entity(CLS_A, alias="a").measurement(alias="m").include("unit")
    rebuilt = roundtrip(b)
    assert all(isinstance(e, tuple) for e in rebuilt.query_graph.selects)
    # with_select dedups via `entry in self.selects`; a JSON-list entry
    # would defeat it and duplicate the projection.
    again = rebuilt.include("unit")
    assert again.query_graph.selects == rebuilt.query_graph.selects


def test_cp_union_false_survives():
    b = q().entity(CLS_A, alias="a").measurement(
        alias="m", include_connection_points=False)
    assert roundtrip(b).query_graph.edges[0].cp_union is False


def test_cp_filter_survives():
    b = q().entity(CLS_A, alias="a").measurement(direction="downstream")
    cp_filters = [e.cp_filter for e in b.query_graph.edges if e.cp_filter]
    assert cp_filters  # the directional form sets one
    rebuilt = roundtrip(b)
    assert [e.cp_filter for e in rebuilt.query_graph.edges if e.cp_filter] == cp_filters


def test_value_pairs_none_vs_empty_distinct():
    b = q().entity(CLS_A, alias="a").related(CLS_B, alias="b")
    (edge,) = b.query_graph.edges
    assert edge.value_pairs is None

    # Simulate a resolved edge: () means "resolved, no matches" and must not
    # come back as None (execute() would re-resolve), nor as a list.
    resolved_edge = replace(edge, value_pairs=(("urn:s", "urn:t"),))
    resolved = Query(
        client=None,
        query_graph=replace(b.query_graph, edges=[resolved_edge]),
    )
    rt = roundtrip(resolved)
    assert rt.query_graph.edges[0].value_pairs == (("urn:s", "urn:t"),)

    empty_edge = replace(edge, value_pairs=())
    rt_empty = roundtrip(Query(
        client=None,
        query_graph=replace(b.query_graph, edges=[empty_edge]),
    ))
    assert rt_empty.query_graph.edges[0].value_pairs == ()
    assert rt_empty.query_graph.edges[0].value_pairs is not None


def test_not_marker_revives():
    b = q().entity(CLS_A, alias="a").measurement(alias="m", medium=Not(MEDIUM))
    rebuilt = roundtrip(b)
    (info,) = rebuilt.query_graph.data_nodes.values()
    assert info.filters["medium"] == Not(MEDIUM)


def test_strict_raises_on_lossy_value():
    b = q().entity(CLS_A, alias="a")
    node = b.query_graph.nodes[0]
    poisoned = replace(node, constraints={**node.constraints, "attrs": {"x": object()}})
    bad = Query(client=None,
                query_graph=b.query_graph.with_node(poisoned))
    with pytest.raises(ValueError, match="strict"):
        bad.to_dict(strict=True)
    # Default stays lenient for display callers.
    assert isinstance(bad.to_dict()["nodes"][0]["constraints"]["attrs"]["x"], str)
