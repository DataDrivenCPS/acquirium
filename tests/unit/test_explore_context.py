"""Tests for ``Query.context()`` and the relation registry.

Builder and compiler tests need no server (URIs bypass text resolution).
Execution tests run the compiled SPARQL on a small in-memory s223 graph
through a fake client, so the relations are checked against real triples,
including a property on a pipe.
"""

import re

import pytest
from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef

from acquirium.Client.explore import (
    DOWNSTREAM_PROPERTY,
    RELATIONS,
    UPSTREAM_PROPERTY,
    Query,
    register_relation,
    reset_relations,
    reverse_chains,
    unregister_relation,
)
from acquirium.Client.query_graph import QueryEdge
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, S223

X = Namespace("urn:x/")
CLS = Namespace("urn:x#")
HAS_PROP = str(S223.hasProperty)
HAS_CP = str(S223.hasConnectionPoint)


def q() -> Query:
    return Query(client=None)


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


@pytest.fixture(autouse=True)
def clean_registry():
    reset_relations()
    yield
    reset_relations()


# ---------------------------------------------------------------- registry

class TestReverseChains:
    def test_single_step_inverts(self):
        assert reverse_chains((((HAS_PROP, None),),)) == (((f"^{HAS_PROP}", None),),)

    def test_class_stays_on_its_node(self):
        # A hasCP CP (CP a Outlet) . CP hasProperty P   ->   P ^hasProperty CP (Outlet) . CP ^hasCP A
        fwd = (((HAS_CP, "urn:Outlet"), (HAS_PROP, None)),)
        assert reverse_chains(fwd) == (((f"^{HAS_PROP}", "urn:Outlet"), (f"^{HAS_CP}", None)),)

    def test_inverse_predicates_flip_back(self):
        fwd = ((("^urn:p", None), ("urn:q", "urn:C")),)
        assert reverse_chains(fwd) == ((("^urn:q", None), ("urn:p", None)),)

    def test_round_trip_drops_only_end_classes(self):
        fwd = ((("urn:p", "urn:A"), ("urn:q", "urn:B"), ("urn:r", None)),)
        assert reverse_chains(reverse_chains(fwd)) == fwd


class TestRegistry:
    def test_defaults(self):
        assert set(RELATIONS) == {"entity", "upstream", "downstream"}
        assert RELATIONS["upstream"] == reverse_chains(DOWNSTREAM_PROPERTY)
        assert RELATIONS["downstream"] == reverse_chains(UPSTREAM_PROPERTY)
        assert RELATIONS["entity"] == (
            ((f"^{HAS_PROP}", None),),
            ((f"^{HAS_PROP}", None), (f"^{HAS_CP}", None)),
        )

    def test_register_and_unregister(self):
        register_relation("system", ((("^urn:p", None), ("^urn:member", None)),))
        assert RELATIONS["system"] == ((("^urn:p", None), ("^urn:member", None)),)
        unregister_relation("system")
        assert "system" not in RELATIONS

    def test_register_normalises_lists(self):
        register_relation("r", [[["urn:p", None]]])
        assert RELATIONS["r"] == ((("urn:p", None),),)

    @pytest.mark.parametrize("bad", [(), (("urn:p",),), ((("*", None),),), "urn:p", ((("", None),),)])
    def test_register_rejects_bad_shapes(self, bad):
        with pytest.raises(ValueError):
            register_relation("bad", bad)

    def test_reset_restores_defaults(self):
        register_relation("extra", ((("urn:p", None),),))
        unregister_relation("entity")
        reset_relations()
        assert set(RELATIONS) == {"entity", "upstream", "downstream"}


# ----------------------------------------------------------------- builder

class TestContextBuilder:
    def test_adds_entity_node_and_relation_edge(self):
        b = q().measurement().context(CLS.Pump)
        g = b.query_graph
        assert set(g.nodes) == {0, 1}
        assert g.nodes[1].constraints == {"rdf_class": str(CLS.Pump)}
        assert 1 not in g.data_nodes
        assert g.current_pointer == 1
        (edge,) = g.edges
        assert (edge.source_id, edge.target_id) == (0, 1)
        assert edge.relation == RELATIONS["entity"] and edge.relation_name == "entity"
        assert edge.patterns is None and edge.predicates is None and edge.direction is None

    def test_default_alias_is_source_and_relation(self):
        assert q().measurement().context(CLS.Pump).query_graph.aliases_reverse[1] == "data_entity"
        assert (q().measurement(alias="m").context(CLS.Pump, via="upstream")
                .query_graph.aliases_reverse[1] == "m_upstream")

    def test_default_alias_uniquified(self):
        b = q().measurement().context(CLS.Pump).refocus("data").context(CLS.Tank)
        assert b.query_graph.aliases_reverse[2] == "data_entity_2"

    def test_explicit_alias_and_uri(self):
        b = q().measurement().context(uri="urn:x/P1", alias="p1")
        assert b.query_graph.aliases["p1"] == 1
        assert b.query_graph.nodes[1].constraints == {"instance_uri": "urn:x/P1"}

    def test_attrs_apply_to_new_node(self):
        b = q().measurement().context(type=CLS.Pump)
        assert b.query_graph.nodes[1].constraints["attrs"] == {"type": str(CLS.Pump)}

    def test_unconstrained_context_node(self):
        b = q().measurement().context()
        assert b.query_graph.nodes[1].constraints == {}
        assert b.query_graph.aliases_reverse[1] == "data_entity"

    def test_from_entity_node_raises(self):
        with pytest.raises(ValueError, match="not a measurement node"):
            q().entity(CLS.Pump).context(CLS.Tank)

    def test_frm_picks_a_data_node(self):
        b = (q().entity(CLS.Tank, alias="t").measurement(alias="m")
             .related(CLS.Pump, via=["urn:p"], alias="p")
             .context(CLS.Pipe, frm="m"))
        (edge,) = [e for e in b.query_graph.edges if e.relation]
        assert edge.source_id == b.query_graph.aliases["m"]

    def test_unknown_via_name_without_client_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            q().measurement().context(CLS.Pump, via="nonsense words")

    def test_via_predicate_uri(self):
        b = q().measurement().context(CLS.Pump, via="^urn:x#of")
        assert b.query_graph.edges[0].relation == ((("^urn:x#of", None),),)
        assert b.query_graph.edges[0].relation_name == "context"
        assert b.query_graph.aliases_reverse[1] == "data_context"

    def test_via_predicate_list(self):
        b = q().measurement().context(CLS.Pump, via=["urn:x#a", "^urn:x#b"])
        assert b.query_graph.edges[0].relation == ((("urn:x#a", None),), (("^urn:x#b", None),))

    def test_via_chain_tuple(self):
        chains = ((("^urn:x#a", None), ("^urn:x#b", "urn:x#C")),)
        b = q().measurement().context(CLS.Pump, via=chains)
        assert b.query_graph.edges[0].relation == chains

    def test_registered_relation_by_name(self):
        register_relation("system", ((("^urn:x#p", None), ("^urn:x#member", None)),))
        b = q().measurement().context(CLS.System, via="system")
        assert b.query_graph.edges[0].relation_name == "system"
        assert b.query_graph.aliases_reverse[1] == "data_system"

    def test_pointer_moves_and_chain_continues(self):
        b = q().measurement().context(CLS.Pump, alias="p").related(CLS.Tank, via=["urn:x#to"], alias="t")
        g = b.query_graph
        assert g.edges[1].source_id == g.aliases["p"]

    def test_to_dict_carries_relation(self):
        d = q().measurement().context(CLS.Pump).to_dict()
        assert d["edges"][0]["relation_name"] == "entity"
        assert d["edges"][0]["relation"] == [[[f"^{HAS_PROP}", None]],
                                             [[f"^{HAS_PROP}", None], [f"^{HAS_CP}", None]]]

    def test_immutability(self):
        base = q().measurement()
        base.context(CLS.Pump)
        assert len(base.query_graph.nodes) == 1


# ---------------------------------------------------------------- compiler

class TestContextCompile:
    def test_entity_relation_renders_union_of_chains(self):
        s = norm(q().measurement().context(CLS.Pump).to_sparql())
        assert f"{{ ?v0 ^<{HAS_PROP}> ?v1 . }}" in s
        assert f"?v0 ^<{HAS_PROP}> ?m_e0_rel_a1_0 . ?m_e0_rel_a1_0 ^<{HAS_CP}> ?v1 ." in s
        assert "subClassOf>* <urn:x#Pump>" in s

    def test_class_on_intermediate_step_is_fenced(self):
        s = norm(q().measurement().context(CLS.Pump, via="upstream").to_sparql())
        assert f"?m_e0_rel_a0_0 <{RDF.type}>/<{RDFS.subClassOf}>* <{S223.OutletConnectionPoint}> ." in s

    def test_instance_pin(self):
        s = norm(q().measurement().context(uri="urn:x/P1").to_sparql())
        assert "VALUES ?v1 { <urn:x/P1> }" in s

    def test_not_resolved_client_side(self):
        from acquirium.Client.explore.traverse import resolve_program_edges
        b = q().measurement().context(CLS.Pump)
        assert resolve_program_edges(b.query_graph, client=None) is b.query_graph

    def test_multi_data_node_keeps_context_inside_its_branch(self):
        b = (q().entity(CLS.Tank, alias="t").related(CLS.Pump, via=["urn:x#p"], alias="p")
             .measurement(frm=["t", "p"], alias="m")
             .context(CLS.Pipe, frm="m_1", alias="pipe"))
        s = norm(b.to_sparql())
        pipe_var = f"?v{b.query_graph.aliases['pipe']}"
        m1_var = f"?v{b.query_graph.aliases['m_1']}"
        fence = f"subClassOf>* <urn:x#Pipe>"
        # the pipe's fence and its relation edge come after the shared part,
        # i.e. after the last shared clause (the ext-ref line of the first branch
        # is inside a branch, so anything past the first "{ " + data var is branch)
        shared_end = s.index(f"{{ {m1_var} ")  # first clause of m_1's branch
        assert fence not in s[:shared_end]
        assert f"{m1_var} ^<{HAS_PROP}> {pipe_var}" in s or f"^<{HAS_CP}> {pipe_var}" in s
        assert s.index(fence) > shared_end


# --------------------------------------------------------------- execution

OUTLET = S223.OutletConnectionPoint
INLET = S223.InletConnectionPoint


def plant() -> Graph:
    """pump -> pipe(conn) -> tank, with a property on every kind of node."""
    g = Graph()
    for c in (CLS.Pump, CLS.Tank):
        g.add((c, RDFS.subClassOf, CLS.Equipment))
    g.add((X.pump, RDF.type, CLS.Pump))
    g.add((X.tank, RDF.type, CLS.Tank))
    g.add((X.conn, RDF.type, S223.Connection))
    g.add((X.pump_out, RDF.type, OUTLET))
    g.add((X.tank_in, RDF.type, INLET))
    g.add((X.pump, S223.hasConnectionPoint, X.pump_out))
    g.add((X.tank, S223.hasConnectionPoint, X.tank_in))
    g.add((X.conn, S223.connectsFrom, X.pump))
    g.add((X.conn, S223.connectsTo, X.tank))
    g.add((X.conn, S223.connectsAt, X.pump_out))
    g.add((X.conn, S223.connectsAt, X.tank_in))
    g.add((X.pump, S223.connectedTo, X.tank))
    g.add((X.tank, S223.connectedFrom, X.pump))
    props = {
        X.p_eff: X.pump,        # pump's own property
        X.p_out: X.pump_out,    # on the pump's outlet connection point
        X.p_pipe: X.conn,       # on the pipe
        X.p_tank_in: X.tank_in, # on the tank's inlet connection point
        X.p_tank: X.tank,       # tank's own property
    }
    for p, carrier in props.items():
        g.add((carrier, S223.hasProperty, p))
        g.add((p, URIRef(HAS_EXTERNAL_REFERENCE), URIRef(f"{p}-ref")))
        g.add((p, RDFS.label, Literal(str(p).rsplit("/", 1)[-1])))
    return g


class FakeClient:
    """Answers sparql_query with rdflib over the fixture graph."""

    base_url = "fake://plant"

    def __init__(self, graph: Graph):
        self.graph = graph

    def sparql_query(self, sparql: str, include_dependencies: bool = True) -> dict:
        res = self.graph.query(sparql)
        cols = [str(v) for v in res.vars]
        rows = [[None if row[i] is None else str(row[i]) for i in range(len(cols))] for row in res]
        return {"columns": cols, "rows": rows}

    def graph_version(self) -> int:
        return 1

    def compact_uri(self, uri: str) -> str:
        return str(uri).replace("urn:x/", "x:")

    def resolve(self, *a, **k):
        raise AssertionError("no text resolution expected")


def pairs(query: Query, data: str, ent: str) -> set:
    res = query.execute()
    cols = res["columns"]
    return {(r[cols.index(data)], r[cols.index(ent)]) for r in res["rows"]}


@pytest.fixture
def pq() -> Query:
    return Query(client=FakeClient(plant()))


class TestContextExecution:
    def test_unconstrained_reaches_every_carrier(self, pq):
        got = pairs(pq.measurement().context(alias="e"), "v0", "v1")
        assert {e for _, e in got} == {str(X.pump), str(X.pump_out), str(X.conn), str(X.tank_in), str(X.tank)}

    def test_entity_direct_and_through_connection_point(self, pq):
        got = pairs(pq.measurement().context(CLS.Equipment, alias="e"), "v0", "v1")
        assert got == {
            (str(X.p_eff), str(X.pump)), (str(X.p_out), str(X.pump)),
            (str(X.p_tank_in), str(X.tank)), (str(X.p_tank), str(X.tank)),
        }

    def test_entity_of_a_pipe_property_is_the_connection(self, pq):
        got = pairs(pq.measurement().context(S223.Connection, alias="c"), "v0", "v1")
        assert got == {(str(X.p_pipe), str(X.conn))}

    def test_upstream(self, pq):
        got = pairs(pq.measurement().context(CLS.Equipment, via="upstream", alias="up"), "v0", "v1")
        assert got == {
            (str(X.p_out), str(X.pump)),      # own outlet CP: downstream of the pump
            (str(X.p_pipe), str(X.pump)),     # on the pipe leaving the pump
            (str(X.p_tank_in), str(X.pump)),  # tank's inlet, fed by the pump
            (str(X.p_tank), str(X.pump)),     # tank itself, connectedTo from the pump
        }

    def test_downstream(self, pq):
        got = pairs(pq.measurement().context(CLS.Equipment, via="downstream", alias="down"), "v0", "v1")
        assert got == {
            (str(X.p_tank_in), str(X.tank)),  # own inlet CP: upstream of the tank
            (str(X.p_pipe), str(X.tank)),     # on the pipe entering the tank
            (str(X.p_out), str(X.tank)),      # pump's outlet, feeding the tank
            (str(X.p_eff), str(X.tank)),      # pump itself, connectedTo the tank
        }

    def test_pipe_property_upstream_and_downstream(self, pq):
        up = pairs(pq.measurement(alias="m").context(CLS.Equipment, via="upstream"), "v0", "v1")
        down = pairs(pq.measurement().context(CLS.Equipment, via="downstream"), "v0", "v1")
        assert (str(X.p_pipe), str(X.pump)) in up
        assert (str(X.p_pipe), str(X.tank)) in down

    def test_parity_with_measurement(self, pq):
        forward = pairs(pq.entity(uri=X.pump, alias="p").measurement(alias="m"), "v1", "v0")
        backward = pairs(pq.measurement(alias="m").context(uri=X.pump, alias="p"), "v0", "v1")
        assert forward == backward == {(str(X.p_eff), str(X.pump)), (str(X.p_out), str(X.pump))}

    def test_attrs_filter_the_context_node(self, pq):
        got = pairs(pq.measurement().context(type=CLS.Tank, via="upstream", alias="e"), "v0", "v1")
        assert got == set()
        got = pairs(pq.measurement().context(type=CLS.Pump, via="upstream", alias="e"), "v0", "v1")
        assert {e for _, e in got} == {str(X.pump)}

    def test_metadata_columns(self, pq):
        df = pq.measurement(alias="m").context(CLS.Equipment, alias="e").metadata()
        assert df.columns[:2] == ["m", "e"] or set(df.columns) >= {"m", "e"}
        assert df.height == 4

    def test_registered_relation_executes(self, pq):
        register_relation("carrier", ((( f"^{HAS_PROP}", None),),))
        got = pairs(pq.measurement().context(uri=X.conn, via="carrier", alias="c"), "v0", "v1")
        assert got == {(str(X.p_pipe), str(X.conn))}
