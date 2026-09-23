"""Tests for insert_metadata: the value-map builder (Client/metadata.py), the
client/facade/query entry points, the register_streams ``metadata`` key, and
the store-side closure check and orphan pruning."""
import threading
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from rdflib import Dataset, Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from acquirium.Client.acquirium import Acquirium
from acquirium.Client.client import AcquiriumClient, _split_stream_metadata
from acquirium.Client.explore.attributes import ATTR_NS, clear_registry_cache
from acquirium.Client.explore.compile import _term
from acquirium.Client.explore.core import Query
from acquirium.Client.metadata import Write, flatten, plan_write, to_term, update_text
from acquirium.Client.explore.relations import writable_edge
from acquirium.internals.internals_namespaces import (
    HAS_EXTERNAL_REFERENCE, HAS_MEDIUM, HAS_UNIT, OF_MEDIUM, S223, WATR,
)
from acquirium.Storage import graph_store as gs
from acquirium.Storage.graph_registry import METADATA_SOURCE_ID, PLANT_GRAPH_URI, source_graph_uri

NODE = "urn:x#valve-1"
UNIT = "http://qudt.org/vocab/unit/MilliGM-PER-L"
HAS_PROP = str(S223.hasProperty)


def noop_resolve(text, kind):
    return {"mg/L": UNIT}.get(text)


def expand(text):
    return text.replace("x:", "urn:x#")


def plan(values, role="data"):
    return plan_write(NODE, values, role=role, resolve=noop_resolve, expand_uri=expand)


class TestFlatten:
    def test_nested_and_lists(self):
        leaves, removed = flatten({
            "a": 1, "b": {"c": "x", "d": {"e": True}}, "tags": ["p", "q", "p"],
            "items": [{"n": 1}, {"n": 2}], "gone": None, "skip": {"k": None},
        })
        assert leaves == {"a": 1, "b.c": "x", "b.d.e": True, "tags.0": "p", "tags.1": "q",
                          "tags.2": "p", "items.0.n": 1, "items.1.n": 2}
        assert removed == ["gone"]

    def test_bad_keys_raise(self):
        with pytest.raises(ValueError, match="invalid metadata key"):
            flatten({"a b": 1})
        with pytest.raises(ValueError, match="invalid metadata key"):
            flatten({"a": {"0": 1}})
        with pytest.raises(ValueError, match="invalid metadata key"):
            flatten({"a.b": 1})


class TestTerms:
    @pytest.mark.parametrize("value", [
        "text", 'say "hi"', 2015, 2.5, True, False, date(2020, 1, 2),
        datetime(2020, 1, 2, 3, 4, 5), "urn:x#node", "https://example.org/a",
    ])
    def test_writer_and_compiler_agree(self, value):
        assert to_term(value).n3() == _term(value)

    def test_unsupported(self):
        with pytest.raises(TypeError):
            to_term(object())


class TestPlanWrite:
    def test_user_attributes_flatten_to_typed_leaves(self):
        w = plan({"last_cleaned": "1999", "product_info": {"year": 2019, "ok": True}, "tags": ["a", "b"]})
        assert w.replace_keys == ["last_cleaned", "product_info", "tags"]
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}product_info.year"), Literal(2019)) in w.triples
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}tags.1"), Literal("b")) in w.triples
        assert len(w.triples) == 5

    def test_uri_like_string_is_a_uri(self):
        w = plan({"datasheet": "https://example.org/ds.pdf"})
        assert w.triples[0][2] == URIRef("https://example.org/ds.pdf")

    def test_builtin_resolves_text(self):
        w = plan({"unit": "mg/L"})
        assert w.replace_forward == [str(HAS_UNIT)]
        assert w.triples == [(URIRef(NODE), HAS_UNIT, URIRef(UNIT))]
        assert w.replace_keys == []

    def test_builtin_unresolvable_raises(self):
        with pytest.raises(ValueError, match="Could not resolve"):
            plan({"unit": "furlongs"})

    def test_medium_predicate_follows_role(self):
        assert plan({"medium": "urn:w#Water"}, role="data").triples[0][1] == OF_MEDIUM
        assert plan({"medium": "urn:w#Water"}, role="entity").triples[0][1] == HAS_MEDIUM

    def test_literal_builtin_and_type(self):
        w = plan({"label": "Influent", "type": "urn:w#Sensor"})
        assert (URIRef(NODE), RDFS.label, Literal("Influent")) in w.triples
        assert (URIRef(NODE), RDF.type, URIRef("urn:w#Sensor")) in w.triples

    def test_role_and_writability_checks(self):
        with pytest.raises(ValueError, match="does not apply to a entity node"):
            plan({"unit": UNIT}, role="entity")
        with pytest.raises(ValueError, match="cannot be written"):
            plan({"cp_type": "urn:w#Inlet"}, role="entity")

    def test_entity_relation_is_inverted(self):
        w = plan({"entity": "x:ozone"})
        assert w.replace_inverse == [HAS_PROP]
        assert w.triples == [(URIRef("urn:x#ozone"), URIRef(HAS_PROP), URIRef(NODE))]

    def test_measurement_relation_is_forward_and_accepts_lists(self):
        w = plan({"measurement": ["x:p1", "x:p2"]}, role="entity")
        assert w.replace_forward == [HAS_PROP]
        assert [t[2] for t in w.triples] == [URIRef("urn:x#p1"), URIRef("urn:x#p2")]

    def test_multi_step_relation_rejected(self):
        with pytest.raises(ValueError, match="no single edge"):
            plan({"upstream": "x:pump"})
        assert writable_edge("upstream") is None and writable_edge("nope") is None

    def test_relation_value_must_be_a_node(self):
        with pytest.raises(ValueError, match="takes node URIs"):
            plan({"entity": {"a": 1}})

    def test_none_removes(self):
        w = plan({"tags": None, "unit": None, "entity": None})
        assert w.replace_keys == ["tags"] and w.replace_forward == [str(HAS_UNIT)]
        assert w.replace_inverse == [HAS_PROP] and w.triples == []


class TestUpdateText:
    def test_groups_by_signature_and_inserts_once(self):
        a = plan({"tags": ["x"], "unit": UNIT, "entity": "x:e"})
        b = Write(subject=URIRef("urn:x#other"))
        b.replace_keys, b.replace_forward, b.replace_inverse = ["tags"], [str(HAS_UNIT)], [HAS_PROP]
        b.triples = [(b.subject, URIRef(f"{ATTR_NS}tags.0"), Literal("y"))]
        text = update_text([a, b])
        ops = text.split(" ;\n")
        assert len(ops) == 3
        assert ops[0].startswith("DELETE { ?s ?p ?o }") and f"VALUES ?s {{ <{NODE}> <urn:x#other> }}" in ops[0]
        assert f'STRSTARTS(STR(?p), "{ATTR_NS}tags.")' in ops[0] and f"?p = <{ATTR_NS}tags>" in ops[0]
        assert f"?p = <{HAS_UNIT}>" in ops[0]
        assert ops[1].startswith("DELETE { ?o ?p ?s }") and f"?p = <{HAS_PROP}>" in ops[1]
        assert ops[2].startswith("INSERT DATA {") and '"y"' in ops[2] and f"<{UNIT}>" in ops[2]

    def test_delete_only(self):
        text = update_text([plan({"tags": None})])
        assert "INSERT DATA" not in text and "DELETE" in text

    def test_empty(self):
        assert update_text([Write(subject=URIRef(NODE))]) == ""


def make_client(roles_rows, resolved=None):
    client = AcquiriumClient.__new__(AcquiriumClient)
    client.base_url = "http://test:8000"
    client._namespaces_cache = {"x": "urn:x#"}
    client.sparql_query = MagicMock(return_value={"columns": ["s", "ref"], "rows": roles_rows})
    client.sparql_update = MagicMock(return_value={"ok": True, "message": "update applied"})
    client.resolve = MagicMock(side_effect=lambda record, min_score=0.4: {
        k: (resolved or {}).get(t) for k, (t, kind) in record.items()})
    client.expand_uri = MagicMock(side_effect=expand)
    client.graph_version = MagicMock(return_value=1)
    return client


class TestClientInsert:
    def test_roles_from_external_reference(self):
        client = make_client([[NODE, "urn:x#ref"], ["urn:x#e", None]])
        assert client.node_roles([NODE, "urn:x#e", "urn:x#missing"]) == {NODE: "data", "urn:x#e": "entity"}
        sparql = client.sparql_query.call_args[0][0]
        assert f"<{HAS_EXTERNAL_REFERENCE}>" in sparql and f"<{NODE}>" in sparql
        assert client.sparql_query.call_args[1] == {"include_dependencies": False}

    def test_insert_builds_update_in_metadata_graph(self):
        client = make_client([[NODE, "urn:x#ref"]], resolved={"mg/L": UNIT})
        out = client.insert_metadata({"x:valve-1": {"unit": "mg/L", "tags": ["a"], "entity": "x:e"}})
        assert out == {"ok": True, "message": "update applied", "nodes": 1}
        update, kwargs = client.sparql_update.call_args[0][0], client.sparql_update.call_args[1]
        assert kwargs == {"source_id": METADATA_SOURCE_ID}
        assert f"<{NODE}> <{HAS_UNIT}> <{UNIT}>" in update
        assert f"<urn:x#e> <{HAS_PROP}> <{NODE}>" in update
        assert f'<{NODE}> <{ATTR_NS}tags.0> "a"' in update

    def test_full_uris_bypass_curie_expansion(self):
        # expand_uri accepts CURIEs only (an integration test pins that); the
        # URIs a query matched, and relation values, must still be accepted.
        client = make_client([[NODE, "urn:x#ref"]])
        client.expand_uri = MagicMock(side_effect=lambda s: (_ for _ in ()).throw(ValueError(s))
                                      if "://" in s or s.startswith("urn:") else expand(s))
        client.insert_metadata({NODE: {"entity": ["urn:x#e", "x:e2"]}})
        update = client.sparql_update.call_args[0][0]
        assert f"<urn:x#e> <{HAS_PROP}> <{NODE}>" in update and f"<urn:x#e2> <{HAS_PROP}> <{NODE}>" in update
        assert client.node_uri("x:e2") == "urn:x#e2" and client.node_uri(NODE) == NODE

    def test_unknown_subject_raises(self):
        client = make_client([])
        with pytest.raises(ValueError, match="unknown node"):
            client.insert_metadata({NODE: {"a": 1}})
        client.sparql_update.assert_not_called()

    def test_empty_records_is_a_noop(self):
        client = make_client([])
        assert client.insert_metadata({}) == {"ok": True, "nodes": 0}
        client.sparql_query.assert_not_called()

    def test_facade_delegates(self):
        aq = Acquirium.__new__(Acquirium)
        aq.client = MagicMock()
        aq.insert_metadata(URIRef(NODE), {"a": 1})
        aq.client.insert_metadata.assert_called_once_with({NODE: {"a": 1}})


class TestQueryInsert:
    def test_applies_to_matched_nodes_of_alias(self):
        clear_registry_cache()
        client = MagicMock()
        client.base_url = "http://test:8000"
        client.graph_version.return_value = 1
        client.sparql_query.return_value = {"columns": ["v0", "v1"],
                                            "rows": [["urn:x#e1", "urn:x#p1"], ["urn:x#e1", "urn:x#p2"]]}
        client.insert_metadata.return_value = {"ok": True, "nodes": 2}
        q = Query(client=client).entity("urn:x#Valve", alias="v").measurement(alias="m")
        assert q.insert_metadata({"reviewed": True}) == {"ok": True, "nodes": 2}
        client.insert_metadata.assert_called_once_with(
            {"urn:x#p1": {"reviewed": True}, "urn:x#p2": {"reviewed": True}})
        q.insert_metadata({"k": 1}, of="v")
        assert client.insert_metadata.call_args[0][0] == {"urn:x#e1": {"k": 1}}

    def test_errors(self):
        with pytest.raises(ValueError, match="no current node"):
            Query(client=MagicMock()).insert_metadata({"a": 1})
        with pytest.raises(ValueError, match="unknown alias"):
            Query(client=MagicMock()).entity("urn:x#V").insert_metadata({"a": 1}, of="nope")


class TestRegisterStreamsMetadata:
    def test_split_merges_stream_fields_and_keeps_the_rest(self):
        stream, rest = _split_stream_metadata({
            "source_id": "s", "ref_name": "r", "unit": UNIT,
            "metadata": {"label": "Flow", "unit": UNIT, "entity": "x:e", "tags": ["a"]}})
        assert stream == {"source_id": "s", "ref_name": "r", "unit": UNIT, "label": "Flow"}
        assert rest == {"entity": "x:e", "tags": ["a"]}

    def test_conflict_raises(self):
        with pytest.raises(ValueError, match="stream gives unit"):
            _split_stream_metadata({"unit": UNIT, "metadata": {"unit": "urn:other"}})

    def test_register_streams_writes_the_extra_triples(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client.resolve_point_metadata = MagicMock(return_value={"unit": UNIT})
        client._point_metadata = MagicMock(return_value={})
        client._units_compatible = MagicMock(return_value=True)
        client.register_streams([{
            "source_id": "src", "ref_name": "flow", "point_uri": NODE,
            "metadata": {"unit": UNIT, "entity": "x:e", "product_info": {"year": 2019}},
        }])
        turtle, kwargs = client.insert_graph.call_args[0][0], client.insert_graph.call_args[1]
        g = Graph().parse(data=turtle, format="turtle")
        assert (URIRef("urn:x#e"), URIRef(HAS_PROP), URIRef(NODE)) in g
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}product_info.year"), Literal(2019)) in g
        assert (URIRef(NODE), HAS_UNIT, URIRef(UNIT)) in g
        assert kwargs["source_id"] == "src" and kwargs["replace"] is False


META = source_graph_uri(METADATA_SOURCE_ID)
PLANT = URIRef(PLANT_GRAPH_URI)


def dataset_with_orphans() -> Dataset:
    ds = Dataset(default_union=False)
    ds.graph(PLANT).add((URIRef("urn:x#kept"), RDF.type, URIRef("urn:w#Valve")))
    ds.graph(URIRef("urn:onto")).add((URIRef(UNIT), RDF.type, URIRef("urn:q#Unit")))
    m = ds.graph(META)
    m.add((URIRef("urn:x#kept"), URIRef(f"{ATTR_NS}a"), Literal(1)))
    m.add((URIRef("urn:x#kept"), HAS_UNIT, URIRef(UNIT)))            # object in an ontology graph
    m.add((URIRef("urn:x#gone"), URIRef(f"{ATTR_NS}a"), Literal(1)))  # subject gone
    m.add((URIRef("urn:x#kept"), URIRef(HAS_PROP), URIRef("urn:x#gone-point")))  # object gone
    return ds


class TestStore:
    def test_prune_keeps_live_and_drops_orphans(self):
        ds = dataset_with_orphans()
        gs.prune_orphan_metadata(ds, META)
        assert set(ds.graph(META)) == {
            (URIRef("urn:x#kept"), URIRef(f"{ATTR_NS}a"), Literal(1)),
            (URIRef("urn:x#kept"), HAS_UNIT, URIRef(UNIT)),
        }

    def test_closure_triples(self):
        g = Graph()
        g.add((URIRef("urn:a"), RDF.type, URIRef("urn:w#Valve")))
        assert gs._closure_triples(g) == frozenset()
        g.add((URIRef("urn:o"), OWL.imports, URIRef("urn:dep")))
        assert len(gs._closure_triples(g)) == 1

    def make_store(self, ds):
        store = object.__new__(gs.OxigraphGraphStore)
        store._lock = threading.RLock()
        store.source_dataset = ds
        store.query_dataset = Dataset(default_union=False)
        store.main_graph_uri = PLANT
        store.acquirium_graph_uri = URIRef(gs.ACQUIRIUM_GRAPH_URI)
        store._finalize_source_write = MagicMock()
        return store

    def test_sparql_update_only_marks_closure_when_imports_change(self):
        store = self.make_store(dataset_with_orphans())
        store.sparql_update(f'INSERT DATA {{ <urn:x#kept> <{ATTR_NS}b> "v" }}', graph_uri=META)
        store._finalize_source_write.assert_called_with(affects_closure=False)
        store.sparql_update("INSERT DATA { <urn:o> <http://www.w3.org/2002/07/owl#imports> <urn:dep> }",
                            graph_uri=PLANT)
        store._finalize_source_write.assert_called_with(affects_closure=True)

    def test_sparql_update_prunes(self):
        store = self.make_store(dataset_with_orphans())
        store.sparql_update("DELETE WHERE { ?s ?p ?o }", graph_uri=PLANT)
        assert len(store.source_dataset.graph(META)) == 0  # every subject gone with the plant

    def test_replace_insert_prunes_but_append_does_not(self):
        store = self.make_store(dataset_with_orphans())
        store._prune_orphan_metadata = MagicMock()
        store.insert_graph("<urn:x#new> a <urn:w#Valve> .", replace=False, graph_uri=PLANT)
        store._prune_orphan_metadata.assert_not_called()
        store.insert_graph("<urn:x#new> a <urn:w#Valve> .", replace=True, graph_uri=PLANT)
        store._prune_orphan_metadata.assert_called_once()
