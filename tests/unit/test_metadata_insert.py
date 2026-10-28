"""Tests for insert_metadata: the value-map builder (Client/metadata.py), the
client/facade/query entry points, the register_streams ``metadata`` key, and
and the store-side closure check."""
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
    return plan_write(NODE, values, role=role, resolve=noop_resolve)


class TestFlatten:
    def test_nested_and_lists(self):
        leaves, removed = flatten({
            "a": 1, "b": {"c": "x", "d": {"e": True}}, "tags": ["p", "q", "p"],
            "items": [{"n": 1}, {"n": 2}], "gone": None, "skip": {"k": None},
        })
        assert leaves == {"a": 1, "b.c": "x", "b.d.e": True, "tags.0": "p", "tags.1": "q",
                          "tags.2": "p", "items.0.n": 1, "items.1.n": 2}
        assert removed == ["gone"]

    def test_empty_containers_remove_the_key(self):
        leaves, removed = flatten({"tags": [], "info": {}, "gone": None, "keep": {"a": [], "b": 1}})
        assert leaves == {"keep.b": 1}
        assert removed == ["tags", "info", "gone"]
        w = plan({"tags": []})
        assert w.replace_keys == ["tags"] and w.triples == []

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
        datetime(2020, 1, 2, 3, 4, 5), "see https://example.org/a for details",
        '{"link": "https://example.org/a"}', "https://example.org/a contains a note",
        "urn:example:also a note",
    ])
    def test_writer_and_compiler_agree(self, value):
        assert to_term(value).n3() == _term(value)

    @pytest.mark.parametrize("value", ["urn:x#node", "https://example.org/a"])
    def test_uri_looking_string_matches_either_way(self, value):
        # written as a URI when the node is known, as text otherwise; a
        # filter on a user attribute matches both
        from acquirium.Client.explore.compile import _value_terms
        terms = _value_terms([value])
        assert to_term(value, namespaces=[value[:-1]]).n3() in terms
        assert to_term(value).n3() in terms and len(terms) == 2
        assert _value_terms([URIRef(value)]) == [f"<{value}>"]

    def test_uri_only_under_a_known_namespace(self):
        ns = ["urn:x#", "http://qudt.org/vocab/unit/"]
        assert to_term("https://example.org/a", ns) == Literal("https://example.org/a")
        assert to_term("urn:x#tank", ns) == URIRef("urn:x#tank")
        assert to_term("http://qudt.org/vocab/unit/PSI", ns) == URIRef("http://qudt.org/vocab/unit/PSI")
        assert to_term("urn:x#", ns) == Literal("urn:x#")             # the namespace itself names nothing
        assert to_term("urn:xy", ns) == Literal("urn:xy")
        assert to_term(URIRef("https://example.org/a"), ns) == URIRef("https://example.org/a")
        # text that mentions a link is text, whatever its namespace
        assert to_term("urn:x#see this", ns) == Literal("urn:x#see this")

    def test_unsupported(self):
        with pytest.raises(TypeError):
            to_term(object())


class TestPlanWrite:
    def test_user_attributes_flatten_to_typed_leaves(self):
        w = plan({"last_cleaned": "1999", "product_info": {"year": 2019, "ok": True}, "tags": ["a", "b"]})
        assert w.replace_keys == ["last_cleaned", "product_info", "tags"]
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}product_info.year"), Literal(2019)) in w.triples
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}tags.1"), Literal("b")) in w.triples
        assert len([t for t in w.triples if not str(t[1]).startswith("urn:acquirium:metadata:document:")]) == 5

    def test_uri_string_is_a_uri_only_under_a_known_namespace(self):
        w = plan({"datasheet": "https://example.org/ds.pdf"})
        assert w.triples[0][2] == Literal("https://example.org/ds.pdf")
        w = plan_write(NODE, {"feeds": "urn:x#tank"}, role="entity", resolve=lambda t, k: None,
                       namespaces=["urn:x#"])
        assert w.triples[0][2] == URIRef("urn:x#tank")

    def test_builtin_resolves_text_and_keeps_it(self):
        w = plan({"unit": "mg/L"})
        assert str(HAS_UNIT) in w.replace_forward
        assert w.triples[:2] == [(URIRef(NODE), URIRef(f"{ATTR_NS}unit.raw"), Literal("mg/L")),
                             (URIRef(NODE), HAS_UNIT, URIRef(UNIT))]
        assert w.replace_keys == ["unit"]          # deletes a previous unit.raw too

    def test_uri_value_stores_no_raw_text(self):
        w = plan({"unit": UNIT})
        assert w.triples[:1] == [(URIRef(NODE), HAS_UNIT, URIRef(UNIT))] and w.replace_keys == ["unit"]

    def test_literal_builtin_has_no_raw_text(self):
        w = plan({"label": "Influent"})
        assert w.triples[:1] == [(URIRef(NODE), RDFS.label, Literal("Influent"))] and w.replace_keys == []

    def test_builtin_unresolvable_raises(self):
        with pytest.raises(ValueError, match="Could not resolve"):
            plan({"unit": "furlongs"})

    def test_semantic_lists_share_declaration_resolution_and_preserve_nulls(self):
        from acquirium.ExperimentDeclarations import Declaration
        values = ["mg/L", None, UNIT, "mg/L"]
        resolve = MagicMock(side_effect=noop_resolve)
        declaration = Declaration("concentration", "scalar", unit=values).resolve(resolve)
        assert declaration.resolved == {"unit": [UNIT, None, UNIT, UNIT]}
        assert resolve.call_count == 2
        resolve.reset_mock()
        write = plan_write(NODE, {"unit": values}, role="data", resolve=resolve,
                           resolved=declaration.resolved)
        resolve.assert_not_called()
        assert [str(o) for _, p, o in write.triples if p == HAS_UNIT] == [UNIT] * 3
        assert [str(o) for _, p, o in write.triples if str(p) == ATTR_NS + "unit.raw"] == ["mg/L"] * 2

    def test_non_strict_projection_resolves_once_and_omits_whole_unresolved_field(self):
        from acquirium.ExperimentMetadata import ExperimentMetadataGraph
        resolve = MagicMock(side_effect=noop_resolve)
        writer = ExperimentMetadataGraph(None, resolve, strict=False)
        write = writer.validate(NODE, {"unit": ["mg/L", None, UNIT]}, variable=True)
        resolve.assert_called_once_with("mg/L", "unit")
        assert [str(o) for _, p, o in write.triples if p == HAS_UNIT] == [UNIT, UNIT]
        write = writer.validate(NODE, {"unit": ["mg/L", "unknown"]}, variable=True)
        assert not any(p == HAS_UNIT or str(p) == ATTR_NS + "unit.raw" for _, p, _ in write.triples)
        # Complete documents retain the unresolved spelling for later repair.
        from acquirium.Metadata import document_triples
        assert set(document_triples(NODE, {"unit": ["mg/L", "unknown"]})) <= set(write.triples)

    @pytest.mark.parametrize("original", [{}, {"unit": None}, {"unit": "old spelling"}])
    def test_pinned_terms_project_without_raw_fields_or_a_live_resolver(self, original):
        from acquirium.ExperimentMetadata import ExperimentMetadataGraph
        resolve = MagicMock(return_value=None)
        write = ExperimentMetadataGraph(None, resolve, strict=False).validate(
            NODE, {**original, "_resolved_semantics": {"unit": UNIT}}, variable=True)
        resolve.assert_not_called()
        assert (URIRef(NODE), HAS_UNIT, URIRef(UNIT)) in write.triples
        raw = [str(o) for _, p, o in write.triples if str(p) == ATTR_NS + "unit.raw"]
        assert raw == ([original["unit"]] if original.get("unit") else [])

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

    def test_none_removes(self):
        w = plan({"tags": None, "unit": None})
        assert sorted(w.replace_keys) == ["tags", "unit"] and str(HAS_UNIT) in w.replace_forward
        assert w.triples == []

    @pytest.mark.parametrize("key", ["entity", "measurement", "upstream", "downstream"])
    def test_relation_names_are_refused(self, key):
        with pytest.raises(ValueError, match="names a relation"):
            plan({key: "x:e"})


class TestUpdateText:
    def test_deletes_name_each_subject_and_inserts_once(self):
        a = plan({"tags": ["x"], "unit": UNIT})
        b = Write(subject=URIRef("urn:x#other"))
        b.replace_keys, b.replace_forward = list(a.replace_keys), list(a.replace_forward)
        b.triples = [(b.subject, URIRef(f"{ATTR_NS}tags.0"), Literal("y"))]
        c = plan({"other": 1})
        c.subject = URIRef("urn:x#third")
        text = update_text([a, b, c])
        ops = text.split(" ;\n")
        assert len(ops) == 4
        # A bound subject is an index lookup; a VALUES join scans the graph.
        assert "VALUES" not in text
        for op, subject in zip(ops, (NODE, "urn:x#other", "urn:x#third")):
            assert op.startswith(f"DELETE {{ <{subject}> ?p ?o }}") and f"<{subject}> ?p ?o ." in op
        assert f'STRSTARTS(STR(?p), "{ATTR_NS}tags.")' in ops[0] and f"?p = <{ATTR_NS}tags>" in ops[0]
        assert f"?p = <{HAS_UNIT}>" in ops[0]
        assert ops[3].startswith("INSERT DATA {") and '"y"' in ops[3] and f"<{UNIT}>" in ops[3]

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
    client._namespace_uris = MagicMock(return_value=("urn:x#",))
    return client


XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


class TestClientInsert:
    def test_roles_from_external_reference(self):
        client = make_client([[NODE, "urn:x#ref"], ["urn:x#e", None]])
        assert client.node_roles([NODE, "urn:x#e", "urn:x#missing"]) == {NODE: "data", "urn:x#e": "entity"}
        sparql = client.sparql_query.call_args[0][0]
        assert f"<{HAS_EXTERNAL_REFERENCE}>" in sparql and f"<{NODE}>" in sparql
        assert client.sparql_query.call_args[1] == {"include_dependencies": False}

    def test_insert_builds_update_in_metadata_graph(self):
        client = make_client([[NODE, "urn:x#ref"]], resolved={"mg/L": UNIT})
        out = client.insert_metadata({"x:valve-1": {"unit": "mg/L", "tags": ["a"]}})
        assert out == {"ok": True, "message": "update applied", "nodes": 1}
        update, kwargs = client.sparql_update.call_args[0][0], client.sparql_update.call_args[1]
        assert kwargs == {"source_id": METADATA_SOURCE_ID, "effective_from": None, "effective_until": None}
        assert f"<{NODE}> <{HAS_UNIT}> <{UNIT}>" in update
        assert f'<{NODE}> <{ATTR_NS}tags.0> "a"' in update

    def test_full_uri_and_curie_subjects(self):
        client = make_client([[NODE, "urn:x#ref"]])
        client.expand_uri = MagicMock(side_effect=lambda s: (_ for _ in ()).throw(ValueError(s))
                                      if "://" in s or s.startswith("urn:") else expand(s))
        client.insert_metadata({NODE: {"a": 1}, "x:valve-1": {"b": 2}})
        assert client.node_uri("x:valve-1") == NODE and client.node_uri(NODE) == NODE

    def test_roles_ignore_user_attribute_links(self):
        # a link saved as a value is mentioned only through attr: predicates
        client = make_client([])
        client.node_roles(["https://example.org/ds.pdf"])
        sparql = client.sparql_query.call_args[0][0]
        assert sparql.count(f'FILTER(!STRSTARTS(STR(?p), "{ATTR_NS}"))') == 1
        assert sparql.count(f'FILTER(!STRSTARTS(STR(?q), "{ATTR_NS}"))') == 1

    def test_insert_writes_uris_under_known_namespaces(self):
        client = make_client([[NODE, None]])
        client.insert_metadata({NODE: {"feeds": "urn:x#tank", "sheet": "https://e.org/a.pdf",
                                       "forced": URIRef("https://e.org/b"), "n": 1}})
        update = client.sparql_update.call_args[0][0]
        assert f"<{NODE}> <{ATTR_NS}feeds> <urn:x#tank>" in update
        assert f'<{NODE}> <{ATTR_NS}sheet> "https://e.org/a.pdf"' in update
        assert f"<{NODE}> <{ATTR_NS}forced> <https://e.org/b>" in update
        assert client.sparql_query.call_count == 1               # the role check; no lookup

    def test_namespace_uris_from_the_server_list(self):
        client = AcquiriumClient.__new__(AcquiriumClient)
        client.base_url = "http://test:8000"
        client._namespaces_cache = None
        client._http = MagicMock()
        client._http.get.return_value = MagicMock(status_code=200, json=lambda: {"ex": "urn:example:"})
        assert "urn:example:" in client._namespace_uris()
        client._namespace_uris()
        assert client._http.get.call_count == 1                  # cached
        client._http.post.return_value = MagicMock(status_code=200)
        client.insert_graph("<urn:a> a <urn:b> .", source_id="plant")
        client._namespace_uris()
        assert client._http.get.call_count == 2                  # a graph insert may bind new prefixes

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
        aq.client.insert_metadata.assert_called_once_with({NODE: {"a": 1}}, effective_from=None, effective_until=None)


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
            "metadata": {"label": "Flow", "unit": UNIT, "tags": ["a"]}})
        assert stream == {"source_id": "s", "ref_name": "r", "unit": UNIT, "label": "Flow"}
        assert rest == {"tags": ["a"]}

    def test_conflict_raises(self):
        with pytest.raises(ValueError, match="stream gives unit"):
            _split_stream_metadata({"unit": UNIT, "metadata": {"unit": "urn:other"}})

    def test_register_streams_keeps_resolved_text(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client.resolve_point_metadata = MagicMock(return_value={"unit": UNIT})
        client._point_metadata = MagicMock(return_value={})
        client._units_compatible = MagicMock(return_value=True)
        client.register_streams([{"source_id": "src", "ref_name": "flow", "point_uri": NODE, "unit": "mg/L"},
                                 {"source_id": "src", "ref_name": "f2", "point_uri": "urn:x#p2", "unit": UNIT}])
        g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}unit.raw"), Literal("mg/L")) in g
        assert not list(g.triples((URIRef("urn:x#p2"), URIRef(f"{ATTR_NS}unit.raw"), None)))  # a URI has no text

    def test_register_streams_keeps_text_that_did_not_resolve(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client.resolve_point_metadata = MagicMock(return_value={"quantity_kind": None})
        client._point_metadata = MagicMock(return_value={})
        with pytest.warns(UserWarning, match="Could not resolve"):
            client.register_streams([{"source_id": "src", "ref_name": "f", "point_uri": NODE,
                                      "quantity_kind": "VolumeFlowRate"}])
        g = Graph().parse(data=client.insert_graph.call_args[0][0], format="turtle")
        assert (URIRef(NODE), URIRef(f"{ATTR_NS}quantity_kind.raw"), Literal("VolumeFlowRate")) in g

    def test_register_streams_writes_the_extra_triples(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client.resolve_point_metadata = MagicMock(return_value={"unit": UNIT})
        client._point_metadata = MagicMock(return_value={})
        client._units_compatible = MagicMock(return_value=True)
        client.register_streams([{
            "source_id": "src", "ref_name": "flow", "point_uri": NODE,
            "metadata": {"unit": UNIT, "product_info": {"year": 2019}},
        }])
        turtle, kwargs = client.insert_graph.call_args[0][0], client.insert_graph.call_args[1]
        g = Graph().parse(data=turtle, format="turtle")
        # the stream field merges into the stream, in the driver's graph
        assert (URIRef(NODE), HAS_UNIT, URIRef(UNIT)) in g
        assert kwargs["source_id"] == "src" and kwargs["replace"] is False
        # the user attribute goes to the metadata graph, replacing the key
        assert not list(g.triples((None, URIRef(f"{ATTR_NS}product_info.year"), None)))
        update, ukw = client.sparql_update.call_args[0][0], client.sparql_update.call_args[1]
        # a registration holds over all time
        assert ukw == {"source_id": METADATA_SOURCE_ID, "effective_from": "-infinity", "effective_until": "infinity"}
        assert f'STRSTARTS(STR(?p), "{ATTR_NS}product_info.")' in update
        assert f'<{NODE}> <{ATTR_NS}product_info.year> "2019"^^<{XSD_INTEGER}>' in update
        assert client.insert_graph.call_count == 1   # the stream first, then the metadata

    def test_register_streams_merges_metadata_per_point(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client._point_metadata = MagicMock(return_value={})
        client.register_streams([
            {"source_id": "src", "ref_name": "a", "point_uri": NODE, "metadata": {"year": 2019, "loop": "L1"}},
            {"source_id": "src", "ref_name": "b", "point_uri": NODE, "metadata": {"year": 2020}},
        ])
        update = client.sparql_update.call_args[0][0]
        assert f'<{NODE}> <{ATTR_NS}year> "2020"^^<{XSD_INTEGER}>' in update and "2019" not in update
        assert f'<{NODE}> <{ATTR_NS}loop> "L1"' in update

    def test_register_streams_bad_metadata_writes_nothing(self):
        client = make_client([])
        client.insert_graph = MagicMock()
        client._point_metadata = MagicMock(return_value={})
        with pytest.raises(ValueError, match="invalid metadata key"):
            client.register_streams([{"source_id": "src", "ref_name": "a", "point_uri": NODE,
                                      "metadata": {"bad key": 1}}])
        client.insert_graph.assert_not_called()
        client.sparql_update.assert_not_called()


META = source_graph_uri(METADATA_SOURCE_ID)
PLANT = URIRef(PLANT_GRAPH_URI)


def dataset_with_metadata(native: bool = False) -> Dataset:
    if native:
        import pyoxigraph
        from oxrdflib.store import OxigraphStore
        ds = Dataset(store=OxigraphStore(store=pyoxigraph.Store()), default_union=False)
    else:
        ds = Dataset(default_union=False)
    ds.graph(PLANT).add((URIRef("urn:x#kept"), RDF.type, URIRef("urn:w#Valve")))
    ds.graph(URIRef("urn:onto")).add((URIRef(UNIT), RDF.type, URIRef("urn:q#Unit")))
    m = ds.graph(META)
    m.add((URIRef("urn:x#kept"), URIRef(f"{ATTR_NS}a"), Literal(1)))
    m.add((URIRef("urn:x#kept"), HAS_UNIT, URIRef(UNIT)))            # object in an ontology graph
    m.add((URIRef("urn:x#gone"), URIRef(f"{ATTR_NS}a"), Literal(1)))  # subject gone
    m.add((URIRef("urn:x#kept"), URIRef(HAS_PROP), URIRef("urn:x#gone-point")))  # object unknown
    m.add((URIRef("urn:x#kept"), URIRef(f"{ATTR_NS}datasheet"), URIRef("https://example.org/p.pdf")))
    return ds


class TestStore:
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
        store = self.make_store(dataset_with_metadata())
        store.sparql_update(f'INSERT DATA {{ <urn:x#kept> <{ATTR_NS}b> "v" }}', graph_uri=META)
        store._finalize_source_write.assert_called_with(affects_closure=False)
        store.sparql_update("INSERT DATA { <urn:o> <http://www.w3.org/2002/07/owl#imports> <urn:dep> }",
                            graph_uri=PLANT)
        store._finalize_source_write.assert_called_with(affects_closure=True)

    def test_metadata_update_runs_natively(self):
        store = self.make_store(dataset_with_metadata(native=True))
        metadata = len(store.source_dataset.graph(META))
        store.metadata_update(f"DELETE WHERE {{ GRAPH <{PLANT}> {{ ?s ?p ?o }} }}", graph_uri=PLANT)
        assert len(store.source_dataset.graph(PLANT)) == 0
        # metadata about nodes that are gone stays: the model history keeps
        # every graph's past, so nothing is removed on another graph's behalf
        assert len(store.source_dataset.graph(META)) == metadata
        store._finalize_source_write.assert_called_once_with(affects_closure=False)
        with pytest.raises(ValueError, match="not a registered data input"):
            store.metadata_update("INSERT DATA { GRAPH <urn:onto> { <urn:x#a> <urn:x#b> 1 } }", graph_uri=URIRef("urn:onto"))

    def test_clear_graphs_empties_named_and_prefixed_graphs(self):
        ds = dataset_with_metadata()
        runs = [source_graph_uri(f"experiment/run-{i}") for i in range(2)]
        for uri in runs:
            ds.graph(uri).add((URIRef(f"{uri}#point"), RDF.type, URIRef("urn:w#Point")))
        ledger = source_graph_uri("experiments")
        ds.graph(ledger).add((URIRef("urn:x#run"), RDF.type, URIRef("urn:w#Run")))
        store = self.make_store(ds)
        assert store.clear_graphs(prefix=str(source_graph_uri("experiment/"))) == 2
        assert all(len(ds.graph(uri)) == 0 for uri in runs)
        assert len(ds.graph(ledger)) == 1  # "experiments" is not under "experiment/"
        store._finalize_source_write.assert_called_once_with(affects_closure=False)
        assert store.clear_graphs([runs[0]]) == 0  # already empty: no write
        store._finalize_source_write.assert_called_once()
