"""Tests for where() / verb attribute filters and their SPARQL expansion.

URI-valued inputs bypass resolution, so most tests run with ``client=None``;
resolution behavior is tested with a mocked client.
"""

from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.attributes import Not
from acquirium.Client.explore.core import Q

CLS_A = "urn:test#TypeA"
QK_URI = "http://qudt.org/vocab/quantitykind/MassFlowRate"
MEDIUM_URI = "urn:nawi-water-ontology#Water-Brine"
PROCESS_URI = "urn:nawi-water-ontology#OzonationProcess"
OF_MEDIUM = "http://data.ashrae.org/standard223#ofMedium"
HAS_MEDIUM = "http://data.ashrae.org/standard223#hasMedium"


def q() -> Q:
    return Q(client=None)


def base() -> Q:
    return q().entity(CLS_A, alias="ro").measurement(alias="m")


class TestWhereStorage:
    def test_data_node_filter_keyed_by_attr_name(self):
        b = base().where(quantity_kind=QK_URI)
        assert b.query_graph.data_nodes[1].filters == {"quantity_kind": QK_URI}

    def test_entity_attr_stored_in_constraints(self):
        b = q().entity(CLS_A, alias="ro").where(process=PROCESS_URI)
        assert b.query_graph.nodes[0].constraints["attrs"] == {"process": PROCESS_URI}

    def test_not_preserved(self):
        b = base().where(medium=Not(MEDIUM_URI))
        val = b.query_graph.data_nodes[1].filters["medium"]
        assert isinstance(val, Not) and val.value == MEDIUM_URI

    def test_target_alias_and_pointer_preserved(self):
        b = base().where(target="ro", medium=MEDIUM_URI)
        assert b.query_graph.nodes[0].constraints["attrs"] == {"medium": MEDIUM_URI}
        assert b.query_graph.current_pointer == 1  # still the measurement node

    def test_target_star_applies_to_all_data_nodes(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_A, alias="b")
             .measurement(frm="*").where(target="*", unit="urn:test#unit"))
        g = b.query_graph
        assert all(g.data_nodes[nid].filters == {"unit": "urn:test#unit"} for nid in g.data_nodes)

    def test_merge_keeps_existing_filters(self):
        b = base().where(quantity_kind=QK_URI).where(medium=MEDIUM_URI)
        assert set(b.query_graph.data_nodes[1].filters) == {"quantity_kind", "medium"}


class TestWhereErrors:
    def test_unknown_attr(self):
        with pytest.raises(ValueError, match="unknown attribute"):
            base().where(flavour="salty")

    def test_empty(self):
        with pytest.raises(ValueError, match="at least one"):
            base().where()

    def test_role_mismatch_entity(self):
        with pytest.raises(ValueError, match="does not apply to entity"):
            q().entity(CLS_A, alias="ro").where(quantity_kind=QK_URI)

    def test_role_mismatch_data(self):
        with pytest.raises(ValueError, match="does not apply to data"):
            base().where(process=PROCESS_URI)

    def test_unknown_target(self):
        with pytest.raises(ValueError, match="unknown target"):
            base().where(target="nope", medium=MEDIUM_URI)


class TestSparqlExpansion:
    def test_medium_unions_both_predicates(self):
        s = base().where(medium=MEDIUM_URI).to_sparql()
        assert f"{{ ?v1 <{OF_MEDIUM}> <{MEDIUM_URI}> . }} UNION {{ ?v1 <{HAS_MEDIUM}> <{MEDIUM_URI}> . }}" in s

    def test_single_pred_scalar_is_bare_triple(self):
        s = base().where(quantity_kind=QK_URI).to_sparql()
        assert f"?v1 <http://qudt.org/schema/qudt/hasQuantityKind> <{QK_URI}> ." in s

    def test_value_list_unions(self):
        s = base().where(quantity_kind=[QK_URI, "urn:test#qk2"]).to_sparql()
        assert "UNION" in s and "urn:test#qk2" in s

    def test_not_becomes_filter_not_exists(self):
        s = base().where(medium=Not(MEDIUM_URI)).to_sparql()
        assert "FILTER NOT EXISTS {" in s and MEDIUM_URI in s

    def test_literal_attr_quoted(self):
        s = base().where(data_source="Lab").to_sparql()
        assert '?v1 <urn:acquirium#dataSource> "Lab" .' in s

    def test_process_compiles_with_subclass_fence(self):
        s = q().entity(CLS_A, alias="ro").where(process=PROCESS_URI).to_sparql()
        assert f"?v0 (<urn:nawi-water-ontology#hasProcess>) ?v0_process ." in s
        assert "?v0_process <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v0_process_typ ." in s
        assert f"SELECT DISTINCT ?v0_process_typ WHERE {{ ?v0_process_typ <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{PROCESS_URI}>" in s

    def test_type_attr_fences_directly(self):
        s = q().entity(uri="urn:test:instance#x").where(type=CLS_A).to_sparql()
        assert "?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v0_type ." in s
        assert f"SELECT DISTINCT ?v0_type WHERE {{ ?v0_type <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{CLS_A}>" in s

    def test_negated_subclass_attr_uses_anchored_path(self):
        s = q().entity(CLS_A, alias="ro").where(process=Not(PROCESS_URI)).to_sparql()
        assert ("FILTER NOT EXISTS { ?v0 (<urn:nawi-water-ontology#hasProcess>)"
                f"/<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>/<http://www.w3.org/2000/01/rdf-schema#subClassOf>* <{PROCESS_URI}> . }}") in s

    def test_cp_type_on_entity(self):
        cp = "http://data.ashrae.org/standard223#OutletConnectionPoint"
        s = q().entity(CLS_A, alias="ro").where(cp_type=cp).to_sparql()
        assert "?v0 (<http://data.ashrae.org/standard223#hasConnectionPoint>) ?v0_cp_type ." in s


class TestVerbAttrSugar:
    def test_measurement_kwargs(self):
        b = q().entity(CLS_A, alias="ro").measurement(alias="m", quantity_kind=QK_URI,
                                                      medium=Not(MEDIUM_URI))
        f = b.query_graph.data_nodes[1].filters
        assert f["quantity_kind"] == QK_URI and isinstance(f["medium"], Not)

    def test_measurement_star_kwargs_apply_to_all(self):
        b = (q().entity(CLS_A, alias="a").entity(CLS_A, alias="b")
             .measurement(frm="*", unit="urn:test#unit"))
        g = b.query_graph
        assert all(g.data_nodes[nid].filters == {"unit": "urn:test#unit"} for nid in g.data_nodes)

    def test_entity_kwargs(self):
        b = q().entity(CLS_A, alias="ro", process=PROCESS_URI)
        assert b.query_graph.nodes[0].constraints["attrs"] == {"process": PROCESS_URI}

    def test_entity_attrs_only(self):
        b = q().entity(process=PROCESS_URI, alias="e")
        assert "rdf_class" not in b.query_graph.nodes[0].constraints
        assert b.query_graph.nodes[0].constraints["attrs"] == {"process": PROCESS_URI}

    def test_related_kwargs(self):
        b = (q().entity(CLS_A, alias="a")
             .related(CLS_A, alias="b", medium=MEDIUM_URI))
        assert b.query_graph.nodes[1].constraints["attrs"] == {"medium": MEDIUM_URI}


class TestResolution:
    def make_client(self, mapping):
        client = MagicMock()
        client.resolve.return_value = mapping
        return client

    def test_joint_record_and_rewrap(self):
        client = self.make_client({"quantity_kind_0": QK_URI, "medium_0": MEDIUM_URI})
        b = (Q(client=client).entity(CLS_A, alias="ro")
             .measurement(alias="m")
             .where(quantity_kind="mass flow rate", medium=Not("brine")))
        client.resolve.assert_called_once_with(
            {"quantity_kind_0": ("mass flow rate", "quantity_kind"),
             "medium_0": ("brine", "class")},
            min_score=0.4,
        )
        f = b.query_graph.data_nodes[1].filters
        assert f["quantity_kind"] == QK_URI
        assert isinstance(f["medium"], Not) and f["medium"].value == MEDIUM_URI

    def test_uri_passthrough_skips_resolver(self):
        client = self.make_client({})
        Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m").where(quantity_kind=QK_URI)
        client.resolve.assert_not_called()

    def test_literal_attr_skips_resolver(self):
        client = self.make_client({})
        Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m").where(data_source="Lab")
        client.resolve.assert_not_called()

    def test_unresolved_raises(self):
        client = self.make_client({"medium_0": None})
        with pytest.raises(ValueError, match="Could not resolve"):
            Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m").where(medium="xyzzy")

    def test_list_elements_resolved_elementwise(self):
        client = self.make_client({"medium_0": MEDIUM_URI, "medium_1": "urn:test#M2"})
        b = (Q(client=client).entity(CLS_A, alias="ro").measurement(alias="m")
             .where(medium=["brine", "urn:test#M2x".replace("x", "")]))
        # second element is already a URI: only the text goes to the resolver
        record = client.resolve.call_args.args[0]
        assert record == {"medium_0": ("brine", "class")}
        assert b.query_graph.data_nodes[1].filters["medium"] == [MEDIUM_URI, "urn:test#M2"]


class TestProcessKind:
    def test_process_resolves_with_its_own_kind(self):
        client = MagicMock()
        client.resolve.return_value = {"process_0": "urn:nawi-water-ontology#Process-ReverseOsmosis"}
        b = (Q(client=client).entity(CLS_A, alias="eq")
             .where(process="reverse osmosis"))
        client.resolve.assert_called_once_with(
            {"process_0": ("reverse osmosis", "process")}, min_score=0.4)
        assert b.query_graph.nodes[0].constraints["attrs"]["process"] == \
            "urn:nawi-water-ontology#Process-ReverseOsmosis"


class TestAttributeDocs:
    def test_all_attribute_methods_document_the_registry(self):
        from acquirium.Client.explore.attributes import REGISTRY
        for method in (Q.entity, Q.related, Q.measurement, Q.where,
                       Q.include, Q.options, Q.facets):
            doc = method.__doc__ or ""
            assert "Attributes (usable on):" in doc, method.__name__
            for name in REGISTRY:
                assert name in doc, (method.__name__, name)
