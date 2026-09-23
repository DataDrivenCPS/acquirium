"""Tests for attribute expressions: aq.attr paths, comparisons, boolean
operators, their resolution in where(), and their SPARQL."""
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, Not, clear_registry_cache
from acquirium.Client.explore.compile import _term
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.expr import AttrProxy, BoolOp, Compare, Path

CLS_A = "urn:test#TypeA"
UNIT = "http://qudt.org/vocab/unit/MilliGM-PER-L"
HAS_UNIT = "http://qudt.org/schema/qudt/hasUnit"
PREDS = [f"{ATTR_NS}last_cleaned", f"{ATTR_NS}product_info.year",
         f"{ATTR_NS}product_info.manufacturer", f"{ATTR_NS}tags.0", f"{ATTR_NS}tags.1"]


@pytest.fixture(autouse=True)
def clean():
    clear_registry_cache()
    yield
    clear_registry_cache()


def make_client(resolved=None):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p"], "rows": [[p] for p in PREDS]} if sparql == DISCOVERY_SPARQL
        else {"columns": [], "rows": []})
    client.resolve.side_effect = lambda record, min_score=0.4: {
        k: (resolved or {}).get(text) for k, (text, kind) in record.items()}
    return client


def attr(client=None):
    return AttrProxy(client)


class TestPaths:
    def test_unbound_proxy_accepts_anything(self):
        p = attr().anything.nested[3]
        assert isinstance(p, Path) and p.name == "anything.nested.3"
        assert repr(p) == "attr.anything.nested.3"

    def test_bound_proxy_validates_and_completes(self):
        a = attr(make_client())
        assert a.product_info.year.name == "product_info.year"
        assert a.tags[1].name == "tags.1"
        assert "product_info" in dir(a) and "unit" in dir(a)
        assert dir(a.product_info) == ["manufacturer", "year"]
        with pytest.raises(AttributeError, match="unknown attribute 'product_infoo'"):
            a.product_infoo
        with pytest.raises(AttributeError, match="unknown attribute 'product_info.colour'"):
            a.product_info.colour

    def test_call_spells_a_path(self):
        assert attr(make_client())("product_info.year").name == "product_info.year"

    def test_malformed_keys_raise(self):
        with pytest.raises(ValueError, match="invalid attribute path"):
            attr()("product info")
        with pytest.raises(ValueError, match="invalid attribute path"):
            attr()("0.year")
        with pytest.raises(ValueError, match="invalid metadata key"):
            getattr(attr().product_info, "ye ar")
        assert attr()("last-cleaned").name == "last-cleaned"

    def test_index_must_be_int(self):
        with pytest.raises(TypeError):
            attr().tags["0"]

    def test_proxy_and_path_are_read_only(self):
        with pytest.raises(AttributeError):
            attr().x = 1
        with pytest.raises(AttributeError):
            attr().x.y = 1


class TestExpressions:
    def test_comparisons(self):
        p = attr().product_info.year
        for op, e in (("==", p == 2015), ("!=", p != 2015), ("<", p < 2015),
                      ("<=", p <= 2015), (">", p > 2015), (">=", p >= 2015)):
            assert isinstance(e, Compare) and e.op == op and e.value == 2015 and e.attr == "product_info.year"
        assert p.is_in([1, 2]).op == "in" and p.is_in([1, 2]).value == [1, 2]
        assert p.exists().op == "exists"

    def test_boolean_operators(self):
        a, b = attr().x == 1, attr().y == 2
        assert (a & b).op == "and" and (a | b).op == "or" and (~a).op == "not"
        assert ((a & b) | ~a).attributes() == ("x", "y")

    def test_python_and_or_raise(self):
        a, b = attr().x == 1, attr().y == 2
        with pytest.raises(TypeError, match="& \\(and\\)"):
            a and b
        with pytest.raises(TypeError):
            bool(a)

    def test_cannot_compare_two_paths(self):
        with pytest.raises(TypeError):
            attr().x == attr().y

    def test_repr(self):
        assert repr((attr().x >= 1) | ~(attr().y == "a")) == "((attr.x >= 1) | ~(attr.y == 'a'))"


class TestTerms:
    def test_typed_rendering(self):
        assert _term(2015) == "2015" and _term(2.5) == "2.5" and _term(True) == "true"
        assert _term("a") == '"a"' and _term('say "hi"') == '"say \\"hi\\""'
        assert _term(date(2020, 1, 2)).startswith('"2020-01-02"^^')
        assert _term(datetime(2020, 1, 2, 3, 4)).startswith('"2020-01-02T03:04:00"^^')
        assert _term("urn:x#y") == "<urn:x#y>"


class TestWhereWithExpressions:
    def test_equality_folds_into_kwargs(self):
        a = attr(make_client())
        q = Query(client=make_client()).entity(CLS_A, alias="ro").where(a.last_cleaned == "x")
        assert q.query_graph.nodes[0].constraints["attrs"] == {"last_cleaned": "x"}
        assert "exprs" not in q.query_graph.nodes[0].constraints

    def test_not_equal_and_is_in_fold(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro")
             .where(a.last_cleaned != "x", a.tags.is_in(["a", "b"])))
        attrs = q.query_graph.nodes[0].constraints["attrs"]
        assert attrs["last_cleaned"] == Not("x") and attrs["tags"] == ["a", "b"]

    def test_expression_and_kwarg_on_same_attr_both_apply(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro")
             .where(a.last_cleaned == "y", last_cleaned="x"))
        assert q.query_graph.nodes[0].constraints["attrs"] == {"last_cleaned": "x"}
        assert q.query_graph.nodes[0].constraints["exprs"][0].value == "y"

    def test_ordering_compiles_to_exists_filter(self):
        a = attr(make_client())
        q = Query(client=make_client()).entity(CLS_A, alias="ro").where(a.product_info.year >= 2015)
        assert (f"FILTER(EXISTS {{ ?v0 (<{ATTR_NS}product_info.year>) ?_x . FILTER(?_x >= 2015) }})"
                in q.to_sparql())

    def test_or_and_not(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro")
             .where((a.product_info.year >= 2015) | ~(a.product_info.manufacturer == "Siemens")))
        s = q.to_sparql()
        assert ("FILTER((EXISTS { ?v0 (<%sproduct_info.year>) ?_x . FILTER(?_x >= 2015) } || "
                "!(EXISTS { ?v0 (<%sproduct_info.manufacturer>) \"Siemens\" . })))"
                % (ATTR_NS, ATTR_NS)) in s

    def test_list_any_element_and_exists(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro")
             .where((a.tags == "lab") | a.last_cleaned.exists()))
        s = q.to_sparql()
        assert f'EXISTS {{ ?v0 (<{ATTR_NS}tags.0>|<{ATTR_NS}tags.1>) "lab" . }}' in s
        assert f"EXISTS {{ ?v0 (<{ATTR_NS}last_cleaned>) ?_x . }}" in s

    def test_data_node_expression(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro").measurement(alias="m")
             .where((a.product_info.year > 2000) & (a.tags == "x")))
        assert q.query_graph.data_nodes[1].exprs[0].op == "and"
        assert "FILTER((EXISTS { ?v1 (<" in q.to_sparql()

    def test_target_alias_with_expression(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro").measurement(alias="m")
             .where("ro", a.product_info.year > 2000))
        assert q.query_graph.nodes[0].constraints["exprs"][0].op == ">"
        assert q.query_graph.current_pointer == 1

    def test_text_on_builtin_is_resolved(self):
        client = make_client(resolved={"mg/L": UNIT})
        q = (Query(client=client).entity(CLS_A, alias="ro").measurement(alias="m")
             .where((attr(client).unit == "mg/L") | (attr(client).product_info.year > 1)))
        assert f"EXISTS {{ ?v1 (<{HAS_UNIT}>) <{UNIT}> . }}" in q.to_sparql()

    def test_unresolvable_text_raises(self):
        client = make_client()
        with pytest.raises(ValueError, match="Could not resolve 'nope'"):
            (Query(client=client).entity(CLS_A).measurement()
             .where((attr(client).unit == "nope") | (attr(client).tags == "x")))

    def test_ordering_on_uri_attribute_rejected(self):
        client = make_client()
        with pytest.raises(ValueError, match="applies to literal attributes"):
            Query(client=client).entity(CLS_A).measurement().where(attr(client).unit >= "x")

    def test_subclass_attribute_only_bare_equality(self):
        client = make_client()
        Query(client=client).entity(CLS_A).where(attr(client).process == "urn:x#P")
        with pytest.raises(ValueError, match="bare =="):
            Query(client=client).entity(CLS_A).where((attr(client).process == "urn:x#P") | (attr(client).tags == "a"))

    def test_role_check(self):
        client = make_client()
        with pytest.raises(ValueError, match="does not apply to entity"):
            Query(client=client).entity(CLS_A).where(attr(client).unit.exists())

    def test_non_expression_positional_rejected(self):
        with pytest.raises(TypeError, match="expected an attribute expression"):
            Query(client=None).entity(CLS_A).where(None, "x")

    def test_unknown_attribute_in_expression(self):
        with pytest.raises(ValueError, match="unknown attribute 'nope'"):
            Query(client=None).entity(CLS_A).where(attr().nope > 1)

    def test_to_dict_serialises_expressions(self):
        a = attr(make_client())
        q = (Query(client=make_client()).entity(CLS_A, alias="ro").measurement(alias="m")
             .where((a.product_info.year >= 2015) | ~a.last_cleaned.exists()))
        d = q.to_dict()["data_nodes"][0]["exprs"][0]
        assert d == {"op": "or", "operands": [
            {"attr": "product_info.year", "op": ">=", "value": 2015},
            {"op": "not", "operands": [{"attr": "last_cleaned", "op": "exists", "value": None}]}]}


class TestPathsAsColumnNames:
    def test_include_options_drop_take_paths(self):
        client = make_client()
        a = attr(client)
        q = Query(client=client).entity(CLS_A, alias="ro").include(a.product_info.year, a.tags)
        assert [n for _, n, _ in q.query_graph.selects] == ["product_info.year", "tags"]
        assert q.drop(a.tags).query_graph.selects == ((0, "product_info.year", False),)
        client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
            {"columns": ["p"], "rows": [[p] for p in PREDS]} if sparql == DISCOVERY_SPARQL
            else {"columns": ["v0"], "rows": []})
        assert q.options(a.last_cleaned).columns == ["last_cleaned", "count"]


class TestBindings:
    def test_acquirium_and_facade_expose_attr(self):
        from acquirium.Client.acquirium import Acquirium
        from acquirium.Materialization.planner import _QueryFacade
        assert isinstance(Acquirium.attr, property)
        assert isinstance(_QueryFacade(client=None).attr, AttrProxy)
