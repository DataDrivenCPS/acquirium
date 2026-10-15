"""List attributes: projected with their element predicate, assembled into
one ordered pl.List cell per node by metadata()."""
from unittest.mock import MagicMock

import polars as pl
import pytest
from rdflib.plugins.sparql import prepareQuery

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, is_list_attr, list_index, user_attributes
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD

CLS_A = "urn:test#TypeA"
PREDS = [f"{ATTR_NS}tags", f"{ATTR_NS}tags.0", f"{ATTR_NS}tags.1", f"{ATTR_NS}nums.0", f"{ATTR_NS}nums.1",
         f"{ATTR_NS}items.0.n", f"{ATTR_NS}items.1.n", f"{ATTR_NS}name"]


def make_client(result=None):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p"], "rows": [[p] for p in PREDS]} if sparql == DISCOVERY_SPARQL
        else (result or {"columns": [], "rows": []}))
    return client


class TestHelpers:
    def test_list_index(self):
        assert list_index(f"{ATTR_NS}tags.3") == (3,)
        assert list_index(f"{ATTR_NS}items.1.n") == (1,)
        assert list_index(f"{ATTR_NS}grid.2.0") == (2, 0)
        assert list_index(f"{ATTR_NS}tags") == (-1,)

    def test_is_list_attr(self):
        attrs = user_attributes(PREDS)
        assert is_list_attr(attrs["tags"]) and is_list_attr(attrs["nums"]) and is_list_attr(attrs["items.n"])
        assert not is_list_attr(attrs["tags.0"]) and not is_list_attr(attrs["name"])
        from acquirium.Client.explore.attributes import REGISTRY
        assert not is_list_attr(REGISTRY["medium"])


class TestCompile:
    def test_list_attribute_projects_its_predicate(self):
        q = Query(client=make_client()).entity(CLS_A, alias="e").include("tags", "tags.0", "name")
        s = q.to_sparql()
        assert (f"OPTIONAL {{ VALUES ?attrp0_tags {{ <{ATTR_NS}tags> <{ATTR_NS}tags.0> <{ATTR_NS}tags.1> }} "
                f"?v0 ?attrp0_tags ?attr0_tags . }}") in s
        assert f"OPTIONAL {{ ?v0 (<{ATTR_NS}tags.0>) ?attr0_tags_46_0 . }}" in s
        first = s.splitlines()[0]
        assert "?attr0_tags ?attrp0_tags" in first and "?attr0_name" in first
        prepareQuery(s)

    def test_required_list_has_no_optional(self):
        s = Query(client=make_client()).entity(CLS_A, alias="e").include("tags", required=True).to_sparql()
        assert "VALUES ?attrp0_tags" in s and "OPTIONAL { VALUES" not in s


def rows_for(*entries):
    """entries: (node, tags_value, tags_pred, name) -> result dict with datatypes."""
    rows, dts = [], []
    for node, tv, tp, name in entries:
        rows.append([node, tv, tp, name])
        dts.append(["iri", None if tv is None else None, None if tp is None else "iri", None])
    return {"columns": ["v0", "attr0_tags", "attrp0_tags", "attr0_name"], "rows": rows, "datatypes": dts}


class TestMetadataLists:
    def test_elements_in_index_order_scalar_as_singleton_absent_as_null(self):
        res = rows_for(
            ("urn:p#a", "critical", f"{ATTR_NS}tags.1", "A"),
            ("urn:p#a", "lab", f"{ATTR_NS}tags.0", "A"),
            ("urn:p#b", "lab", f"{ATTR_NS}tags", "B"),       # scalar under the same key
            ("urn:p#c", None, None, "C"),
        )
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("tags", "name").metadata()
        assert df.schema == {"e": pl.String, "e.tags": pl.List(pl.String), "e.name": pl.String}
        assert df["e"].to_list() == ["p:a", "p:b", "p:c"]
        assert df["e.tags"].to_list() == [["lab", "critical"], ["lab"], None]
        assert df.height == 3

    def test_typed_elements(self):
        res = {"columns": ["v0", "attr0_nums", "attrp0_nums"],
               "rows": [["urn:p#a", "2", f"{ATTR_NS}nums.1"], ["urn:p#a", "1", f"{ATTR_NS}nums.0"]],
               "datatypes": [["iri", XSD + "integer", "iri"], ["iri", XSD + "integer", "iri"]]}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("nums").metadata()
        assert df.schema["e.nums"] == pl.List(pl.Int64) and df["e.nums"].to_list() == [[1, 2]]

    def test_two_lists_cross_product_is_undone(self):
        cols = ["v0", "attr0_tags", "attrp0_tags", "attr0_nums", "attrp0_nums"]
        rows = [["urn:p#a", t, tp, n, np_]
                for t, tp in (("lab", f"{ATTR_NS}tags.0"), ("x", f"{ATTR_NS}tags.1"))
                for n, np_ in (("1", f"{ATTR_NS}nums.0"), ("2", f"{ATTR_NS}nums.1"))]
        res = {"columns": cols, "rows": rows,
               "datatypes": [["iri", None, "iri", XSD + "integer", "iri"]] * 4}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("tags", "nums").metadata()
        assert df.height == 1
        assert df["e.tags"].to_list() == [["lab", "x"]] and df["e.nums"].to_list() == [[1, 2]]

    def test_nested_list_leaf(self):
        res = {"columns": ["v0", "attr0_items_46_n", "attrp0_items_46_n"],
               "rows": [["urn:p#a", "2", f"{ATTR_NS}items.1.n"], ["urn:p#a", "1", f"{ATTR_NS}items.0.n"]],
               "datatypes": [["iri", XSD + "integer", "iri"]] * 2}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("items.n").metadata()
        assert df["e.items.n"].to_list() == [[1, 2]]

    def test_index_leaf_stays_scalar(self):
        res = {"columns": ["v0", "attr0_tags_46_0"], "rows": [["urn:p#a", "lab"]], "datatypes": [["iri", None]]}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("tags.0").metadata()
        assert df.schema["e.tags.0"] == pl.String and df["e.tags.0"].to_list() == ["lab"]

    def test_predicate_column_hidden_unless_internals(self):
        res = rows_for(("urn:p#a", "lab", f"{ATTR_NS}tags.0", "A"))
        q = Query(client=make_client(res)).entity(CLS_A, alias="e").include("tags", "name")
        assert "e.tags" in q.metadata().columns and not any("attrp" in c for c in q.metadata().columns)
        assert any("attrp" in c for c in q.metadata(include_internals=True).columns)
