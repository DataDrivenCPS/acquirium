"""Parent keys: include("product_info") is one pl.Struct cell per node."""
from unittest.mock import MagicMock

import polars as pl
import pytest
from rdflib.plugins.sparql import prepareQuery

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, Registry
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD

CLS_A = "urn:test#TypeA"
PREDS = [f"{ATTR_NS}product_info.manufacturer", f"{ATTR_NS}product_info.year",
         f"{ATTR_NS}product_info.build_year", f"{ATTR_NS}calibration.limits.max",
         f"{ATTR_NS}calibration.by", f"{ATTR_NS}calibration.tags.0", f"{ATTR_NS}calibration.tags.1",
         f"{ATTR_NS}items.0.n", f"{ATTR_NS}items.1.n", f"{ATTR_NS}name"]


def make_client(result=None, preds=PREDS):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p"], "rows": [[p] for p in preds]} if sparql == DISCOVERY_SPARQL
        else (result or {"columns": [], "rows": []}))
    return client


class TestRegistryGroups:
    def test_children_and_is_group(self):
        r = Registry(make_client())
        assert [a.name for a in r.children("product_info")] == [
            "product_info.build_year", "product_info.manufacturer", "product_info.year"]
        assert [a.name for a in r.children("calibration")] == [
            "calibration.by", "calibration.limits.max", "calibration.tags"]
        assert [a.name for a in r.children("items")] == ["items.n"]
        assert r.is_group("product_info") and r.is_group("calibration") and r.is_group("items")
        assert not r.is_group("name") and not r.is_group("nope") and "product_info" not in r


class TestCompile:
    def test_group_projects_every_child(self):
        s = Query(client=make_client()).entity(CLS_A, alias="e").include("product_info").to_sparql()
        for leaf in ("manufacturer", "year", "build_year"):
            assert f"?attr0_product_info_46_{leaf}" in s.splitlines()[0]
        prepareQuery(s)

    def test_alias_dot_group(self):
        q = (Query(client=make_client()).entity(CLS_A, alias="e").measurement(alias="m")
             .include("e.product_info"))
        assert q.query_graph.selects == ((0, "product_info", False),)


def result(cols, rows, dts):
    return {"columns": cols, "rows": rows, "datatypes": dts}


class TestMetadataStructs:
    def test_union_of_fields_with_nulls(self):
        res = result(["v0", "attr0_product_info_46_manufacturer", "attr0_product_info_46_year",
                      "attr0_product_info_46_build_year"],
                     [["urn:p#a", "Grundfos", "2019", None], ["urn:p#b", "ProMinent", None, "2015"],
                      ["urn:p#c", None, None, None]],
                     [["iri", None, XSD + "integer", None], ["iri", None, None, XSD + "integer"],
                      ["iri", None, None, None]])
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("product_info").metadata()
        assert df.columns == ["e", "e.product_info"]
        assert df.schema["e.product_info"] == pl.Struct({"build_year": pl.Int64, "manufacturer": pl.String,
                                                         "year": pl.Int64})
        assert df["e.product_info"].to_list() == [
            {"build_year": None, "manufacturer": "Grundfos", "year": 2019},
            {"build_year": 2015, "manufacturer": "ProMinent", "year": None},
            None,
        ]

    def test_nested_struct_and_list_field(self):
        res = result(["v0", "attr0_calibration_46_limits_46_max", "attr0_calibration_46_by",
                      "attr0_calibration_46_tags", "attrp0_calibration_46_tags"],
                     [["urn:p#a", "2.0", "lab", "x", f"{ATTR_NS}calibration.tags.1"],
                      ["urn:p#a", "2.0", "lab", "w", f"{ATTR_NS}calibration.tags.0"]],
                     [["iri", XSD + "double", None, None, "iri"]] * 2)
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").include("calibration").metadata()
        assert df.height == 1
        assert df.schema["e.calibration"] == pl.Struct({
            "by": pl.String, "limits": pl.Struct({"max": pl.Float64}), "tags": pl.List(pl.String)})
        assert df["e.calibration"].to_list() == [{"by": "lab", "limits": {"max": 2.0}, "tags": ["w", "x"]}]

    def test_explicit_child_kept_next_to_struct(self):
        res = result(["v0", "attr0_product_info_46_manufacturer", "attr0_product_info_46_year",
                      "attr0_product_info_46_build_year"],
                     [["urn:p#a", "Grundfos", "2019", None]], [["iri", None, XSD + "integer", None]])
        q = (Query(client=make_client(res)).entity(CLS_A, alias="e")
             .include("product_info", "product_info.year"))
        assert q.to_sparql().splitlines()[0].count("?attr0_product_info_46_year") == 1
        prepareQuery(q.to_sparql())
        df = q.metadata()
        assert df.columns == ["e", "e.product_info", "e.product_info.year"]
        assert df["e.product_info.year"].to_list() == [2019]

    def test_mixed_value_and_group_is_refused(self):
        client = make_client(preds=PREDS + [f"{ATTR_NS}product_info"])
        with pytest.raises(ValueError, match="value on some nodes and a group of keys"):
            Query(client=client).entity(CLS_A, alias="e").include("product_info")

    def test_where_on_group_points_at_leaves(self):
        with pytest.raises(ValueError, match="group of keys; filter one of its leaves"):
            Query(client=make_client()).entity(CLS_A, alias="e").where(product_info="x")
        from acquirium.Client.explore.expr import AttrProxy
        a = AttrProxy(make_client())
        assert a.product_info.year.name == "product_info.year"  # the proxy still descends through a group
