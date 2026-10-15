"""Query.schema(): the polars schema metadata() will return, from the registry."""
from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, Registry, user_attributes
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD, dtype_for_datatypes

CLS_A = "urn:test#TypeA"
DISCOVERED = [
    [f"{ATTR_NS}tags.0", XSD + "string"], [f"{ATTR_NS}tags.1", XSD + "string"],
    [f"{ATTR_NS}year", XSD + "integer"], [f"{ATTR_NS}year", XSD + "string"],
    [f"{ATTR_NS}rating", XSD + "double"], [f"{ATTR_NS}rating", XSD + "integer"],
    [f"{ATTR_NS}product_info.year", XSD + "integer"], [f"{ATTR_NS}product_info.name", XSD + "string"],
    [f"{ATTR_NS}cal.limits.max", XSD + "double"], [f"{ATTR_NS}cal.when", XSD + "date"],
    [f"{ATTR_NS}spec", XSD + "string"], [f"{ATTR_NS}spec.volts", XSD + "integer"],
    [f"{ATTR_NS}link", "iri"],
]


def make_client(result=None):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p", "dt"], "rows": DISCOVERED} if sparql == DISCOVERY_SPARQL
        else (result or {"columns": [], "rows": []}))
    return client


class TestDatatypeDiscovery:
    def test_discovery_query_parses(self):
        from rdflib.plugins.sparql import prepareQuery
        prepareQuery(DISCOVERY_SPARQL)

    def test_user_attributes_collect_datatypes(self):
        attrs = user_attributes([r[0] for r in DISCOVERED], [r[1] for r in DISCOVERED])
        assert attrs["year"].datatypes == {XSD + "integer", XSD + "string"}
        assert attrs["tags"].datatypes == {XSD + "string"} and attrs["tags.0"].datatypes == {XSD + "string"}
        assert attrs["link"].datatypes == {"iri"}

    def test_registry_reads_the_dt_column(self):
        r = Registry(make_client())
        assert r["rating"].datatypes == {XSD + "double", XSD + "integer"}
        assert r["unit"].datatypes == frozenset()

    def test_dtype_for_datatypes(self):
        assert dtype_for_datatypes({XSD + "integer"}) == pl.Int64
        assert dtype_for_datatypes({XSD + "integer", XSD + "double"}) == pl.Float64
        assert dtype_for_datatypes({XSD + "date"}) == pl.Date
        assert dtype_for_datatypes({XSD + "dateTime"}) == pl.Datetime("us", "UTC")
        assert dtype_for_datatypes({XSD + "string", "iri"}) == pl.String
        assert dtype_for_datatypes({XSD + "integer", XSD + "string"}) == pl.Object
        assert dtype_for_datatypes(set()) is None


class TestSchema:
    def test_columns_and_dtypes(self):
        q = (Query(client=make_client()).entity(CLS_A, alias="e").measurement(alias="m")
             .include("tags", "year", "rating", "product_info", "product_info.year", "cal", "link", of="e")
             .include("unit", "label"))
        assert q.schema() == pl.Schema({
            "e": pl.String,
            "e.tags": pl.List(pl.String),
            "e.year": pl.Object,                                  # integer and string
            "e.rating": pl.Float64,                               # integer and double widen
            "e.product_info": pl.Struct({"name": pl.String, "year": pl.Int64}),
            "e.product_info.year": pl.Int64,                      # explicit child kept
            "e.cal": pl.Struct({"limits": pl.Struct({"max": pl.Float64}), "when": pl.Date}),
            "e.link": pl.String,                                  # a node value, CURIE text
            "m": pl.String,
            "m.label": pl.String,
            "m.unit": pl.String,                                  # built-ins are String
        })

    def test_value_and_group_is_object_and_casts_show(self):
        q = (Query(client=make_client()).entity(CLS_A, alias="e")
             .include("spec").include("year", type="int").include("tags", type="float"))
        assert q.schema() == pl.Schema({"e": pl.String, "e.spec": pl.Object, "e.year": pl.Int64,
                                        "e.tags": pl.List(pl.Float64)})
        assert q.include("spec", type="string").schema()["e.spec"] == pl.String
        assert q.include("spec", type="struct").schema()["e.spec"] == pl.Object  # no struct fits a value+dict key

    def test_metadata_agrees_with_schema(self):
        res = {"columns": ["v0", "attr0_tags", "attrp0_tags", "attr0_year", "attr0_rating",
                           "attr0_product_info_46_name", "attr0_product_info_46_year", "attr0_link"],
               "rows": [["urn:p#a", "lab", f"{ATTR_NS}tags.0", "2019", "2.5", "x", "2019", "urn:p#z"],
                        ["urn:p#b", None, None, "2015", "1", None, None, None]],
               "datatypes": [["iri", XSD + "string", "iri", XSD + "integer", XSD + "double", XSD + "string",
                              XSD + "integer", "iri"],
                             ["iri", None, None, XSD + "string", XSD + "integer", None, None, None]]}
        q = (Query(client=make_client(res)).entity(CLS_A, alias="e")
             .include("tags", "year", "rating", "product_info", "link"))
        assert q.metadata().schema == q.schema()

    def test_all_null_column_is_typed_from_the_registry(self):
        res = {"columns": ["v0", "attr0_rating", "attr0_cal_46_when", "attr0_tags", "attrp0_tags"],
               "rows": [["urn:p#a", None, None, None, None]],
               "datatypes": [["iri", None, None, None, None]]}
        q = Query(client=make_client(res)).entity(CLS_A, alias="e").include("rating", "cal.when", "tags")
        df = q.metadata()
        assert df.schema == pl.Schema({"e": pl.String, "e.rating": pl.Float64, "e.cal.when": pl.Date,
                                       "e.tags": pl.List(pl.String)})
        assert df.schema == q.schema()

    def test_no_client_schema_is_strings(self):
        q = Query(client=None).entity(CLS_A, alias="e").measurement(alias="m").include("unit")
        assert q.schema() == pl.Schema({"e": pl.String, "m": pl.String, "m.label": pl.String, "m.unit": pl.String})
