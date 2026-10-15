"""include(type=..., strict=...): casting result columns."""
from datetime import date, datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD, CastError, cast_values, normalize_type

CLS_A = "urn:test#TypeA"
PREDS = [f"{ATTR_NS}tags.0", f"{ATTR_NS}tags.1", f"{ATTR_NS}year", f"{ATTR_NS}when"]


def make_client(result):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p"], "rows": [[p] for p in PREDS]} if sparql == DISCOVERY_SPARQL else result)
    return client


class TestCastValues:
    def test_names_types_and_dtypes(self):
        assert normalize_type("int") == normalize_type(int) == normalize_type(pl.Int64) == "int"
        assert normalize_type("str") == normalize_type(pl.String) == "string"
        assert normalize_type(pl.Datetime("ms")) == "datetime"
        with pytest.raises(ValueError, match="unknown type"):
            normalize_type("decimal")

    def test_scalar_conversions(self):
        assert cast_values(["3", 4, 5.0, True], "int")[0] == [3, 4, 5, 1]
        assert cast_values(["2.5", 1, "3"], "float")[0] == [2.5, 1.0, 3.0]
        assert cast_values(["true", 0, True], "bool")[0] == [True, False, True]
        assert cast_values(["2024-03-12", datetime(2024, 3, 12, 5)], "date")[0] == [date(2024, 3, 12)] * 2
        assert cast_values([date(2024, 3, 12), "2024-03-12T01:02:03"], "datetime")[0] == [
            datetime(2024, 3, 12), datetime(2024, 3, 12, 1, 2, 3)]
        assert cast_values([2019, date(2024, 3, 12), {"a": 1}, [1, "x"]], "string")[0] == [
            "2019", "2024-03-12", '{"a": 1}', '[1, "x"]']

    def test_strict_raises_with_value_and_node(self):
        with pytest.raises(CastError, match="cannot cast 'abc' on p:b"):
            cast_values(["2.5", "abc"], "float", labels=["p:a", "p:b"], column="tags")

    def test_non_strict_gives_null(self):
        assert cast_values(["2.5", "abc", None], "float", strict=False)[0] == [2.5, None, None]

    def test_lists_cast_element_wise(self):
        assert cast_values([["2.5", "3"], ["abc"]], "float", strict=False)[0] == [[2.5, 3.0], [None]]
        assert cast_values([["a", 1]], "string")[0] == ['["a", 1]']


class TestIncludeType:
    def rows(self):
        return {"columns": ["v0", "attr0_tags", "attrp0_tags", "attr0_year"],
                "rows": [["urn:p#a", "2.5", f"{ATTR_NS}tags.0", "2019"],
                         ["urn:p#b", "3.5", f"{ATTR_NS}tags.0", "2015"],
                         ["urn:p#c", "abc", f"{ATTR_NS}tags.0", None]],
                "datatypes": [["iri", XSD + "double", "iri", XSD + "integer"],
                              ["iri", None, "iri", None], ["iri", None, "iri", None]]}

    def test_default_is_object_for_a_mixture(self):
        df = Query(client=make_client(self.rows())).entity(CLS_A, alias="e").include("tags", "year").metadata()
        assert df.schema["e.tags"] == pl.Object and df.schema["e.year"] == pl.Object
        assert df["e.tags"].to_list() == [[2.5], ["3.5"], ["abc"]]
        assert df["e.year"].to_list() == [2019, "2015", None]

    def test_type_float_strict_raises_naming_the_node(self):
        q = Query(client=make_client(self.rows())).entity(CLS_A, alias="e").include("tags", type="float")
        with pytest.raises(CastError, match="cannot cast 'abc' on p:c"):
            q.metadata()

    def test_type_float_non_strict(self):
        df = (Query(client=make_client(self.rows())).entity(CLS_A, alias="e")
              .include("tags", type="float", strict=False).include("year", type="int").metadata())
        assert df.schema["e.tags"] == pl.List(pl.Float64) and df["e.tags"].to_list() == [[2.5], [3.5], [None]]
        assert df.schema["e.year"] == pl.Int64 and df["e.year"].to_list() == [2019, 2015, None]

    def test_type_string(self):
        df = (Query(client=make_client(self.rows())).entity(CLS_A, alias="e")
              .include("tags", "year", type="string").metadata())
        assert df.schema["e.tags"] == pl.String and df["e.tags"].to_list() == ['[2.5]', '["3.5"]', '["abc"]']
        assert df["e.year"].to_list() == ["2019", "2015", None]

    def test_cast_recorded_dropped_and_serialised(self):
        q = Query(client=make_client(self.rows())).entity(CLS_A, alias="e").include("year", type="int")
        assert q.query_graph.casts == ((0, "year", "int", True),)
        assert q.to_dict()["casts"] == [{"node": 0, "attr": "year", "type": "int", "strict": True}]
        assert q.include("year").query_graph.casts == ((0, "year", "int", True),)   # re-include keeps it
        assert q.drop("year").query_graph.casts == ()

    def test_bad_type_fails_at_include(self):
        with pytest.raises(ValueError, match="unknown type"):
            Query(client=make_client(self.rows())).entity(CLS_A, alias="e").include("year", type="decimal")
