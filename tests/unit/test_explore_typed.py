"""Typed result cells: parse by datatype, typed columns in metadata()."""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import polars as pl
import pytest

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.typed import XSD, column_dtype, parse_cell, typed_column

CLS_A = "urn:test#TypeA"


class TestParseCell:
    @pytest.mark.parametrize("value,datatype,expected", [
        ("2019", XSD + "integer", 2019),
        ("-3", XSD + "int", -3),
        ("2.5", XSD + "double", 2.5),
        ("1.25", XSD + "decimal", 1.25),
        ("true", XSD + "boolean", True),
        ("false", XSD + "boolean", False),
        ("2024-03-12", XSD + "date", date(2024, 3, 12)),
        ("2020-01-02T03:04:05", XSD + "dateTime", datetime(2020, 1, 2, 3, 4, 5)),
        ("2020-01-02T03:04:05Z", XSD + "dateTime", datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        ("text", None, "text"),
        ("urn:x#a", "iri", "urn:x#a"),
        (None, XSD + "integer", None),
        ("not-a-number", XSD + "integer", "not-a-number"),
        ("x", XSD + "gYear", "x"),
    ])
    def test_values(self, value, datatype, expected):
        assert parse_cell(value, datatype) == expected


class TestColumnDtype:
    def test_single_kinds(self):
        assert column_dtype([1, None, 2]) == pl.Int64
        assert column_dtype([1.5]) == pl.Float64
        assert column_dtype([True]) == pl.Boolean
        assert column_dtype([date(2020, 1, 1)]) == pl.Date
        assert column_dtype([datetime(2020, 1, 1)]) == pl.Datetime("us", "UTC")
        assert column_dtype(["a"]) == pl.String
        assert column_dtype([None, None]) == pl.String

    def test_int_and_float_widen_to_float(self):
        values, dtype = typed_column(["1", "2.5"], [XSD + "integer", XSD + "double"])
        assert dtype == pl.Float64 and values == [1.0, 2.5]

    def test_other_mixtures_are_object_with_values_as_written(self):
        values, dtype = typed_column(["2019", "2019", "true", "2024-03-12"],
                                     [XSD + "integer", None, XSD + "boolean", XSD + "date"])
        assert dtype == pl.Object and values == [2019, "2019", True, date(2024, 3, 12)]


def make_client(result):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.sparql_query.return_value = result
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    return client


class TestMetadataTyping:
    def test_typed_columns_from_datatypes(self):
        res = {
            "columns": ["v0", "attr0_year", "attr0_ok", "attr0_when", "attr0_name"],
            "rows": [["urn:p#a", "2019", "true", "2024-03-12", "x"],
                     ["urn:p#b", "2015", "false", None, "urn:not-a-node"]],
            "datatypes": [["iri", XSD + "integer", XSD + "boolean", XSD + "date", None],
                          ["iri", XSD + "integer", XSD + "boolean", None, None]],
        }
        q = (Query(client=make_client(res)).entity(CLS_A, alias="e")
             .include("label"))  # any select; column names come from the fake result
        df = q.metadata()
        assert df.schema["e"] == pl.String and df["e"].to_list() == ["p:a", "p:b"]
        assert df.schema["e.year"] == pl.Int64 and df["e.year"].to_list() == [2019, 2015]
        assert df.schema["e.ok"] == pl.Boolean
        assert df.schema["e.when"] == pl.Date and df["e.when"].to_list() == [date(2024, 3, 12), None]
        # a plain literal that happens to look like a URI is not compacted
        assert df["e.name"].to_list() == ["x", "urn:not-a-node"]

    def test_without_datatypes_everything_is_string_as_before(self):
        res = {"columns": ["v0", "attr0_year"], "rows": [["urn:p#a", "2019"]]}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").metadata()
        assert df.schema == {"e": pl.String, "e.year": pl.String}
        assert df["e"].to_list() == ["p:a"]

    def test_mixed_int_and_string_column_is_object(self):
        res = {"columns": ["v0", "attr0_year"], "rows": [["urn:p#a", "2019"], ["urn:p#b", "2015"]],
               "datatypes": [["iri", XSD + "integer"], ["iri", None]]}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").metadata()
        assert df.schema["e.year"] == pl.Object and df["e.year"].to_list() == [2019, "2015"]

    def test_duplicate_rows_collapse(self):
        res = {"columns": ["v0"], "rows": [["urn:p#a"], ["urn:p#a"]], "datatypes": [["iri"], ["iri"]]}
        df = Query(client=make_client(res)).entity(CLS_A, alias="e").metadata()
        assert df.height == 1
