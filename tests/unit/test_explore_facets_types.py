"""Typed options() values and the grouped, typed facets() summary."""
from datetime import date
from unittest.mock import MagicMock

import polars as pl

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL
from acquirium.Client.explore.core import Query
from acquirium.Client.explore.facets import FacetSummary
from acquirium.Client.explore.typed import XSD

PREDS = [[f"{ATTR_NS}cleaned", XSD + "date"], [f"{ATTR_NS}cleaned", XSD + "string"],
         [f"{ATTR_NS}info.year", XSD + "integer"], [f"{ATTR_NS}info.maker", XSD + "string"]]


def make_client(lookup):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")

    def responder(sparql, include_dependencies=True, wait_for_fresh=False):
        if sparql == DISCOVERY_SPARQL:
            return {"columns": ["p", "dt"], "rows": PREDS}
        if sparql.startswith("SELECT ?v ?opt"):
            return lookup(sparql)
        return {"columns": ["v0"], "rows": [["urn:p#a"], ["urn:p#b"]], "datatypes": [["iri"], ["iri"]]}

    client.sparql_query.side_effect = responder
    return client


def test_options_values_are_typed_and_distinguish_kinds():
    lookup = lambda s: {"columns": ["v", "opt"], "rows": [["urn:p#a", "2024-03-12"], ["urn:p#b", "2024-03-12"]],
                        "datatypes": [["iri", XSD + "date"], ["iri", None]]}
    df = Query(client=make_client(lookup)).entity("urn:x#A", alias="e").options("cleaned")
    assert df.schema["cleaned"] == pl.Object and df.height == 2
    assert sorted(map(repr, df["cleaned"].to_list())) == ["'2024-03-12'", "datetime.date(2024, 3, 12)"]


def test_options_single_type_column():
    lookup = lambda s: {"columns": ["v", "opt"], "rows": [["urn:p#a", "2019"], ["urn:p#b", "2019"]],
                        "datatypes": [["iri", XSD + "integer"]] * 2}
    df = Query(client=make_client(lookup)).entity("urn:x#A", alias="e").options("info.year")
    assert df.schema == {"info.year": pl.Int64, "count": pl.Int64} and df.rows() == [(2019, 2)]


def test_summary_groups_user_attributes_with_types():
    s = FacetSummary("e")
    s.frames = {"type": pl.DataFrame({"type": ["s223:Pump"], "count": [2]}),
                "info.year": pl.DataFrame({"info.year": [2019], "count": [1]}),
                "info.maker": pl.DataFrame({"info.maker": ["Grundfos"], "count": [1]}),
                "cleaned": pl.DataFrame({"cleaned": pl.Series([date(2024, 3, 12), "2024-03-12"], dtype=pl.Object),
                                         "count": [1, 1]})}
    s.scopes = {k: "matched" for k in s.frames}
    s.dtypes = {"info.year": "Int64", "info.maker": "String", "cleaned": "Object"}
    assert repr(s).splitlines() == [
        "FacetSummary('e')",
        "  type [matched]: s223:Pump (2)",
        "  cleaned <Object> [matched]: datetime.date(2024, 3, 12) (1), '2024-03-12' (1)",
        "  info",
        "    .maker <String> [matched]: Grundfos (1)",
        "    .year <Int64> [matched]: 2019 (1)",
    ]
