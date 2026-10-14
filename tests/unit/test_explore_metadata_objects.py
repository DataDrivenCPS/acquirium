"""Query.metadata_objects(): rows as nested Python dicts."""
from datetime import date
from unittest.mock import MagicMock

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL
from acquirium.Client.explore.core import Query, _nest
from acquirium.Client.explore.typed import XSD

CLS_A = "urn:test#TypeA"
PREDS = [[f"{ATTR_NS}product_info.year", XSD + "integer"], [f"{ATTR_NS}product_info.maker", XSD + "string"],
         [f"{ATTR_NS}tags.0", XSD + "string"], [f"{ATTR_NS}tags.1", XSD + "string"],
         [f"{ATTR_NS}cleaned", XSD + "date"]]


def make_client(result):
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p", "dt"], "rows": PREDS} if sparql == DISCOVERY_SPARQL else result)
    return client


class TestNest:
    def test_merge_rules(self):
        d = {}
        _nest(d, ["a", "b"], 1)
        _nest(d, ["a"], {"b": None, "c": 2})       # struct merges, None does not overwrite
        _nest(d, ["a", "b"], None)
        _nest(d, ["x"], "plain")
        _nest(d, ["x", "y"], 3)                    # a plain value holds the key
        assert d == {"a": {"b": 1, "c": 2}, "x": "plain"}
        n = {"p": None}
        _nest(n, ["p", "y"], None)                 # null parent stays null
        _nest(n, ["q", "y"], None)                 # an absent parent gets its leaf
        assert n == {"p": None, "q": {"y": None}}


class TestMetadataDict:
    def test_nested_rows(self):
        res = {"columns": ["v0", "attr0_product_info_46_maker", "attr0_product_info_46_year",
                           "attr0_tags", "attrp0_tags", "attr0_cleaned", "v1", "lbl1"],
               "rows": [["urn:p#a", "Grundfos", "2019", "lab", f"{ATTR_NS}tags.0", "2024-03-12", "urn:p#m", "Flow"],
                        ["urn:p#a", "Grundfos", "2019", "crit", f"{ATTR_NS}tags.1", "2024-03-12", "urn:p#m", "Flow"],
                        ["urn:p#b", None, None, None, None, None, "urn:p#n", None]],
               "datatypes": [["iri", None, XSD + "integer", None, "iri", XSD + "date", "iri", None],
                             ["iri", None, XSD + "integer", None, "iri", XSD + "date", "iri", None],
                             ["iri", None, None, None, None, None, "iri", None]]}
        q = (Query(client=make_client(res)).entity(CLS_A, alias="e").measurement(alias="m")
             .include("product_info", "product_info.year", "tags", "cleaned", of="e"))
        assert q.metadata_objects() == [
            {"e": {"id": "p:a", "product_info": {"maker": "Grundfos", "year": 2019},
                   "tags": ["lab", "crit"], "cleaned": date(2024, 3, 12)},
             "m": {"id": "p:m", "label": "Flow"}},
            {"e": {"id": "p:b", "product_info": None, "tags": None, "cleaned": None},
             "m": {"id": "p:n", "label": None}},
        ]
