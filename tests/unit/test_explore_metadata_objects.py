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
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True, wait_for_fresh=False: (
        {"columns": ["p", "dt"], "rows": PREDS} if sparql == DISCOVERY_SPARQL else result)
    return client


class TestNest:
    def test_merge_rules(self):
        d = {}
        _nest(d, ["a", "b"], 1)
        _nest(d, ["a"], {"b": None, "c": 2})       # struct merges, None does not overwrite
        _nest(d, ["a", "b"], None)
        _nest(d, ["x"], "plain")
        _nest(d, ["x", "y"], 3)                    # a plain value holds the key; the leaf goes beside it
        assert d == {"a": {"b": 1, "c": 2}, "x": "plain", "x.y": 3}
        n = {"p": None}
        _nest(n, ["p", "y"], None)                 # null parent stays null
        _nest(n, ["q", "y"], None)                 # an absent parent gets its leaf
        assert n == {"p": None, "q": {"y": None}}
        u = {"unit": "unit:PSI"}
        _nest(u, ["unit", "raw"], "PSI")           # a plain value keeps the leaf under its dotted name
        assert u == {"unit": "unit:PSI", "unit.raw": "PSI"}
        v = {}
        _nest(v, ["unit", "raw"], "PSI")           # the leaf first, the plain value second
        _nest(v, ["unit"], "unit:PSI")
        assert v == {"unit": "unit:PSI", "unit.raw": "PSI"}


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
            {"e": {"id": "p:b"}, "m": {"id": "p:n"}},     # nothing saved: only the ids
        ]

    def test_struct_fields_from_other_nodes_are_left_out(self):
        res = {"columns": ["v0", "attr0_settings_46_duration", "attr0_settings_46_mode"],
               "rows": [["urn:p#a", None, "baseline"], ["urn:p#b", "60", "trial"]],
               "datatypes": [["iri", None, None], ["iri", XSD + "integer", None]]}
        preds = [[f"{ATTR_NS}settings.mode", XSD + "string"], [f"{ATTR_NS}settings.duration", XSD + "integer"]]
        client = make_client(res)
        client.sparql_query.side_effect = lambda sparql, include_dependencies=True, wait_for_fresh=False: (
            {"columns": ["p", "dt"], "rows": preds} if sparql == DISCOVERY_SPARQL else res)
        q = Query(client=client).entity(CLS_A, alias="point").include("settings")
        assert q.metadata()["point.settings"].to_list()[0] == {"duration": None, "mode": "baseline"}  # table: shared schema
        assert q.metadata_objects() == [
            {"point": {"id": "p:a", "settings": {"mode": "baseline"}}},
            {"point": {"id": "p:b", "settings": {"duration": 60, "mode": "trial"}}},
        ]

    def test_saved_keeps_lists_as_they_are(self):
        from acquirium.Client.explore.core import _saved
        assert _saved({"a": None, "b": {"c": None}, "d": [1, None], "e": {"f": {"g": None}, "h": 0}}) == {
            "d": [1, None], "e": {"h": 0}}
