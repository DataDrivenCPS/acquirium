"""Query.metadata_objects(): rows as Python dicts in the shape they were saved."""
from datetime import date
from unittest.mock import MagicMock

import pytest

from acquirium.Client.explore.attributes import ATTR_NS, DISCOVERY_SPARQL, clear_registry_cache
from acquirium.Client.explore.core import Query, _lists, _place
from acquirium.Client.explore.typed import CastError, XSD

CLS_A = "urn:test#TypeA"
PREDS = [[f"{ATTR_NS}product_info.year", XSD + "integer"], [f"{ATTR_NS}product_info.maker", XSD + "string"],
         [f"{ATTR_NS}tags.0", XSD + "string"], [f"{ATTR_NS}tags.1", XSD + "string"],
         [f"{ATTR_NS}cleaned", XSD + "date"]]


def make_client(result, preds=PREDS):
    clear_registry_cache()
    client = MagicMock()
    client.base_url = "http://test:8000"
    client.graph_version.return_value = 1
    client.compact_uri.side_effect = lambda x: str(x).replace("urn:p#", "p:")
    client.sparql_query.side_effect = lambda sparql, include_dependencies=True: (
        {"columns": ["p", "dt"], "rows": preds} if sparql == DISCOVERY_SPARQL else result)
    return client


def point(result, preds, *include, **kw):
    q = Query(client=make_client(result, preds)).entity(CLS_A, alias="point")
    return q.include(*include, **kw).metadata_objects()


class TestPlace:
    def test_paths_and_positions(self):
        d = {}
        _place(d, ["a", "b"], 1)
        _place(d, ["a", "c"], 2)
        _place(d, ["items", "1", "count"], 2)
        _place(d, ["items", "0", "name"], "A")
        _place(d, ["items", "1", "name"], "B")
        _place(d, ["matrix", "1", "0"], 3)
        _place(d, ["matrix", "0", "1"], 2)
        assert _lists(d) == {"a": {"b": 1, "c": 2},
                             "items": [{"name": "A"}, {"count": 2, "name": "B"}],
                             "matrix": [[2], [3]]}

    def test_plain_value_and_leaf_under_it(self):
        u = {"unit": "unit:PSI"}
        _place(u, ["unit", "raw"], "PSI")           # a plain value keeps the leaf under its dotted name
        assert u == {"unit": "unit:PSI", "unit.raw": "PSI"}
        v = {}
        _place(v, ["unit", "raw"], "PSI")           # the leaf first, the plain value second
        _place(v, ["unit"], "unit:PSI")
        assert v == {"unit": "unit:PSI", "unit.raw": "PSI"}


class TestMetadataObjects:
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
            {"e": {"id": "urn:p#a", "product_info": {"maker": "Grundfos", "year": 2019},
                   "tags": ["lab", "crit"], "cleaned": date(2024, 3, 12)},
             "m": {"id": "urn:p#m", "label": "Flow"}},
            {"e": {"id": "urn:p#b"}, "m": {"id": "urn:p#n"}},     # nothing saved: only the ids
        ]

    def test_struct_fields_from_other_nodes_are_left_out(self):
        res = {"columns": ["v0", "attr0_settings_46_duration", "attr0_settings_46_mode"],
               "rows": [["urn:p#a", None, "baseline"], ["urn:p#b", "60", "trial"]],
               "datatypes": [["iri", None, None], ["iri", XSD + "integer", None]]}
        preds = [[f"{ATTR_NS}settings.mode", XSD + "string"], [f"{ATTR_NS}settings.duration", XSD + "integer"]]
        q = Query(client=make_client(res, preds)).entity(CLS_A, alias="point").include("settings")
        assert q.metadata()["point.settings"].to_list()[0] == {"duration": None, "mode": "baseline"}  # table: shared schema
        assert q.metadata_objects() == [
            {"point": {"id": "urn:p#a", "settings": {"mode": "baseline"}}},
            {"point": {"id": "urn:p#b", "settings": {"duration": 60, "mode": "trial"}}},
        ]

    def test_list_of_dicts_keeps_each_item_together(self):
        # saved: {"items": [{"name": "A"}, {"name": "B", "count": 2}]}; SPARQL
        # cross-multiplies the two list leaves, the positions undo that
        preds = [[f"{ATTR_NS}items.0.name", XSD + "string"], [f"{ATTR_NS}items.1.name", XSD + "string"],
                 [f"{ATTR_NS}items.1.count", XSD + "integer"]]
        res = {"columns": ["v0", "attr0_items_46_count", "attrp0_items_46_count",
                           "attr0_items_46_name", "attrp0_items_46_name"],
               "rows": [["urn:p#a", "2", f"{ATTR_NS}items.1.count", "A", f"{ATTR_NS}items.0.name"],
                        ["urn:p#a", "2", f"{ATTR_NS}items.1.count", "B", f"{ATTR_NS}items.1.name"]],
               "datatypes": [["iri", XSD + "integer", "iri", None, "iri"]] * 2}
        assert point(res, preds, "items") == [
            {"point": {"id": "urn:p#a", "items": [{"name": "A"}, {"name": "B", "count": 2}]}}]

    def test_nested_list_keeps_its_rows(self):
        preds = [[f"{ATTR_NS}matrix.{i}.{j}", XSD + "integer"] for i in range(2) for j in range(2)]
        res = {"columns": ["v0", "attr0_matrix", "attrp0_matrix"],
               "rows": [["urn:p#b", str(v), f"{ATTR_NS}matrix.{i}.{j}"]
                        for (i, j), v in zip([(1, 1), (0, 0), (1, 0), (0, 1)], [4, 1, 3, 2])],
               "datatypes": [["iri", XSD + "integer", "iri"]] * 4}
        assert point(res, preds, "matrix") == [{"point": {"id": "urn:p#b", "matrix": [[1, 2], [3, 4]]}}]

    def test_plain_value_stays_plain_next_to_a_list_on_another_node(self):
        preds = [[f"{ATTR_NS}tag", XSD + "string"], [f"{ATTR_NS}tag.0", XSD + "string"],
                 [f"{ATTR_NS}tag.1", XSD + "string"]]
        res = {"columns": ["v0", "attr0_tag", "attrp0_tag"],
               "rows": [["urn:p#c", "A", f"{ATTR_NS}tag.0"], ["urn:p#c", "B", f"{ATTR_NS}tag.1"],
                        ["urn:p#d", "A", f"{ATTR_NS}tag"]],
               "datatypes": [["iri", None, "iri"]] * 3}
        q = Query(client=make_client(res, preds)).entity(CLS_A, alias="point").include("tag")
        assert q.metadata()["point.tag"].to_list() == [["A", "B"], ["A"]]   # table: one list column
        assert q.metadata_objects() == [{"point": {"id": "urn:p#c", "tag": ["A", "B"]}},
                                        {"point": {"id": "urn:p#d", "tag": "A"}}]

    def test_id_is_the_uri_unless_one_was_saved(self):
        preds = [[f"{ATTR_NS}id", XSD + "string"]]
        res = {"columns": ["v0", "attr0_id"], "rows": [["urn:p#a", None], ["urn:p#b", "pump-7"]],
               "datatypes": [["iri", None]] * 2}
        assert point(res, preds, "id") == [{"point": {"id": "urn:p#a"}}, {"point": {"id": "pump-7"}}]

    def test_cast_applies_under_the_key(self):
        preds = [[f"{ATTR_NS}items.0.n", XSD + "string"], [f"{ATTR_NS}items.1.n", XSD + "string"]]
        res = {"columns": ["v0", "attr0_items_46_n", "attrp0_items_46_n"],
               "rows": [["urn:p#a", "1", f"{ATTR_NS}items.0.n"], ["urn:p#a", "x", f"{ATTR_NS}items.1.n"]],
               "datatypes": [["iri", None, "iri"]] * 2}
        assert point(res, preds, "items.n", type="int", strict=False) == [
            {"point": {"id": "urn:p#a", "items": [{"n": 1}, {"n": None}]}}]
        with pytest.raises(CastError, match="urn:p#a"):
            point(res, preds, "items.n", type="int")

    def test_builtin_and_its_raw_text(self):
        res = {"columns": ["v0", "attr0_unit", "attr0_unit_46_raw"],
               "rows": [["urn:p#m", "http://qudt.org/vocab/unit/PSI", "PSI"]],
               "datatypes": [["iri", "iri", None]]}
        client = make_client(res, [[f"{ATTR_NS}unit.raw", XSD + "string"]])
        client.compact_uri.side_effect = lambda x: str(x).replace("http://qudt.org/vocab/unit/", "unit:")
        q = Query(client=client).entity(CLS_A, alias="m").include("unit", "unit.raw")
        assert q.metadata_objects() == [{"m": {"id": "urn:p#m", "unit": "unit:PSI", "unit.raw": "PSI"}}]
