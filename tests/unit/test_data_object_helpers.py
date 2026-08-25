"""Unit tests for DataObject's pure frame-shaping helpers."""

from datetime import datetime, timezone

import polars as pl

from acquirium.Client.data_object import _pivot_split_values


def _tall(keys):
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return pl.DataFrame({
        "key": keys,
        "time": [t] * len(keys),
        "value_numeric": [1.0] * len(keys),
        "value_text": [None] * len(keys),
    })


class TestWideColumnOrder:
    def test_time_first_then_case_insensitive_alphabetical(self):
        tall = _tall(["data__ns1:storage-tank-3-out-tds",
                      "data__ns1:PXR-brine-out-tds",
                      "data__ns1:intake-in-tds"])
        wide = _pivot_split_values(tall, "key")
        assert wide.columns == ["time",
                                "data__ns1:intake-in-tds",
                                "data__ns1:PXR-brine-out-tds",
                                "data__ns1:storage-tank-3-out-tds"]


class TestEmptyDataObjectConversion:
    def test_convert_to_on_empty_is_a_noop(self):
        from unittest.mock import MagicMock
        from acquirium.Client.data_object import DataObject
        from acquirium.Client.query_graph import QueryGraph

        client = MagicMock()
        empty = DataObject._empty(QueryGraph(), client=client)
        out = empty.convert_to("mg/L")
        assert out.is_empty()
        client.resolve_conversion.assert_not_called()  # nothing to convert

    def test_empty_from_query_carries_the_client(self):
        from unittest.mock import MagicMock
        from acquirium.Client.explore.core import Query

        client = MagicMock()
        client.sparql_query.return_value = {"columns": ["v0", "v1"], "rows": []}
        d = Query(client=client).entity("urn:t#A", alias="a").measurement(alias="m").data()
        assert d._client is client
        assert d.convert_to("mg/L").is_empty()


def _data_object(bindings, rows):
    from unittest.mock import MagicMock
    from acquirium.Client.data_object import DataObject
    from acquirium.Client.query_graph import QueryGraph

    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tall = pl.DataFrame({
        "data_alias": [r[0] for r in rows],
        "point_uri": [r[1] for r in rows],
        "ref_uri": [r[2] for r in rows],
        "time": [t] * len(rows),
        "value_numeric": [1.0] * len(rows),
        "value_text": [None] * len(rows),
    })
    client = MagicMock()
    client.compact_uri.side_effect = lambda u: str(u).rsplit("#", 1)[-1]
    return DataObject(
        _bindings=bindings,
        _entity_columns=[],
        _query_graph=QueryGraph(),
        _client=client,
        _tall=tall,
        _materialized=True,
    )


class TestPointLabelsFirst:
    def _binding(self, nid, point, ref, alias, label):
        from acquirium.Client.data_object import BindingInfo
        return BindingInfo(
            nid=nid, point_uri=point, ref_uri=ref, alias=alias,
            entity_contexts=[{}], point_label=label,
        )

    def test_auto_alias_column_named_by_label(self):
        b = self._binding(0, "urn:t#p1", "urn:t#r1", "0", "svcw__flow")
        d = _data_object([b], [("0", "urn:t#p1", "urn:t#r1")])
        assert d.dataframe(shape="wide").columns == ["time", "svcw__flow"]

    def test_multi_point_alias_disambiguated_by_label(self):
        bs = [
            self._binding(0, "urn:t#p1", "urn:t#r1", "flow", "intake"),
            self._binding(0, "urn:t#p2", "urn:t#r2", "flow", "outfall"),
        ]
        d = _data_object(bs, [("flow", "urn:t#p1", "urn:t#r1"),
                              ("flow", "urn:t#p2", "urn:t#r2")])
        assert d.dataframe(shape="wide").columns == ["time", "flow__intake", "flow__outfall"]

    def test_duplicate_labels_fall_back_to_uris(self):
        bs = [
            self._binding(0, "urn:t#p1", "urn:t#r1", "flow", "same"),
            self._binding(0, "urn:t#p2", "urn:t#r2", "flow", "same"),
        ]
        d = _data_object(bs, [("flow", "urn:t#p1", "urn:t#r1"),
                              ("flow", "urn:t#p2", "urn:t#r2")])
        assert d.dataframe(shape="wide").columns == ["time", "flow__p1", "flow__p2"]

    def test_metadata_has_point_label_column(self):
        from acquirium.Client.data_object import DataObject
        from acquirium.Client.query_graph import QueryGraph

        b = self._binding(0, "urn:t#p1", "urn:t#r1", "flow", "intake")
        d = DataObject(
            _bindings=[b], _entity_columns=[], _query_graph=QueryGraph(),
        )
        meta = d.metadata()
        assert meta.columns[:3] == ["data_alias", "point_label", "point_uri"]
        assert meta["point_label"].to_list() == ["intake"]
