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
        from acquirium.Client.explore.core import Q

        client = MagicMock()
        client.sparql_query.return_value = {"columns": ["v0", "v1"], "rows": []}
        d = Q(client=client).entity("urn:t#A", alias="a").measurement(alias="m").data()
        assert d._client is client
        assert d.convert_to("mg/L").is_empty()
