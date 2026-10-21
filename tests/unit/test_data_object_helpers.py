"""Unit tests for DataObject's pure frame-shaping helpers."""

import warnings
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from acquirium.Client.data_object import _apply_downsample, _pivot_split_values


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


class TestApplyDownsample:
    ORIGIN = datetime(2026, 1, 1, tzinfo=timezone.utc)
    EVERY = timedelta(hours=1)
    # first bucket: unimodal (2.0) once nulls are ignored; second bucket: bimodal (1.0, 3.0)
    VALUES = [1.0, 2.0, 2.0, None, None, None, 1.0, 1.0, 3.0, 3.0, 4.0, 9.0]

    def _series(self, values):
        times = [self.ORIGIN + i * timedelta(minutes=10) for i in range(len(values))]
        return pl.DataFrame(
            {"time": times, "value": values},
            schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64},
        )

    def test_median(self):
        out = _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, "median")
        assert out["time"].to_list() == [self.ORIGIN, self.ORIGIN + self.EVERY]
        assert out["value"].to_list() == [2.0, 3.0]

    def test_multimodal_mode_warns_and_takes_median_of_modes(self):
        with pytest.warns(UserWarning, match=r"1 of 2 buckets in 'flow' were multimodal"):
            out = _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, "mode", label="flow")
        assert out.columns == ["time", "value"]
        assert out["value"].to_list() == [2.0, 2.0]

    def test_unimodal_mode_does_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            out = _apply_downsample(self._series(self.VALUES[:6]), self.ORIGIN, self.EVERY, "mode")
        assert out["value"].to_list() == [2.0]


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

    def test_query_dataframe_interface_matches_data_object(self):
        """The shared parameters must stay in lockstep so
        ``q.dataframe(...)`` always equals ``q.data(...).dataframe(...)``."""
        import inspect
        from acquirium.Client.data_object import DataObject
        from acquirium.Client.explore.core import Query

        q = inspect.signature(Query.dataframe).parameters
        d = inspect.signature(DataObject.dataframe).parameters
        for name in ("shape", "start", "end", "limit", "order", "include_ref", "compact"):
            assert name in q and name in d, name
            assert q[name].default == d[name].default, name
        # shape is the one positional display parameter on both
        assert list(q)[1] == list(d)[1] == "shape"

    def test_dataframe_window_filters_client_side(self):
        from acquirium.Client.data_object import BindingInfo
        from unittest.mock import MagicMock
        from acquirium.Client.data_object import DataObject
        from acquirium.Client.query_graph import QueryGraph

        t = [datetime(2026, 1, d, tzinfo=timezone.utc) for d in (1, 2, 3)]
        tall = pl.DataFrame({
            "data_alias": ["flow"] * 3,
            "point_uri": ["urn:t#p1"] * 3,
            "ref_uri": ["urn:t#r1"] * 3,
            "time": t,
            "value_numeric": [1.0, 2.0, 3.0],
            "value_text": [None] * 3,
        })
        client = MagicMock()
        client.compact_uri.side_effect = lambda u: str(u).rsplit("#", 1)[-1]
        d = DataObject(
            _bindings=[BindingInfo(nid=0, point_uri="urn:t#p1", ref_uri="urn:t#r1",
                                   alias="flow", entity_contexts=[{}])],
            _entity_columns=[], _query_graph=QueryGraph(),
            _client=client, _tall=tall, _materialized=True,
        )

        assert d.dataframe("wide", start=t[1])["flow"].to_list() == [2.0, 3.0]
        assert d.dataframe("wide", end=t[1])["flow"].to_list() == [1.0, 2.0]
        assert d.dataframe("wide", limit=2, order="desc")["flow"].to_list() == [3.0, 2.0]
        assert d.dataframe("narrow", start="2026-01-02", order="desc")["value_numeric"].to_list() == [3.0, 2.0]

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
