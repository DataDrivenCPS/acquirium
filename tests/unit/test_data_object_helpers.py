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


class TestCastValue:
    def _object(self, values, cast_value):
        from unittest.mock import MagicMock
        from acquirium.Client.data_object import BindingInfo, DataObject
        from acquirium.Client.query_graph import QueryGraph

        t = [datetime(2026, 1, d, tzinfo=timezone.utc) for d in range(1, len(values) + 1)]
        tall = pl.DataFrame({
            "data_alias": ["flow"] * len(values),
            "point_uri": ["urn:t#p1"] * len(values),
            "ref_uri": ["urn:t#r1"] * len(values),
            "time": t,
            "value_numeric": pl.Series(values, dtype=pl.Float64),
            "value_text": [None] * len(values),
        })
        client = MagicMock()
        client.compact_uri.side_effect = lambda u: str(u).rsplit("#", 1)[-1]
        return DataObject(
            _bindings=[BindingInfo(nid=0, point_uri="urn:t#p1", ref_uri="urn:t#r1",
                                   alias="flow", entity_contexts=[{}])],
            _entity_columns=[], _query_graph=QueryGraph(), _client=client,
            _tall=tall, _materialized=True, _query_params={"cast_value": cast_value},
        )

    def test_unknown_cast_value_is_rejected(self):
        import pytest
        from acquirium.Client.data_object import _validate_cast_value
        for ok in ("float", "int", "str", None):
            assert _validate_cast_value(ok) == ok
        with pytest.raises(ValueError, match="cast_value must be"):
            _validate_cast_value("double")

    def test_str_sends_everything_to_text(self):
        from acquirium.Client.data_object import _split_value_column
        out = _split_value_column(pl.DataFrame({"value": [1.5, 2.0]}), "str")
        assert out["value_numeric"].to_list() == [None, None]
        assert out["value_text"].to_list() == ["1.5", "2.0"]

    def test_float_parses_numbers_out_of_text(self):
        from acquirium.Client.data_object import _split_value_column
        df = pl.DataFrame({"value": ["12.5", "ON", None, " 3 "]})
        out = _split_value_column(df, "float")
        assert out["value_numeric"].to_list() == [12.5, None, None, 3.0]
        assert out["value_text"].to_list() == [None, "ON", None, None]

    def test_no_cast_keeps_text_as_text(self):
        from acquirium.Client.data_object import _split_value_column
        out = _split_value_column(pl.DataFrame({"value": ["12.5", "ON"]}), None)
        assert out["value_numeric"].to_list() == [None, None]
        assert out["value_text"].to_list() == ["12.5", "ON"]

    def test_int_returns_int64_columns(self):
        d = self._object([1.0, 2.0], "int")
        assert d["flow"]["value"].dtype == pl.Int64
        assert d.dataframe("wide")["flow"].to_list() == [1, 2]
        assert d.dataframe("narrow")["value_numeric"].dtype == pl.Int64
        assert d.dataframe("narrow", compact=False)["value_numeric"].dtype == pl.Int64

    def test_int_truncates_fractional_values_with_a_warning(self):
        import pytest
        d = self._object([1.0, 1.5], "int")
        with pytest.warns(UserWarning, match="truncates 1 non-integer value.*such as 1.5"):
            assert d.dataframe("wide")["flow"].to_list() == [1, 1]
        with pytest.warns(UserWarning, match="cast_value='int'"):
            assert d["flow"]["value"].to_list() == [1, 1]

    def test_float_leaves_values_alone(self):
        d = self._object([1.0, 1.5], "float")
        assert d["flow"]["value"].to_list() == [1.0, 1.5]
        assert d["flow"]["value"].dtype == pl.Float64


class TestStreamInfo:
    def _object(self):
        from acquirium.Client.data_object import BindingInfo, DataObject
        from acquirium.Client.query_graph import QueryGraph
        bs = [
            BindingInfo(nid=0, point_uri="urn:t#p1", ref_uri="urn:t#r1", alias="flow",
                        entity_contexts=[{}], source_id="scada", ref_name="FT-101"),
            BindingInfo(nid=0, point_uri="urn:t#p1", ref_uri="urn:t#r2", alias="flow",
                        entity_contexts=[{}], source_id="lab", ref_name="flow_csv"),
            BindingInfo(nid=1, point_uri="urn:t#p2", ref_uri="urn:t#r3", alias="temp",
                        entity_contexts=[{}]),
        ]
        return DataObject(_bindings=bs, _entity_columns=[], _query_graph=QueryGraph())

    def test_one_row_per_stream_with_driver_names(self):
        info = self._object().stream_info()
        assert info.columns == ["stream", "alias", "point_uri", "ref_uri", "source_id", "ref_name"]
        assert info["stream"].to_list() == [0, 1, 2]
        assert info["source_id"].to_list() == ["scada", "lab", None]
        assert info["ref_name"].to_list() == ["FT-101", "flow_csv", None]

    def test_alias_keeps_the_numbering(self):
        d = self._object()
        assert d.stream_info("temp")["stream"].to_list() == [2]
        import pytest
        with pytest.raises(KeyError):
            d.stream_info("nope")

    def test_metadata_with_refs_shows_identity(self):
        meta = self._object().metadata(include_ref_uris=True)
        assert meta.filter(meta["ref_uri"] == "urn:t#r1")["ref_name"].to_list() == ["FT-101"]

    def test_streams_resolve_by_number_or_ref_uri(self):
        d = self._object()
        assert d._resolve_streams(None) is None
        assert d._resolve_streams(1) == {"urn:t#r2"}
        assert d._resolve_streams("urn:t#r3") == {"urn:t#r3"}
        assert d._resolve_streams([0, "urn:t#r2"]) == {"urn:t#r1", "urn:t#r2"}

    def test_unknown_stream_raises(self):
        import pytest
        d = self._object()
        with pytest.raises(KeyError, match="0..2"):
            d._resolve_streams([3])
        with pytest.raises(KeyError, match="stream_info"):
            d._resolve_streams(["urn:t#nope"])
        with pytest.raises(TypeError):
            d._resolve_streams([1.5])

    def test_dataframe_streams_filters_the_tall_frame(self):
        import polars as pl
        from datetime import datetime, timezone
        d = self._object()
        t = datetime(2026, 1, 1, tzinfo=timezone.utc)
        d._tall = pl.DataFrame({
            "data_alias": ["flow", "flow", "temp"],
            "point_uri": ["urn:t#p1", "urn:t#p1", "urn:t#p2"],
            "ref_uri": ["urn:t#r1", "urn:t#r2", "urn:t#r3"],
            "time": [t, t, t],
            "value_numeric": [1.0, 1.1, 20.0],
            "value_text": pl.Series([None, None, None], dtype=pl.Utf8),
        })
        d._materialized = True
        assert d.dataframe(compact=False)["flow"].to_list() == [1.0]       # first stream wins
        assert d.dataframe(compact=False, streams=[1])["flow"].to_list() == [1.1]
        narrow = d.dataframe(shape="narrow", compact=False, streams=["urn:t#r2", 2])
        assert sorted(narrow["ref_uri"].to_list()) == ["urn:t#r2", "urn:t#r3"]


class TestLazyStats:
    """Stream statistics are fetched on the first stat read, not by data()."""

    R1 = "urn:t#ref1"
    R2 = "urn:t#ref2"
    T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    T1 = datetime(2026, 1, 2, tzinfo=timezone.utc)

    def _data(self):
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from acquirium.Client.explore.core import Query

        client = MagicMock()
        # one entity hosting two points, one stream each
        client.sparql_query.return_value = {
            "columns": ["v0", "v1", "ext1"],
            "rows": [["urn:t#A", "urn:t#p1", self.R1], ["urn:t#A", "urn:t#p2", self.R2]],
        }
        client.timeseries_info_batch.return_value = {
            self.R1: SimpleNamespace(row_count=3, earliest=self.T0, latest=self.T1),
        }
        client.timeseries_df.return_value = pl.DataFrame(
            {"ts": [self.T0], "value": [1.0], "uri": [self.R1]}
        )
        data = Query(client=client).entity("urn:t#A", alias="a").measurement(alias="m").data()
        return data, client

    def test_data_does_not_fetch_stats(self):
        data, client = self._data()
        assert data.aliases == ["m"]
        data.metadata()
        data.stream_info()
        client.timeseries_info_batch.assert_not_called()

    def test_reading_values_does_not_fetch_stats(self):
        data, client = self._data()
        data["m"]
        data.latest()
        assert not data.is_empty()  # materialized: answered from the rows
        client.timeseries_info_batch.assert_not_called()

    def test_query_dataframe_does_not_fetch_stats(self):
        data, client = self._data()
        data.dataframe(shape="narrow", compact=False)
        client.timeseries_info_batch.assert_not_called()

    def test_first_stat_read_fetches_once_for_every_stream(self):
        data, client = self._data()
        assert data.total_rows == 3
        assert data.time_range == (self.T0, self.T1)
        assert not data.is_empty()
        client.timeseries_info_batch.assert_called_once()
        (uris,), _ = client.timeseries_info_batch.call_args
        assert sorted(uris) == [self.R1, self.R2]

    def test_bindings_carry_stats(self):
        data, client = self._data()
        counts = {b.ref_uri: b.row_count for b in data.bindings}
        assert counts == {self.R1: 3, self.R2: 0}  # a stream without stats has no rows
        client.timeseries_info_batch.assert_called_once()

    def test_is_empty_before_a_fetch_reads_stats(self):
        data, client = self._data()
        client.timeseries_info_batch.return_value = {}
        assert data.is_empty()
        client.timeseries_info_batch.assert_called_once()

    def test_lazy_repr_does_not_fetch(self):
        data, client = self._data()
        assert "2 streams" in repr(data)
        client.timeseries_info_batch.assert_not_called()
        data.total_rows
        assert "~3 rows" in repr(data)

    def test_sub_objects_reuse_loaded_stats(self):
        data, client = self._data()
        data.total_rows
        (_, sub), = data.by("a")
        assert sub.total_rows == 3
        client.timeseries_info_batch.assert_called_once()

    def test_sub_objects_fetch_their_own_stats(self):
        data, client = self._data()
        (_, sub), = data.by("a")
        client.timeseries_info_batch.assert_not_called()
        assert sub.total_rows == 3
        client.timeseries_info_batch.assert_called_once()
