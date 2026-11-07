"""Unit tests for DataObject's pure frame-shaping helpers."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

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

    def test_streams_resolve_stream_handles_like_measurement_ref(self):
        from acquirium.Client.streams import Stream
        d = self._object()
        assert d._resolve_streams(Stream("urn:t#r2")) == {"urn:t#r2"}
        assert d._resolve_streams([Stream("urn:t#r1"), "urn:t#r1", 2]) == {"urn:t#r1", "urn:t#r3"}
        assert d._resolve_streams([]) == set()  # an empty list selects nothing

    def test_unknown_stream_raises(self):
        import pytest
        from acquirium.Client.streams import Stream
        d = self._object()
        with pytest.raises(KeyError, match="0..2"):
            d._resolve_streams([3])
        with pytest.raises(KeyError, match="stream_info"):
            d._resolve_streams(["urn:t#nope"])
        with pytest.raises(KeyError, match="stream_info"):
            d._resolve_streams(Stream("urn:t#nope"))
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

    def _data(self, *, entities=("urn:t#A", "urn:t#A")):
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from acquirium.Client.explore.core import Query

        client = MagicMock()
        # two points with one stream each, hosted by ``entities`` (one by
        # default, so by("a") yields a single sub-object)
        client.sparql_query.return_value = {
            "columns": ["v0", "v1", "ext1", "unit1"],
            "rows": [[entities[0], "urn:t#p1", self.R1, "urn:unit#m"],
                     [entities[1], "urn:t#p2", self.R2, "urn:unit#m"]],
        }
        client.timeseries_info_batch.return_value = {
            self.R1: SimpleNamespace(row_count=3, earliest=self.T0, latest=self.T1),
        }
        client.timeseries_exists_batch.return_value = {self.R1}
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
        client.timeseries_exists_batch.assert_not_called()

    def test_reading_values_does_not_fetch_stats(self):
        data, client = self._data()
        data["m"]
        data.latest()
        assert not data.is_empty()  # materialized: answered from the rows
        client.timeseries_info_batch.assert_not_called()
        client.timeseries_exists_batch.assert_not_called()

    def test_query_dataframe_does_not_fetch_stats(self):
        data, client = self._data()
        data.dataframe(shape="narrow", compact=False)
        client.timeseries_info_batch.assert_not_called()

    def test_first_stat_read_fetches_once_for_every_stream(self):
        data, client = self._data()
        assert data.total_rows == 3
        assert data.time_range == (self.T0, self.T1)
        assert not data.is_empty()  # read from the loaded statistics
        client.timeseries_info_batch.assert_called_once()
        client.timeseries_exists_batch.assert_not_called()
        (uris,), _ = client.timeseries_info_batch.call_args
        assert sorted(uris) == [self.R1, self.R2]

    def test_bindings_carry_stats(self):
        data, client = self._data()
        counts = {b.ref_uri: b.row_count for b in data.bindings}
        assert counts == {self.R1: 3, self.R2: 0}  # a stream without stats has no rows
        client.timeseries_info_batch.assert_called_once()

    def test_is_empty_asks_only_which_streams_have_rows(self):
        data, client = self._data()
        assert not data.is_empty()
        client.timeseries_exists_batch.return_value = set()
        assert not data.is_empty()  # cached
        client.timeseries_exists_batch.assert_called_once()
        client.timeseries_info_batch.assert_not_called()
        (uris,), _ = client.timeseries_exists_batch.call_args
        assert sorted(uris) == [self.R1, self.R2]

    def test_is_empty_when_no_stream_has_rows(self):
        data, client = self._data()
        client.timeseries_exists_batch.return_value = set()
        assert data.is_empty()

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

    def test_by_loop_is_empty_asks_once_for_the_whole_family(self):
        data, client = self._data(entities=("urn:t#A", "urn:t#B"))
        assert {uri: sub.is_empty() for uri, sub in data.by("a")} == {
            "urn:t#A": False, "urn:t#B": True,
        }
        assert not data.is_empty()
        client.timeseries_exists_batch.assert_called_once()
        client.timeseries_info_batch.assert_not_called()
        (uris,), _ = client.timeseries_exists_batch.call_args
        assert sorted(uris) == [self.R1, self.R2]

    def test_by_loop_fetches_stats_once_for_the_whole_family(self):
        # One request for every stream of the data() call, whichever
        # sub-object reads first; the siblings and the parent reuse it.
        data, client = self._data(entities=("urn:t#A", "urn:t#B"))
        seen = {}
        for uri, sub in data.by("a"):
            seen[uri] = (sub.total_rows, sub.time_range, sub.is_empty())
        assert seen == {
            "urn:t#A": (3, (self.T0, self.T1), False),
            "urn:t#B": (0, (None, None), True),
        }
        assert data.total_rows == 3
        client.timeseries_info_batch.assert_called_once()
        client.timeseries_exists_batch.assert_not_called()
        (uris,), _ = client.timeseries_info_batch.call_args
        assert sorted(uris) == [self.R1, self.R2]

    def test_by_does_not_fetch_until_a_statistic_is_read(self):
        data, client = self._data(entities=("urn:t#A", "urn:t#B"))
        subs = dict(data.by("a"))
        for sub in subs.values():
            sub.metadata()
            sub.stream_info()
            repr(sub)
        client.timeseries_info_batch.assert_not_called()
        client.timeseries_exists_batch.assert_not_called()

    def test_convert_to_shares_the_stats(self):
        data, client = self._data()
        client.resolve_conversion.return_value = {
            "factors": {"from_uri": "urn:unit#m", "to_uri": "urn:unit#km", "factor": 0.001}
        }
        converted = data.convert_to("urn:unit#km")
        client.timeseries_info_batch.assert_not_called()
        assert converted.total_rows == 3
        assert data.total_rows == 3
        assert [b.row_count for b in data.bindings] == [3, 0]
        client.timeseries_info_batch.assert_called_once()

    def test_refresh_stats_refetches_for_the_whole_family(self):
        from types import SimpleNamespace
        data, client = self._data()
        (_, sub), = data.by("a")
        assert sub.total_rows == 3
        client.timeseries_info_batch.return_value = {
            self.R1: SimpleNamespace(row_count=5, earliest=self.T0, latest=self.T1),
            self.R2: SimpleNamespace(row_count=1, earliest=self.T1, latest=self.T1),
        }
        assert data.total_rows == 3  # cached snapshot, no refetch
        sub.refresh_stats()
        assert data.total_rows == 6
        assert sub.total_rows == 6
        assert client.timeseries_info_batch.call_count == 2

    def test_refresh_stats_forgets_which_streams_have_rows(self):
        data, client = self._data()
        (_, sub), = data.by("a")
        assert not sub.is_empty()
        client.timeseries_exists_batch.return_value = set()
        assert not data.is_empty()  # cached
        data.refresh_stats()
        assert sub.is_empty()
        assert client.timeseries_exists_batch.call_count == 2

    def test_object_built_without_a_cache_fetches_its_own(self):
        from acquirium.Client.data_object import DataObject
        data, client = self._data()
        own = DataObject(
            _bindings=list(data._bindings[:1]),
            _entity_columns=data._entity_columns,
            _query_graph=data._query_graph,
            _client=client,
        )
        assert own.total_rows == 3
        client.timeseries_info_batch.assert_called_once()
        (uris,), _ = client.timeseries_info_batch.call_args
        assert uris == [self.R1]


class TestResample:
    """resample() records a bucketing on a lazy object and lets the server
    reduce the rows; on a fetched object it reduces the rows it holds."""

    R1 = "urn:t#ref1"
    R2 = "urn:t#ref2"
    T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _data(self, *, cast_value="float", units=("urn:unit#m", "urn:unit#m")):
        from unittest.mock import MagicMock
        from acquirium.Client.explore.core import Query

        client = MagicMock()
        client.sparql_query.return_value = {
            "columns": ["v0", "v1", "ext1", "unit1"],
            "rows": [["urn:t#A", "urn:t#p1", self.R1, units[0]],
                     ["urn:t#A", "urn:t#p2", self.R2, units[1]]],
        }
        client.compact_uri.side_effect = lambda u: str(u).rsplit("#", 1)[-1]

        def resampled(uri, **kw):
            # the server's answer: buckets 0 s and 10 s for ref1, 10 s only for ref2
            ts = [self.T0, self.T0 + timedelta(seconds=10)] if uri == self.R1 else [self.T0 + timedelta(seconds=10)]
            return pl.DataFrame({"ts": ts, "value": [1.0] * len(ts), "uri": [uri] * len(ts)})

        client.timeseries_resampled_df.side_effect = resampled

        def raw(uri, **kw):
            # the raw readings: ref1 at 0, 4, 8, 12 s; ref2 has none
            if uri != self.R1:
                return pl.DataFrame()
            return pl.DataFrame(
                {"ts": [self.T0 + timedelta(seconds=s) for s in (0, 4, 8, 12)], "value": [1.0, 2.0, 3.0, 4.0], "uri": [uri] * 4}
            )

        client.timeseries_df.side_effect = raw
        data = Query(client=client).entity("urn:t#A", alias="a").measurement(alias="m").data(cast_value=cast_value)
        return data, client

    def test_lazy_until_the_values_are_needed(self):
        from datetime import timedelta
        from acquirium.internals.resampling import EPOCH

        data, client = self._data()
        r = data.resample("10s", agg="max")
        assert r.resampling.every == timedelta(seconds=10)
        assert r.resampling.agg == "max"
        assert "resampled every=10s agg=max" in repr(r)
        client.timeseries_resampled_df.assert_not_called()
        client.timeseries_df.assert_not_called()
        assert data.resampling is None  # the original is untouched

        wide = r.dataframe(compact=False)
        assert wide.columns == ["time", "m__p1", "m__p2"]
        assert wide.height == 2
        client.timeseries_df.assert_not_called()
        assert client.timeseries_resampled_df.call_count == 2
        _, kw = client.timeseries_resampled_df.call_args
        assert kw["every"] == timedelta(seconds=10)
        assert kw["agg"] == "max"
        assert kw["origin"] == EPOCH
        assert kw["limit"] is None and kw["order"] == "asc"

    def test_center_label_shifts_the_grid_and_the_labels(self):
        from acquirium.internals.resampling import EPOCH

        data, client = self._data()
        r = data.resample("10s", label="center")
        times = r["m"].filter(pl.col("point_uri") == "urn:t#p1")["time"].to_list()
        _, kw = client.timeseries_resampled_df.call_args
        assert kw["origin"] == EPOCH - timedelta(seconds=5)
        assert times == [self.T0 + timedelta(seconds=5), self.T0 + timedelta(seconds=15)]

    def test_origin_is_passed_through(self):
        data, client = self._data()
        origin = datetime(2026, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
        data.resample("10s", origin=origin).dataframe()
        _, kw = client.timeseries_resampled_df.call_args
        assert kw["origin"] == origin
        data.resample("10s", origin="2026-01-01T00:00:03").dataframe()
        _, kw = client.timeseries_resampled_df.call_args
        assert kw["origin"] == origin

    def test_window_and_limit_of_the_data_call_are_forwarded(self):
        from unittest.mock import MagicMock
        from acquirium.Client.explore.core import Query

        data, client = self._data()
        q = Query(client=client).entity("urn:t#A", alias="a").measurement(alias="m")
        q.data(start=self.T0, end="2026-01-02", limit=3, order="desc").resample("1h").dataframe()
        _, kw = client.timeseries_resampled_df.call_args
        assert (kw["start"], kw["end"], kw["limit"], kw["order"]) == (self.T0, "2026-01-02", 3, "desc")

    def test_latest_asks_for_the_newest_bucket(self):
        data, client = self._data()
        data.resample("10s").latest()
        _, kw = client.timeseries_resampled_df.call_args
        assert (kw["limit"], kw["order"]) == (1, "desc")

    def test_conversion_applies_to_the_reduced_values(self):
        data, client = self._data()
        client.resolve_conversion.return_value = {"factors": {
            "from_uri": "urn:unit#m", "to_uri": "urn:unit#cm", "compatible": True,
            "from_multiplier": 1.0, "from_offset": 0.0, "to_multiplier": 0.01, "to_offset": 0.0,
        }}
        client.get_conversion_factors.return_value = client.resolve_conversion.return_value["factors"]
        r = data.convert_to("cm").resample("10s")
        assert r["m"]["value"].to_list() == [100.0, 100.0, 100.0]
        assert r.units() == {"m": ["urn:unit#cm"]}

    def test_count_has_no_unit_and_is_not_converted(self):
        data, client = self._data()
        client.resolve_conversion.return_value = {"factors": {
            "from_uri": "urn:unit#m", "to_uri": "urn:unit#cm", "compatible": True,
            "from_multiplier": 1.0, "from_offset": 0.0, "to_multiplier": 0.01, "to_offset": 0.0,
        }}
        client.get_conversion_factors.return_value = client.resolve_conversion.return_value["factors"]
        r = data.convert_to("cm").resample("10s", agg="count")
        assert r.units() == {"m": [None]}
        assert r.metadata()["unit"].to_list() == [None, None]
        assert r["m"]["value"].to_list() == [1.0, 1.0, 1.0]
        client.get_conversion_factors.assert_not_called()

    def test_errors(self):
        data, _ = self._data()
        with pytest.raises(ValueError, match="agg"):
            data.resample("10s", agg="mode")
        with pytest.raises(ValueError, match="label"):
            data.resample("10s", label="right")
        with pytest.raises(ValueError, match="every"):
            data.resample("1mo")
        with pytest.raises(ValueError, match="already resampled"):
            data.resample("10s").resample("1m")
        with pytest.raises(ValueError, match="already resampled"):
            data.resample("10s").align("1m")
        text, _ = self._data(cast_value="str")
        with pytest.raises(ValueError, match="cast_value='str'"):
            text.resample("10s")

    def test_by_sub_objects_carry_the_bucketing(self):
        data, client = self._data()
        r = data.resample("10s", agg="min")
        (_, sub), = r.by("a")
        assert sub.resampling == r.resampling
        sub.dataframe()
        _, kw = client.timeseries_resampled_df.call_args
        assert kw["agg"] == "min"

    def test_fetched_rows_are_reduced_in_polars(self):
        data, client = self._data()
        data["m"]  # fetch the raw readings
        client.timeseries_resampled_df.assert_not_called()
        r = data.resample("10s", agg="mean")
        client.timeseries_resampled_df.assert_not_called()
        assert r.resampling.agg == "mean"
        assert "resampled every=10s agg=mean" in repr(r)
        df = r["m"]
        # ref1: 1,2,3 at 0,4,8 s -> bucket 0 s; 4 at 12 s -> bucket 10 s
        assert df["time"].to_list() == [self.T0, self.T0 + timedelta(seconds=10)]
        assert df["value"].to_list() == [2.0, 4.0]
        assert data["m"]["value"].to_list() == [1.0, 2.0, 3.0, 4.0]  # the original keeps its rows

    @pytest.mark.parametrize("agg, expected", [
        ("mean", [2.0, 4.0]), ("min", [1.0, 4.0]), ("max", [3.0, 4.0]), ("sum", [6.0, 4.0]),
        ("count", [3.0, 1.0]), ("first", [1.0, 4.0]), ("last", [3.0, 4.0]), ("median", [2.0, 4.0]),
    ])
    def test_every_aggregate_on_fetched_rows(self, agg, expected):
        data, _ = self._data()
        data["m"]
        assert data.resample("10s", agg=agg)["m"]["value"].to_list() == expected

    def test_center_label_on_fetched_rows(self):
        data, _ = self._data()
        data["m"]
        df = data.resample("10s", label="center")["m"]
        # windows [-5, 5) -> 1, 2 ; [5, 15) -> 3, 4
        assert df["time"].to_list() == [self.T0, self.T0 + timedelta(seconds=10)]
        assert df["value"].to_list() == [1.5, 3.5]

    def test_resampled_empty_object_stays_empty(self):
        from unittest.mock import MagicMock
        from acquirium.Client.data_object import DataObject
        from acquirium.Client.query_graph import QueryGraph

        empty = DataObject._empty(QueryGraph(), client=MagicMock())
        assert empty.resample("10s").is_empty()
        assert empty.align("10s", fill=0).is_empty()


class TestAlign:
    """align() puts every stream on the shared bucket grid and fills the gaps."""

    T0 = TestResample.T0

    def _fetched(self, ref1_rows, ref2_rows):
        from acquirium.Client.data_object import BindingInfo, DataObject
        from acquirium.Client.query_graph import QueryGraph
        from unittest.mock import MagicMock

        rows = [("m", "urn:t#p1", TestResample.R1, t, v) for t, v in ref1_rows] + \
               [("m", "urn:t#p2", TestResample.R2, t, v) for t, v in ref2_rows]
        tall = pl.DataFrame({
            "data_alias": [r[0] for r in rows],
            "point_uri": [r[1] for r in rows],
            "ref_uri": [r[2] for r in rows],
            "time": [self.T0 + timedelta(seconds=r[3]) for r in rows],
            "value_numeric": [float(r[4]) for r in rows],
            "value_text": [None] * len(rows),
        }).with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
        client = MagicMock()
        client.compact_uri.side_effect = lambda u: str(u).rsplit("#", 1)[-1]
        bindings = [
            BindingInfo(nid=1, point_uri="urn:t#p1", ref_uri=TestResample.R1, alias="m", entity_contexts=[{}]),
            BindingInfo(nid=1, point_uri="urn:t#p2", ref_uri=TestResample.R2, alias="m", entity_contexts=[{}]),
        ]
        return DataObject(_bindings=bindings, _entity_columns=[], _query_graph=QueryGraph(),
                          _client=client, _tall=tall, _materialized=True)

    def _wide(self, data, **kw):
        return data.align(**kw).dataframe(compact=False)

    def test_every_stream_gets_every_bucket(self):
        d = self._fetched([(0, 1), (10, 2), (20, 3)], [(21, 30)])
        wide = self._wide(d, every="10s")
        assert wide["time"].to_list() == [self.T0 + timedelta(seconds=s) for s in (0, 10, 20)]
        assert wide["m__p1"].to_list() == [1.0, 2.0, 3.0]
        assert wide["m__p2"].to_list() == [None, None, 30.0]
        assert d.align("10s")["m"].filter(pl.col("point_uri") == "urn:t#p2").height == 3

    def test_grid_spans_the_whole_object(self):
        # p2 ends before p1 begins; both get the full grid
        d = self._fetched([(40, 1), (50, 2)], [(0, 30)])
        assert self._wide(d, every="10s").height == 6

    def test_fills(self):
        d = self._fetched([(0, 1), (30, 4)], [(10, 20), (20, 40), (40, 10)])
        assert self._wide(d, every="10s")["m__p1"].to_list() == [1.0, None, None, 4.0, None]
        assert self._wide(d, every="10s", fill="ffill")["m__p1"].to_list() == [1.0, 1.0, 1.0, 4.0, 4.0]
        assert self._wide(d, every="10s", fill="interpolate")["m__p1"].to_list() == [1.0, 2.0, 3.0, 4.0, None]
        assert self._wide(d, every="10s", fill="interpolate")["m__p2"].to_list() == [None, 20.0, 40.0, 25.0, 10.0]
        assert self._wide(d, every="10s", fill=0)["m__p1"].to_list() == [1.0, 0.0, 0.0, 4.0, 0.0]
        assert self._wide(d, every="10s", fill=-1.5)["m__p2"].to_list() == [-1.5, 20.0, 40.0, -1.5, 10.0]

    def test_reduction_before_the_fill(self):
        d = self._fetched([(0, 1), (2, 3), (20, 5)], [])
        assert self._wide(d, every="10s", agg="max", fill="interpolate")["m__p1"].to_list() == [3.0, 4.0, 5.0]

    def test_bad_fill(self):
        d = self._fetched([(0, 1)], [])
        for bad in ("bfill", True, "0"):
            with pytest.raises(ValueError, match="fill"):
                d.align("10s", fill=bad)

    def test_every_inferred_from_fetched_rows(self):
        d = self._fetched([(0, 1), (9, 2), (18, 3), (27, 4)], [(0, 1), (60, 2)])
        a = d.align()
        assert a.resampling.every == timedelta(seconds=10)  # 27 s over 3 gaps = 9 s -> 10 s
        assert a.resampling.align is True

    def test_every_inferred_from_stream_statistics_on_a_lazy_object(self):
        from types import SimpleNamespace
        data, client = TestResample()._data()
        client.timeseries_info_batch.return_value = {
            TestResample.R1: SimpleNamespace(row_count=61, earliest=self.T0, latest=self.T0 + timedelta(minutes=1)),
            TestResample.R2: SimpleNamespace(row_count=3, earliest=self.T0, latest=self.T0 + timedelta(minutes=1)),
        }
        a = data.align(fill="ffill")
        assert a.resampling.every == timedelta(seconds=1)
        client.timeseries_resampled_df.assert_not_called()  # inference needs no rows
        client.timeseries_info_batch.assert_called_once()

    def test_every_cannot_be_inferred_without_two_readings(self):
        d = self._fetched([(0, 1)], [(5, 2)])
        with pytest.raises(ValueError, match="pass every="):
            d.align()

    def test_lazy_align_fills_after_the_fetch(self):
        data, client = TestResample()._data()
        a = data.align("10s", fill="ffill")
        client.timeseries_resampled_df.assert_not_called()
        wide = a.dataframe(compact=False)
        # the server returns buckets 0 s, 10 s for ref1 and 10 s for ref2
        assert wide["m__p2"].to_list() == [None, 1.0]
        assert wide.height == 2
        assert "aligned every=10s agg=mean fill='ffill'" in repr(a)

    def test_window_filters_the_grid(self):
        d = self._fetched([(0, 1), (30, 4)], [])
        wide = d.align("10s", fill="interpolate").dataframe(
            compact=False, start=self.T0 + timedelta(seconds=10), end=self.T0 + timedelta(seconds=20)
        )
        assert wide["m__p1"].to_list() == [2.0, 3.0]
