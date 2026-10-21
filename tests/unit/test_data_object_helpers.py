"""Unit tests for DataObject's pure frame-shaping helpers."""

import warnings
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from acquirium.Client.data_object import _apply_downsample, _pivot_split_values, _snap_to_grid


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


def _frame(times, values):
    return pl.DataFrame(
        {"time": times, "value": values},
        schema={"time": pl.Datetime(time_zone="UTC"), "value": pl.Float64},
    )


class TestApplyDownsample:
    ORIGIN = datetime(2026, 1, 1, tzinfo=timezone.utc)
    EVERY = timedelta(hours=1)
    # Measurements every 10 min from ORIGIN - 25 min (clear of window boundaries), so the
    # first six fall in ORIGIN's [-30 min, +30 min) window and the next six in the following one.
    # First window: unimodal (2.0) once nulls are ignored; second window: bimodal (1.0, 3.0).
    VALUES = [1.0, 2.0, 2.0, None, None, None, 1.0, 1.0, 3.0, 3.0, 4.0, 9.0]

    def _series(self, values):
        start = self.ORIGIN - timedelta(minutes=25)
        return _frame([start + i * timedelta(minutes=10) for i in range(len(values))], values)

    def test_measurements_on_window_boundary_warn(self):
        # a 1-min ramp sampled on whole minutes: 00:30 is in the 01:00 window, 01:30 is not
        times = [self.ORIGIN + timedelta(minutes=m) for m in range(30, 91)]
        with pytest.warns(UserWarning, match=r"2 measurement\(s\) in 'ramp' fall exactly on a window boundary"):
            out = _apply_downsample(
                _frame(times, [float(m) for m in range(30, 91)]), self.ORIGIN, self.EVERY, "mean", label="ramp",
            )
        assert out["value"].to_list() == [59.5, 90.0]

    def test_measurements_off_window_boundary_do_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, "median")

    def test_median(self):
        out = _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, "median")
        assert out["time"].to_list() == [self.ORIGIN, self.ORIGIN + self.EVERY]
        assert out["value"].to_list() == [2.0, 3.0]

    def test_window_is_centered_and_half_open(self):
        minutes = [-30, 29, 30, 89]
        times = [self.ORIGIN + timedelta(minutes=m) for m in minutes]
        snapped = _snap_to_grid(_frame(times, [0.0] * 4), self.ORIGIN, self.EVERY)
        assert snapped["time"].to_list() == [self.ORIGIN, self.ORIGIN, self.ORIGIN + self.EVERY, self.ORIGIN + self.EVERY]
        assert snapped["offset_us"].to_list() == [m * 60_000_000 for m in (-30, 29, -30, 29)]

    @pytest.mark.parametrize("method", ["ignore_intermediate", "closest"])
    def test_ignore_intermediate_keeps_closest_and_earlier_on_tie(self, method):
        minutes = [-20, 5, 25,  # closest to ORIGIN is +5
                   50, 70]      # tie around ORIGIN + 1h: the earlier (+50) wins
        times = [self.ORIGIN + timedelta(minutes=m) for m in minutes]
        out = _apply_downsample(_frame(times, [1.0, 2.0, 3.0, 4.0, 5.0]), self.ORIGIN, self.EVERY, method)
        assert out["value"].to_list() == [2.0, 4.0]

    def test_single_measurement_per_grid_time_needs_no_method(self):
        times = [self.ORIGIN + timedelta(minutes=7), self.ORIGIN + timedelta(hours=1, minutes=7)]
        out = _apply_downsample(_frame(times, [1.0, 2.0]), self.ORIGIN, self.EVERY, None)
        assert out["time"].to_list() == [self.ORIGIN, self.ORIGIN + self.EVERY]
        assert out["value"].to_list() == [1.0, 2.0]

    def test_several_measurements_without_method_raises(self):
        with pytest.raises(ValueError, match="downsample=None"):
            _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, None)

    def test_multimodal_mode_warns_and_takes_median_of_modes(self):
        with pytest.warns(UserWarning, match=r"1 of 2 grid times in 'flow' were multimodal"):
            out = _apply_downsample(self._series(self.VALUES), self.ORIGIN, self.EVERY, "mode", label="flow")
        assert out.columns == ["time", "value"]
        assert out["value"].to_list() == [2.0, 2.0]

    def test_unimodal_mode_does_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            out = _apply_downsample(self._series(self.VALUES[:6]), self.ORIGIN, self.EVERY, "mode")
        assert out["value"].to_list() == [2.0]


def _series_data_object(series):
    """A materialized DataObject with one single-point alias per ``series`` entry
    (``{alias: (times, values)}``)."""
    from unittest.mock import MagicMock
    from acquirium.Client.data_object import BindingInfo, DataObject
    from acquirium.Client.query_graph import QueryGraph

    bindings, frames = [], []
    for nid, (alias, (times, values)) in enumerate(series.items()):
        uri = f"urn:t#{alias}"
        bindings.append(BindingInfo(nid=nid, point_uri=uri, ref_uri=uri, alias=alias, entity_contexts=[{}]))
        frames.append(_frame(times, values).rename({"value": "value_numeric"}).with_columns(
            data_alias=pl.lit(alias), point_uri=pl.lit(uri), ref_uri=pl.lit(uri),
            value_text=pl.lit(None, dtype=pl.Utf8),
        ))
    tall = pl.concat(frames).select("data_alias", "point_uri", "ref_uri", "time", "value_numeric", "value_text")
    return DataObject(
        _bindings=bindings, _entity_columns=[], _query_graph=QueryGraph(),
        _client=MagicMock(), _tall=tall, _materialized=True,
    )


class TestReconcileGridAlignment:
    T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _hourly_at_7_past(self, n=24):
        return [self.T0 + timedelta(hours=i, minutes=7) for i in range(n)], [float(i) for i in range(n)]

    def test_off_grid_measurements_at_same_resolution_are_kept(self):
        times, values = self._hourly_at_7_past()
        df = _series_data_object({"a": (times, values)}).reconcile(resolution="1h")
        assert df["time"].to_list() == [self.T0 + timedelta(hours=i) for i in range(24)]
        assert df["a_reconciled"].to_list() == values

    def test_off_grid_measurements_at_default_resolution_are_kept(self):
        times, values = self._hourly_at_7_past()
        df = _series_data_object({"a": (times, values)}).reconcile()
        assert df["a_reconciled"].to_list() == values

    @pytest.mark.parametrize(("method", "kwargs", "fill"), [
        ("zero", {}, 0.0),
        ("default_value", {"fill_value": -1.0}, -1.0),
        ("null", {}, None),
    ])
    def test_constant_fills_keep_off_grid_measurements(self, method, kwargs, fill):
        times, values = self._hourly_at_7_past(n=3)
        df = _series_data_object({"a": (times, values)}).reconcile(resolution="10m", upsample=method, **kwargs)
        # 10:07 is nearest the 10:10 grid time
        nearest = {t + timedelta(minutes=3): v for t, v in zip(times, values)}
        for t, v in zip(df["time"].to_list(), df["a_reconciled"].to_list()):
            assert v == nearest.get(t, fill)

    def test_jittered_measurements_at_explicit_resolution_need_no_method(self):
        jitter_s = [0, 1, 1, 2, -1, 0]
        times = [self.T0 + timedelta(hours=i, seconds=j) for i, j in enumerate(jitter_s)]
        values = [float(i) for i in range(len(times))]
        df = _series_data_object({"a": (times, values)}).reconcile(resolution="1h")
        assert df["a_reconciled"].to_list() == values

    def test_shorter_span_requires_upsample(self):
        a = ([self.T0 + timedelta(hours=h) for h in range(3)], [0.0, 1.0, 2.0])
        b = ([self.T0 + timedelta(hours=h) for h in range(5)], [0.0, 1.0, 2.0, 3.0, 4.0])
        with pytest.raises(ValueError, match="'a' has no measurement at 2 of 5 grid times"):
            _series_data_object({"a": a, "b": b}).reconcile(resolution="1h")
        df = _series_data_object({"a": a, "b": b}).reconcile(resolution="1h", upsample="null")
        assert df["a_reconciled"].to_list() == [0.0, 1.0, 2.0, None, None]

    def test_downsampled_values_kept_and_gap_filled_from_true_timestamps(self):
        """A fine series with a gap: grid times with measurements keep their
        downsampled value, and the empty one is interpolated from the
        neighboring true measurements."""
        minutes = [-10, 10, 50, 70, 230, 250]  # nothing near 02:00 or 03:00
        times = [self.T0 + timedelta(minutes=m) for m in minutes]
        df = _series_data_object({"a": (times, [float(m) for m in minutes])}).reconcile(
            resolution="1h", downsample="mean", upsample="interpolate",
        )
        assert df["a_reconciled"].to_list() == pytest.approx([0.0, 60.0, 120.0, 180.0, 240.0])

    def test_interpolation_uses_true_timestamps_not_snapped_ones(self):
        """10:07 lands in the 10:10 window, but 10:10 must still be interpolated
        from the true 10:07 and 11:07 timestamps, not take the 10:07 value."""
        t0 = self.T0 + timedelta(hours=10, minutes=7)
        df = _series_data_object({"a": ([t0, t0 + timedelta(hours=1)], [0.0, 60.0])}).reconcile(
            resolution="10m", upsample="interpolate",
        )
        inside = df.filter(pl.col("time") <= t0 + timedelta(hours=1))
        expected = [(t - t0).total_seconds() / 60 for t in inside["time"].to_list()]
        assert inside["time"][0] == self.T0 + timedelta(hours=10, minutes=10)
        assert inside["a_reconciled"].to_list() == pytest.approx(expected)

    def test_gap_is_interpolated_from_original_data_not_averages(self):
        # 01:00's window averages 00:55 and 01:05 to 50; 02:00 is empty
        times = [self.T0 + timedelta(minutes=m) for m in (55, 65, 180)]
        df = _series_data_object({"a": (times, [0.0, 100.0, 300.0])}).reconcile(
            resolution="1h", downsample="mean", upsample="interpolate",
        )
        # 02:00 interpolates 01:05 (100) -> 03:00 (300), not 01:00 (50) -> 03:00 (300)
        assert df["a_reconciled"].to_list() == pytest.approx([50.0, 100 + 200 * 55 / 115, 300.0])

    def test_interpolation_does_not_depend_on_whether_point_has_gaps(self):
        times = [self.T0 + timedelta(hours=h, minutes=7) for h in range(6)]
        minutes = [(t - times[0]).total_seconds() / 60 for t in times]
        on_grid = ([self.T0 + timedelta(hours=h) for h in range(7)], [0.0] * 7)

        def reconciled(keep):
            a = ([times[i] for i in keep], [minutes[i] for i in keep])
            df = _series_data_object({"a": a, "b": on_grid}).reconcile(resolution="1h", upsample="interpolate")
            return df.filter(pl.col("time") != self.T0 + timedelta(hours=3))["a_reconciled"].to_list()

        assert reconciled([0, 1, 2, 3, 4, 5]) == pytest.approx(reconciled([0, 1, 2, 4, 5]), nan_ok=True)

    def test_grid_ends_at_grid_time_nearest_last_measurement(self):
        times, values = self._hourly_at_7_past(n=3)
        df = _series_data_object({"a": (times, values)}).reconcile(resolution="1h")
        assert df["time"].to_list()[-1] == self.T0 + timedelta(hours=2)
        assert df["a_reconciled"].null_count() == 0

    def test_downsampled_and_interpolated_ramp_agree(self):
        """The same linear signal, downsampled from 1-min data and interpolated
        from 2-h data, must give the same value at every grid time whose window
        is fully covered (no half-step offset between the two paths)."""
        fast_times = [self.T0 + timedelta(minutes=i + 0.5) for i in range(240)]
        slow_times = [self.T0 + timedelta(hours=h) for h in (0, 2, 4)]
        minutes = lambda ts: [(t - self.T0).total_seconds() / 60 for t in ts]
        df = _series_data_object({
            "fast": (fast_times, minutes(fast_times)),
            "slow": (slow_times, minutes(slow_times)),
        }).reconcile(resolution="1h", downsample="mean", upsample="interpolate")
        interior = df.filter(pl.col("time").is_between(self.T0 + timedelta(hours=1), self.T0 + timedelta(hours=3)))
        assert interior.height == 3
        assert interior["fast_reconciled"].to_list() == pytest.approx(interior["slow_reconciled"].to_list())


class TestQueryReconcileEdgePadding:
    T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def _query(self, n_hours=6):
        """A stand-in query whose ``data(start, end)`` serves hourly readings (value = hour)
        from T0, windowed like a real fetch, and records each requested window."""
        from types import SimpleNamespace

        times = [self.T0 + timedelta(hours=h) for h in range(n_hours)]
        full = _series_data_object({"a": (times, [float(h) for h in range(n_hours)])})
        fetches = []

        def data(start=None, end=None, **_):
            fetches.append((start, end))
            tall = full._tall.filter(pl.col("time").is_between(start, end))
            return _series_data_object({"a": (tall["time"].to_list(), tall["value_numeric"].to_list())})

        return SimpleNamespace(data=data), fetches

    def test_edge_windows_use_data_beyond_start_and_end(self):
        from acquirium.Client.query import Q

        query, fetches = self._query()
        start, end = self.T0 + timedelta(hours=2), self.T0 + timedelta(hours=4)
        # hourly readings at odd hours sit exactly on the 2h window boundaries
        with pytest.warns(UserWarning, match="window boundary"):
            df = Q.reconcile(query, ["a"], start=start, end=end, resolution="2h", downsample="mean")
        assert fetches == [(start - timedelta(hours=1), end + timedelta(hours=1))]
        assert df["time"].to_list() == [start, end]
        # 02:00 window [01:00, 03:00) and 04:00 window [03:00, 05:00) are both full
        assert df["a_reconciled"].to_list() == [1.5, 3.5]

    def test_inferred_resolution_is_used_for_padding(self):
        from acquirium.Client.query import Q

        query, fetches = self._query()
        start, end = self.T0 + timedelta(hours=1), self.T0 + timedelta(hours=3)
        df = Q.reconcile(query, ["a"], start=start, end=end)
        assert fetches[-1] == (start - timedelta(minutes=30), end + timedelta(minutes=30))
        assert df["a_reconciled"].to_list() == [1.0, 2.0, 3.0]


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
