"""The resampling vocabulary, and the polars bucketing against the store's SQL."""

from datetime import datetime, timedelta, timezone
import random

import polars as pl
import pytest

from acquirium.internals.resampling import (
    AGGREGATES, EPOCH, describe_every, format_every, nice_every, normalize_agg, normalize_label,
    parse_every,
)


class TestParseEvery:
    @pytest.mark.parametrize("text, expected", [
        ("10s", timedelta(seconds=10)),
        ("500ms", timedelta(milliseconds=500)),
        ("250us", timedelta(microseconds=250)),
        ("5m", timedelta(minutes=5)),
        ("5min", timedelta(minutes=5)),
        ("1.5h", timedelta(hours=1, minutes=30)),
        ("1d", timedelta(days=1)),
        ("1w", timedelta(weeks=1)),
        (" 2 h ", timedelta(hours=2)),
    ])
    def test_strings(self, text, expected):
        assert parse_every(text) == expected

    def test_timedelta_and_seconds(self):
        assert parse_every(timedelta(seconds=3)) == timedelta(seconds=3)
        assert parse_every(3) == timedelta(seconds=3)
        assert parse_every(0.5) == timedelta(milliseconds=500)

    @pytest.mark.parametrize("bad", ["1mo", "1y", "", "ten seconds", "10", "-5s"])
    def test_unparseable(self, bad):
        with pytest.raises(ValueError):
            parse_every(bad)

    @pytest.mark.parametrize("bad", [timedelta(0), timedelta(seconds=-1), 0, -2])
    def test_not_positive(self, bad):
        with pytest.raises(ValueError):
            parse_every(bad)

    def test_wrong_type(self):
        with pytest.raises(TypeError):
            parse_every(True)
        with pytest.raises(TypeError):
            parse_every(None)

    def test_format_round_trips_exactly(self):
        every = timedelta(seconds=7, microseconds=3)
        assert parse_every(format_every(every)) == every

    @pytest.mark.parametrize("every, text", [
        (timedelta(seconds=10), "10s"),
        (timedelta(minutes=5), "5m"),
        (timedelta(hours=1), "1h"),
        (timedelta(days=2), "2d"),
        (timedelta(weeks=1), "1w"),
        (timedelta(milliseconds=500), "500ms"),
        (timedelta(microseconds=250), "250us"),
        (timedelta(seconds=90), "90s"),
    ])
    def test_describe(self, every, text):
        assert describe_every(every) == text


class TestNames:
    def test_aggregates(self):
        for name in AGGREGATES:
            assert normalize_agg(name) == name
        assert normalize_agg("avg") == "mean"
        assert normalize_agg("MEAN") == "mean"
        with pytest.raises(ValueError):
            normalize_agg("mode")
        with pytest.raises(TypeError):
            normalize_agg(None)

    def test_labels(self):
        assert normalize_label("left") == "left"
        assert normalize_label("center") == "center"
        with pytest.raises(ValueError):
            normalize_label("right")


class TestNiceEvery:
    @pytest.mark.parametrize("gap, expected", [
        (timedelta(seconds=2), timedelta(seconds=2)),
        (timedelta(seconds=9.8), timedelta(seconds=10)),
        (timedelta(seconds=11), timedelta(seconds=10)),
        (timedelta(seconds=13), timedelta(seconds=15)),
        (timedelta(seconds=50), timedelta(minutes=1)),
        (timedelta(minutes=7), timedelta(minutes=5)),
        (timedelta(hours=1.4), timedelta(hours=1)),
        (timedelta(days=30), timedelta(weeks=1)),
        (timedelta(microseconds=1), timedelta(milliseconds=1)),
    ])
    def test_nearest_conventional_step(self, gap, expected):
        assert nice_every(gap) == expected

    def test_not_positive(self):
        with pytest.raises(ValueError):
            nice_every(timedelta(0))


# ── polars bucketing against the DuckDB store ─────────────────


def _store(tmp_path):
    from acquirium.Storage.duckdb_store import DuckDBStore
    store = DuckDBStore(tmp_path / "resample.duckdb")
    store.ensure_table()
    return store


def _rows(seed=1, n=400):
    rng = random.Random(seed)
    t0 = datetime(2026, 3, 1, 12, 0, 1, tzinfo=timezone.utc)
    t = t0
    rows = []
    for _ in range(n):
        t += timedelta(seconds=rng.uniform(0.3, 9.0))
        rows.append((t, round(rng.uniform(-50, 50), 3)))
    return rows


@pytest.mark.parametrize("agg", AGGREGATES)
@pytest.mark.parametrize("label", ["left", "center"])
@pytest.mark.parametrize("origin", [EPOCH, datetime(2026, 3, 1, 12, 0, 4, tzinfo=timezone.utc)])
def test_polars_bucketing_matches_the_store(tmp_path, agg, label, origin):
    """``DataObject`` buckets fetched rows itself with the same result the
    store gives a lazy object, for every aggregate, label and origin."""
    from acquirium.Client.data_object import Resampling, _resample_tall

    store = _store(tmp_path)
    ref = "urn:t#equiv"
    rows = _rows()
    store.upsert_rows(ref, rows, value_kind="numeric")
    # a text reading never counts
    store.upsert_rows(ref, [(rows[5][0] + timedelta(milliseconds=1), "ON")], value_kind="numeric")

    spec = Resampling(every=timedelta(seconds=15), agg=agg, origin=origin, label=label)
    from_store = pl.concat([
        pl.from_arrow(b) for b in store.timeseries_resampled(
            ref, every=spec.every, agg=agg, origin=spec.grid_origin,
        )
    ]).select(
        (pl.col("ts") + pl.duration(microseconds=spec.label_shift // timedelta(microseconds=1))).alias("time"),
        pl.col("value").cast(pl.Float64).alias("value_numeric"),
    )
    store.close()

    tall = pl.DataFrame({
        "data_alias": ["m"] * (len(rows) + 1),
        "point_uri": ["urn:t#p"] * (len(rows) + 1),
        "ref_uri": [ref] * (len(rows) + 1),
        "time": [t for t, _ in rows] + [rows[5][0] + timedelta(milliseconds=1)],
        "value_numeric": [v for _, v in rows] + [None],
        "value_text": [None] * len(rows) + ["ON"],
    }).with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
    from_polars = _resample_tall(tall, spec, ["data_alias", "point_uri", "ref_uri"]).select("time", "value_numeric")

    assert from_polars["time"].to_list() == from_store["time"].to_list()
    assert from_polars["value_numeric"].to_list() == pytest.approx(from_store["value_numeric"].to_list())
    assert from_polars.height > 20
