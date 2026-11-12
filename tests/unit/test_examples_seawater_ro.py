from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from rdflib import Graph, Namespace

from acquirium.examples import seawater_ro

S223 = Namespace("http://data.ashrae.org/standard223#")


@pytest.fixture(scope="module")
def year() -> pl.DataFrame:
    return seawater_ro.generate()


def test_default_is_2025_in_ten_minute_steps(year):
    assert year.height == 365 * 144
    assert year["timestamp"][0] == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert year["timestamp"][-1] == datetime(2025, 12, 31, 23, 50, tzinfo=timezone.utc)


def test_one_column_per_model_point(year):
    graph = Graph().parse(seawater_ro.model_path)
    points = {str(o).removeprefix("urn:swro/") for o in graph.objects(None, S223.hasProperty)}
    assert set(year.columns) - {"timestamp"} == points


def test_same_call_gives_same_frame(year):
    assert seawater_ro.generate().equals(year)


def test_seed_changes_the_noise(year):
    other = seawater_ro.generate(seed=7)
    assert not other["intake-in-flow-rate-m3s"].equals(year["intake-in-flow-rate-m3s"])


def test_a_window_matches_the_same_rows_of_the_year(year):
    start = datetime(2025, 6, 10, tzinfo=timezone.utc)
    end = start + timedelta(days=2)
    window = seawater_ro.generate(start, end)
    expected = year.filter((pl.col("timestamp") >= start) & (pl.col("timestamp") < end))
    assert window.equals(expected)


def test_naive_datetimes_are_utc():
    naive = seawater_ro.generate(datetime(2025, 3, 1), datetime(2025, 3, 2))
    aware = seawater_ro.generate(
        datetime(2025, 3, 1, tzinfo=timezone.utc), datetime(2025, 3, 2, tzinfo=timezone.utc)
    )
    assert naive.equals(aware)


def test_empty_window():
    start = datetime(2025, 3, 1)
    assert seawater_ro.generate(start, start).height == 0


def test_values_are_finite_and_positive(year):
    values = year.drop("timestamp")
    assert values.null_count().sum_horizontal().item() == 0
    assert all(values[c].is_finite().all() for c in values.columns)
    assert all(values[c].min() > 0 for c in values.columns)


def test_ro_mass_balance(year):
    for part in ("water", "tds"):
        residual = (
            year[f"RO-in-flow-mass-{part}"]
            - year[f"RO-out-flow-mass-{part}"]
            - year[f"RO-out-retentate-flow-mass-{part}"]
        )
        assert residual.abs().max() < 1e-9


def test_recovery_and_rejection_stay_physical(year):
    recovery = year["RO-out-flow-mass-water"] / year["RO-in-flow-mass-water"]
    assert 0.3 < recovery.min() and recovery.max() < 0.6
    feed = year["intake-in-tds-concentration"]
    assert (year["storage-tank-3-out-tds-concentration"] < 0.02 * feed).all()
    brine = year["PXR-brine-out-flow-mass-tds"] / year["PXR-brine-out-flow-mass-water"]
    assert (brine > year["RO-in-flow-mass-tds"] / year["RO-in-flow-mass-water"]).all()
