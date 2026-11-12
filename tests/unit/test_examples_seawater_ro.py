from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from rdflib import Graph, Namespace

from acquirium.examples import seawater_ro

S223 = Namespace("http://data.ashrae.org/standard223#")


@pytest.fixture(scope="module")
def data() -> dict:
    return seawater_ro.generate()


@pytest.fixture(scope="module")
def year(data) -> pl.DataFrame:
    """The default year with one column per point."""
    first = next(iter(data.values()))
    columns = {"ts": [ts for ts, _ in first]}
    columns.update({name: [value for _, value in rows] for name, rows in data.items()})
    return pl.DataFrame(columns)


def test_layout_for_insert_timeseries_batch(data):
    name, rows = next(iter(data.items()))
    assert isinstance(name, str)
    ts, value = rows[0]
    assert ts == datetime(2025, 1, 1, tzinfo=timezone.utc) and ts.utcoffset() == timedelta(0)
    assert isinstance(value, float)
    assert all(a[0] < b[0] for a, b in zip(rows, rows[1:]))


def test_default_is_2025_in_ten_minute_steps(data):
    assert all(len(rows) == 365 * 144 for rows in data.values())
    rows = next(iter(data.values()))
    assert rows[-1][0] == datetime(2025, 12, 31, 23, 50, tzinfo=timezone.utc)


def test_one_stream_per_model_point(data):
    graph = Graph().parse(seawater_ro.model_path)
    points = {str(o).removeprefix("urn:swro/") for o in graph.objects(None, S223.hasProperty)}
    assert set(data) == points


def test_same_call_gives_same_data(data):
    assert seawater_ro.generate() == data


def test_seed_changes_the_noise(data):
    name = "intake-in-flow-rate-m3s"
    assert seawater_ro.generate(seed=7)[name] != data[name]


def test_a_window_matches_the_same_rows_of_the_year(data):
    start = datetime(2025, 6, 10, tzinfo=timezone.utc)
    end = start + timedelta(days=2)
    window = seawater_ro.generate(start, end)
    for name, rows in data.items():
        assert window[name] == [row for row in rows if start <= row[0] < end]


def test_naive_datetimes_are_utc():
    naive = seawater_ro.generate(datetime(2025, 3, 1), datetime(2025, 3, 2))
    aware = seawater_ro.generate(
        datetime(2025, 3, 1, tzinfo=timezone.utc), datetime(2025, 3, 2, tzinfo=timezone.utc)
    )
    assert naive == aware


def test_empty_window():
    start = datetime(2025, 3, 1)
    assert all(rows == [] for rows in seawater_ro.generate(start, start).values())


def test_values_are_finite_and_positive(year):
    values = year.drop("ts")
    assert all(values[c].is_finite().all() and values[c].min() > 0 for c in values.columns)


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
