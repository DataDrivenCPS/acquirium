"""The turbidity tutorial's cleaning and alarm functions do what it says."""

from datetime import datetime, timedelta, timezone

import polars as pl
from pytest import approx

from examples.turbidity.calculations import alarm, clean

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


def raw(rows):
    """Raw readings as (seconds after START, value or text)."""
    return pl.DataFrame({
        "time": [START + timedelta(seconds=s) for s, _ in rows],
        "value": [None if isinstance(v, str) else float(v) for _, v in rows],
        "text": [v if isinstance(v, str) else None for _, v in rows],
    }, schema={"time": pl.Datetime("us", "UTC"), "value": pl.Float64, "text": pl.String}).to_pandas(use_pyarrow_extension_array=True)


def cleaned(frame, minute):
    context = {"output_time": START + minute * MINUTE, "parameters": {"threshold": 1.0}}
    result = clean(frame, context)["turbidity"]
    return result.loc[result["time"] == START + minute * MINUTE, "value"].to_list()


def test_clean_drops_faults_and_spikes_and_averages_each_minute():
    frame = raw([(0, .75), (30, "Fault"), (60, .75), (90, 9.9), (120, .8), (150, .9)])
    assert cleaned(frame, 0) == [.75]
    assert cleaned(frame, 1) == [.75]
    assert cleaned(frame, 2) == approx([.85])


def test_clean_interpolates_a_missing_minute_and_stops_at_the_edges():
    frame = raw([(0, .2), (30, .2), (120, .6), (150, .6)])
    assert cleaned(frame, 1) == approx([.4])
    assert cleaned(frame, -1) == []
    assert cleaned(frame, 3) == []


def test_clean_returns_no_reading_when_no_valid_samples_remain():
    assert cleaned(raw([]), 0) == []
    assert cleaned(raw([(0, "Fault"), (30, "Fault")]), 0) == []
    assert cleaned(raw([(0, 0.), (30, 9.9)]), 0) == []


def test_alarm_needs_every_minute_of_the_duration_above_the_limit():
    def frame(values):
        return pl.DataFrame({
            "time": [START + i * MINUTE for i in range(len(values))],
            "value": values,
        }, schema={"time": pl.Datetime("us", "UTC"), "value": pl.Float64})

    def run(values, minute):
        context = {
            "output_time": START + minute * MINUTE,
            "parameters": {"limit_ntu": .5, "sustained_minutes": 3},
        }
        return alarm(frame(values), context)["alarm"]["value"].to_list()

    assert run([.8, .8, .8], 2) == [1]
    assert run([.8, .8], 1) == [0]
    assert run([.8, .4, .8, .8], 3) == [0]
    assert run([.8, .8, .8], 3) == []
