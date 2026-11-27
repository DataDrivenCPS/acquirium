"""The turbidity tutorial's cleaning and alarm functions do what it says."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

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
    """Run the cleaner and read its output for the requested minute."""
    context = {"output_time": START + minute * MINUTE, "parameters": {"threshold": 1.0}}
    result = clean(frame, context)["turbidity"]
    return result.loc[result["time"] == START + minute * MINUTE, "value"].to_list()


def test_clean_drops_faults_and_spikes_and_averages_each_minute():
    # The first two minutes each contain an invalid sample. The third averages two good ones.
    frame = raw([(0, .75), (30, "Fault"), (60, .75), (90, 9.9), (120, .8), (150, .9)])
    assert cleaned(frame, 0) == [.75]
    assert cleaned(frame, 1) == [.75]
    assert cleaned(frame, 2) == pytest.approx([.85])


def test_clean_interpolates_a_missing_minute_and_stops_at_the_edges():
    # Minutes zero and two bracket the gap. Minutes outside that range stay empty.
    frame = raw([(0, .2), (30, .2), (120, .6), (150, .6)])
    assert cleaned(frame, 1) == pytest.approx([.4])
    assert cleaned(frame, -1) == []
    assert cleaned(frame, 3) == []


@pytest.mark.parametrize("readings", [
    pytest.param([], id="no-samples"),
    pytest.param([(0, "Fault"), (30, "Fault")], id="fault-codes"),
    pytest.param([(0, 0.), (30, 9.9)], id="zero-and-outlier"),
])
def test_clean_returns_no_reading_when_no_valid_samples_remain(readings):
    assert cleaned(raw(readings), 0) == []


@pytest.mark.parametrize("values, minute, expected", [
    pytest.param([.8, .8, .8], 2, 1, id="three-high-minutes"),
    pytest.param([.8, .8], 1, 0, id="not-enough-history"),
    pytest.param([.8, .4, .8, .8], 3, 0, id="low-reading-resets-duration"),
    pytest.param([.8, .8, .8], 3, None, id="current-reading-missing"),
])
def test_alarm_needs_every_minute_of_the_duration_above_the_limit(values, minute, expected):
    frame = pl.DataFrame({
        "time": [START + i * MINUTE for i in range(len(values))],
        "value": values,
    }, schema={"time": pl.Datetime("us", "UTC"), "value": pl.Float64})
    context = {
        "output_time": START + minute * MINUTE,
        "parameters": {"limit_ntu": .5, "sustained_minutes": 3},
    }
    assert alarm(frame, context)["alarm"] == expected
