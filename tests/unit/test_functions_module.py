from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from acquirium import functions as f

TIME = pl.Datetime("us", "UTC")
AT = datetime(2026, 1, 1, 10, 0, 40, 123456, tzinfo=timezone.utc)
NS = 1767261640123456000


def test_emit_and_empty_have_utc_datetime_schema():
    result = f.emit({"output_time": AT}, 12)
    assert result.schema == f.SCHEMA
    assert result["time"].item() == AT
    assert result["value"].item() == 12
    assert f.emit({"output_time": AT}, None).schema == f.SCHEMA
    assert f.empty().is_empty()
    assert f.emit({"output_time": AT}, "high")["text"].item() == "high"


def test_batch_outputs_keep_dates_and_quality():
    times = [AT, AT + timedelta(microseconds=1)]
    result = f.emit_windows(pl.DataFrame({
        "output_time": times, "value": [12, 13], "quality": [0, 1],
    }))
    assert result.schema == f.SCHEMA
    assert result["time"].to_list() == times
    assert result["quality"].to_list() == [0, 1]


def test_context_timestamps_and_durations_are_datetimes_and_timedeltas():
    native = {
        "window_ns": NS, "window_start_ns": NS - 1000, "window_end_ns": NS + 1,
        "parameters": {"group": ["urn:tank"]},
        "inputs": [{"span_start_ns": NS, "span_end_ns": NS + 1}],
        "operator": {
            "window": {"kind": "rolling", "lookback_ns": 1000, "step_ns": 1000,
                       "lookahead_ns": 0, "origin_ns": 0},
            "inputs": [{"valid_from": -(2**63), "valid_to": None}],
            "as_of": {"gain": {"max_age_ns": 1000}},
        },
    }
    context = f._public_context(native)
    assert context["output_time"] == AT
    assert context["window_start"] == AT - timedelta(microseconds=1)
    assert context["window_end"] == AT + timedelta(microseconds=1)
    assert context["inputs"] == [{"span_start": AT, "span_end": AT + timedelta(microseconds=1)}]
    assert context["parameters"]["group"] == ("urn:tank",)
    assert context["operator"]["window"]["lookback"] == timedelta(microseconds=1)
    assert context["operator"]["as_of"] == {"gain": timedelta(microseconds=1)}
    assert context["operator"]["inputs"] == [{"valid_from": None, "valid_to": None}]
    assert native["window_end_ns"] == NS + 1
    assert native["parameters"]["group"] == ["urn:tank"]


def test_sequential_helpers_use_datetime_rows_and_periods():
    row = {"event_time_ns": NS, "sample_key": 0, "value": 12., "text": None, "quality": 0}
    context = f._public_context({
        "window_ns": NS + 1000,
        "previous": {"window_ns": NS, "rows": [{"role": "raw", "stream": "urn:raw", **row}],
                     "outputs": {"mean": [row]}},
    })
    assert context["previous"]["output_time"] == AT
    assert f.previous_rows(context)["time"].item() == AT
    assert f.previous_outputs(context, "mean")["time"].item() == AT
    assert f.previous_value(context, "mean") == 12
    assert f.same_period(context, timedelta(minutes=1))
    assert not f.same_period(context, timedelta(microseconds=1))
    assert not f.same_period({"output_time": AT}, timedelta(minutes=1))
    assert f.previous_rows({}).schema["time"] == TIME
    assert f.previous_outputs({}, "mean").schema == f.SCHEMA


def test_batch_context_and_frames_keep_input_and_output_times_separate():
    source = pl.DataFrame({"event_time_ns": [NS], "window_ns": [NS + 1000], "value": [12.]})
    frame = f._public_frame(source)
    context = f._public_context({"windows": [{"window_ns": NS + 1000}]})
    assert frame["time"].item() == AT
    assert frame["output_time"].item() == AT + timedelta(microseconds=1)
    assert context["windows"][0]["output_time"] == frame["output_time"].item()
    result = f._engine_frame(f.emit_windows(frame.select("output_time", "value")))
    assert result["event_time_ns"].item() == NS + 1000


@pytest.mark.parametrize("ns", [NS, -1000, 0])
def test_public_datetime_roundtrip_is_exact(ns):
    public = f._public_frame(pl.DataFrame({"event_time_ns": [ns]}))
    assert public["time"].dtype == TIME
    assert f._engine_frame(public)["event_time_ns"].item() == ns


def test_submicrosecond_values_are_rejected_instead_of_collapsing():
    with pytest.raises(ValueError, match="microsecond precision"):
        f._public_frame(pl.DataFrame({"event_time_ns": [NS, NS + 1]}))
    with pytest.raises(ValueError, match="microsecond precision"):
        f._public_context({"window_ns": NS + 1})
    with pytest.raises(ValueError, match="microsecond precision"):
        f.emit_windows(pl.DataFrame({"output_time": pl.Series([NS + 1]).cast(pl.Datetime("ns", "UTC"))}))


def test_naive_and_numeric_public_timestamps_are_rejected():
    with pytest.raises(TypeError, match="timezone-aware"):
        f.emit({"output_time": AT.replace(tzinfo=None)}, 12)
    with pytest.raises(TypeError, match="timezone-aware"):
        f.emit_windows(pl.DataFrame({"output_time": [1000], "value": [12]}))


def test_output_dates_cannot_overflow_the_engine_timestamp_range():
    result = f.emit({"output_time": datetime(3000, 1, 1, tzinfo=timezone.utc)}, 12)
    with pytest.raises(ValueError, match="supported range"):
        f._engine_frame(result)


def test_function_window_precision_is_validated_before_deployment():
    import acquirium as aq

    function = {"kind": "python", "path": "calculation.py", "entrypoint": "mean", "sha256": "unused"}
    app = aq.App("precision", [], {"mean": aq.Output()}, function, aq.FixedBucket(1001))
    with pytest.raises(ValueError, match="microsecond precision"):
        app.to_dict()
    # Native engine calculations retain their nanosecond contract.
    aq.App("native", [], {"mean": aq.Output()}, aq.Native.mean("mean"), aq.FixedBucket(1001)).to_dict()
