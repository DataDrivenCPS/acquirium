from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pyarrow as pa
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


def test_batch_outputs_keep_dates_and_values():
    times = [AT, AT + timedelta(microseconds=1)]
    result = f.emit_windows(pl.DataFrame({
        "output_time": times, "value": [12, 13],
    }))
    assert result.schema == f.SCHEMA
    assert result["time"].to_list() == times
    assert result["value"].to_list() == [12, 13]


def test_context_timestamps_and_durations_are_datetimes_and_timedeltas():
    native = native_context(NS)
    native["inputs"] = [dict(native["inputs"][0], reads_ns=(NS, NS + 1))]
    window = {"kind": "rolling", "lookback_ns": 1000, "step_ns": 1000, "lookahead_ns": 0, "origin_ns": 0}
    _, context = f._epoch_input(native_frame(), native, window, ["out"])
    assert context["output_time"] == AT
    assert context["window_start"] == AT - timedelta(microseconds=1)
    assert context["window_end"] == AT + timedelta(microseconds=1)
    assert context["inputs"] == [{
        "input": "raw", "stream": "urn:raw", "context": {},
        "span_start": AT, "span_end": AT + timedelta(microseconds=1),
    }]
    assert context["operator"]["inputs"] == context["inputs"]
    assert context["parameters"]["group"] == ("urn:tank",)
    assert context["operator"]["window"]["lookback"] == timedelta(microseconds=1)
    assert context["operator"]["window"]["origin"] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert native["parameters"]["group"] == ["urn:tank"]


def test_sequential_helpers_use_datetime_rows_and_periods():
    context = {
        "output_time": AT + timedelta(microseconds=1),
        "previous": {
            "output_time": AT,
            "outputs": {"mean": [{"time": AT, "value": 12., "text": None}]},
        },
    }
    assert f.previous_outputs(context, "mean")["time"].item() == AT
    assert f.previous_value(context, "mean") == 12
    assert f.same_period(context, timedelta(minutes=1))
    assert not f.same_period(context, timedelta(microseconds=1))
    assert not f.same_period({"output_time": AT}, timedelta(minutes=1))
    assert f.previous_outputs({}, "mean").schema == f.SCHEMA


def test_batch_context_and_frames_keep_input_and_output_times_separate():
    def calculate(frame, context):
        assert frame["time"][0] == AT
        assert frame["output_time"][0] == AT + timedelta(microseconds=1)
        assert context["windows"][0]["output_time"] == frame["output_time"][0]
        return {"out": f.emit_windows(frame.head(1).select("output_time", "value"))}

    result = batch(calculate, anchor=NS + 1000)
    assert result["time"].dt.epoch("ns").item() == NS + 1000


@pytest.mark.parametrize("ns", [NS, -1000, 0])
def test_public_datetime_roundtrip_is_exact(ns):
    public, _ = f._epoch_input(native_frame([ns, ns + 1000]), native_context(ns), {"kind": "pointwise"}, ["out"])
    assert public["time"].dtype == TIME
    assert public["time"].dt.epoch("ns")[0] == ns


def test_submicrosecond_values_are_rejected_instead_of_collapsing():
    with pytest.raises(ValueError, match="microsecond precision"):
        run(lambda frame, context: {}, frame=native_frame([NS, NS + 1]))
    with pytest.raises(ValueError, match="microsecond precision"):
        f._epoch_input(native_frame(), native_context(NS + 1), {"kind": "pointwise"}, ["out"])
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
        checked(result)


def test_function_window_precision_is_validated_before_deployment():
    import acquirium as aq

    function = {"kind": "python", "path": "calculation.py", "entrypoint": "mean", "sha256": "unused"}
    app = aq.App("precision", {}, {"mean": aq.Output()}, function, aq.FixedBucket(1001))
    with pytest.raises(ValueError, match="microsecond precision"):
        app.to_dict()
    # Native engine calculations retain their nanosecond contract.
    aq.App("native", {}, {"mean": aq.Output()}, aq.Native.mean("mean"), aq.FixedBucket(1001)).to_dict()


def test_engine_wire_accepts_only_public_output_columns():
    result = f.emit({"output_time": AT}, 12)
    assert checked(result).columns == ["time", "value", "text"]
    with pytest.raises(ValueError, match="unexpected soft sensor output columns"):
        checked(result.with_columns(pl.lit(1).alias("quality")))
    with pytest.raises(ValueError, match="unexpected soft sensor output columns"):
        checked(result.with_columns(pl.lit(1).alias("sample_key")))


def native_frame(times=(NS, NS + 1000)):
    return pl.DataFrame({
        "role": ["raw", "raw"], "stream": ["urn:raw", "urn:raw"],
        "time": pl.Series(times).cast(pl.Datetime("ns", "UTC")),
        "value": [12., None], "text": [None, "Fault"],
    })


def native_context(anchor=NS):
    return {
        "template": "mean", "instance": "urn:tank", "parameters": {"group": ["urn:tank"]},
        "anchor_ns": anchor, "body_ns": (anchor, anchor + 1),
        "inputs": [{"role": "raw", "stream": "urn:raw", "context": "{}", "reads_ns": (None, None)}],
    }


def checked(output):
    return f._epoch_outputs({"out": output}, ["out"], [], [])["out"]


def run(calculate, *, frame=None, context=None, port="out"):
    adapter = f._epoch_function(calculate, {"kind": "pointwise"}, [port], [])
    return adapter(native_frame() if frame is None else frame, context or native_context())[port]


def batch(calculate, *, anchor=NS):
    adapter = f._epoch_batch_function(calculate, {"kind": "pointwise"}, ["out"], [])
    group = [pl.lit("urn:tank").alias("instance"), pl.lit(anchor).alias("anchor")]
    return adapter(native_frame().with_columns(group), {
        "template": "mean", "instances": {"urn:tank": {"parameters": {}}},
        "windows": pl.DataFrame({"body_start": [anchor], "body_end": [anchor + 1]}).with_columns(group),
        "inputs": pl.DataFrame({
            "role": ["raw"], "stream": ["urn:raw"], "context": ["{}"],
            "reads_start": [None], "reads_end": [None],
        }).with_columns(group),
    })["out"]


def test_pandas_annotation_selects_arrow_columns_without_copying_value_buffers():
    source = native_frame()
    value_buffer = source["value"].to_arrow().buffers()[1].address

    def calculate(frame: pd.DataFrame, context):
        assert isinstance(frame, pd.DataFrame)
        assert all(isinstance(dtype, pd.ArrowDtype) for dtype in frame.dtypes)
        assert frame["time"].dtype.pyarrow_dtype == pa.timestamp("us", "UTC")
        assert frame["time"].iloc[0] == AT
        assert frame["value"].iloc[1] is pd.NA
        assert frame["text"].iloc[1] == "Fault"
        assert list(frame["input"]) == ["raw", "raw"] and "role" not in frame
        assert frame["value"].array.__arrow_array__().chunk(0).buffers()[1].address == value_buffer
        assert "sample_key" not in frame and "quality" not in frame
        assert context["output_time"] == AT
        result = f.emit(context, frame["value"].mean())
        assert isinstance(result, pd.DataFrame)
        assert all(isinstance(dtype, pd.ArrowDtype) for dtype in result.dtypes)
        assert isinstance(f.empty(), pd.DataFrame)
        return {"mean": result}

    result = run(calculate, frame=source, port="mean")
    assert result["time"].dt.epoch("ns").to_list() == [NS]
    assert result["value"].to_list() == [12.]
    # The choice applies only during that call, including when a worker changes apps.
    assert isinstance(f.empty(), pl.DataFrame)


@pytest.mark.parametrize("annotation", [None, pl.DataFrame])
def test_polars_remains_the_default_and_can_be_selected_explicitly(annotation):
    def calculate(frame, context):
        assert isinstance(frame, pl.DataFrame)
        assert isinstance(f.emit(context, 1), pl.DataFrame)
        return {"out": f.empty()}

    if annotation is not None:
        calculate.__annotations__["frame"] = annotation
    assert run(calculate).is_empty()


def test_postponed_and_aliased_annotations_are_resolved_per_entrypoint(tmp_path):
    source = (
        "from __future__ import annotations\n"
        "from pandas import DataFrame as PandasFrame\n"
        "import polars as pl\n"
        "from acquirium.functions import emit\n"
        "def pandas_call(frame: 'PandasFrame', context) -> 'UnrelatedForwardReference':\n"
        "    assert isinstance(frame, PandasFrame)\n"
        "    return {'out': emit(context, frame['value'].mean())}\n"
        "def polars_call(frame: pl.DataFrame, context):\n"
        "    assert isinstance(frame, pl.DataFrame)\n"
        "    return {'out': emit(context, frame['value'].mean())}\n"
    )
    path = tmp_path / "calculations.py"
    path.write_text(source)
    module = {}
    exec(compile(path.read_bytes(), str(path), "exec"), module)
    for name in ("pandas_call", "polars_call", "pandas_call"):
        output = run(module[name])
        assert output["value"].item() == 12.


@pytest.mark.parametrize("annotation", [pd.Series, "MissingFrameType", pd.DataFrame | pl.DataFrame])
def test_ambiguous_or_unresolved_frame_annotations_raise_clear_errors(annotation):
    def calculate(frame, context):
        raise AssertionError("invalid annotations must be rejected before a call")

    calculate.__annotations__["frame"] = annotation
    with pytest.raises(TypeError, match="frame annotation|annotate frame"):
        f._epoch_function(calculate, {"kind": "pointwise"}, ["out"], [])


def test_a_failed_pandas_call_cannot_change_the_next_calls_helpers():
    def calculate(frame: pd.DataFrame, context):
        assert isinstance(f.empty(), pd.DataFrame)
        raise RuntimeError("test failure")

    with pytest.raises(RuntimeError, match="test failure"):
        run(calculate)
    assert isinstance(f.empty(), pl.DataFrame)


@pytest.mark.parametrize("missing", [pd.NA, float("nan"), None])
def test_pandas_missing_scalar_results_produce_empty_outputs(missing):
    def calculate(frame: pd.DataFrame, context):
        return {"out": f.emit(context, missing)}

    assert run(calculate).is_empty()


def test_pandas_batch_columns_and_manual_results_keep_their_timestamps():
    def calculate(frame: pd.DataFrame, context):
        assert frame["output_time"].dtype.pyarrow_dtype == pa.timestamp("us", "UTC")
        means = frame.groupby("output_time", as_index=False)["value"].mean()
        result = f.emit_windows(means)
        assert isinstance(result, pd.DataFrame)
        return {"out": result}

    result = batch(calculate)
    assert result["time"].dt.epoch("ns").to_list() == [NS]
    assert result["value"].to_list() == [12.]


def test_pandas_sequential_helpers_keep_arrow_types_and_python_context_dates():
    def calculate(frame: pd.DataFrame, context):
        outputs = f.previous_outputs(context, "out")
        assert isinstance(outputs, pd.DataFrame)
        assert outputs["time"].dtype.pyarrow_dtype == pa.timestamp("us", "UTC")
        assert context["previous"]["output_time"] == AT
        assert f.previous_value(context, "out") == 5.
        return {"out": f.emit(context, f.previous_value(context, "out") + frame["value"].sum())}

    native = native_context(NS + 1000)
    native["inputs"].append({"port": "out", "stream": "urn:out", "role": "previous"})
    earlier = pl.DataFrame({
        "role": ["previous"], "stream": ["urn:out"],
        "time": pl.Series([NS]).cast(pl.Datetime("ns", "UTC")),
        "value": [5.], "text": pl.Series([None], dtype=pl.String),
    })
    result = run(calculate, frame=pl.concat([native_frame(), earlier]), context=native)
    assert result["time"].dt.epoch("ns").item() == NS + 1000
    assert result["value"].item() == 17.


def test_returned_pandas_frames_use_the_same_validation_as_polars():
    valid = f.emit({"output_time": AT}, 12).to_pandas(use_pyarrow_extension_array=True)
    assert checked(valid)["time"].dt.epoch("ns").item() == NS
    with pytest.raises(ValueError, match="unexpected soft sensor output columns"):
        checked(valid.assign(quality=1))
    with pytest.raises(ValueError, match="reset_index"):
        checked(valid.set_index("time"))
    with pytest.raises(TypeError, match="timezone-aware"):
        checked(pd.DataFrame({"time": [AT.replace(tzinfo=None)], "value": [1.]}))
    with pytest.raises(ValueError, match="microsecond precision"):
        checked(pd.DataFrame({"time": [pd.Timestamp(AT) + pd.Timedelta(1, "ns")], "value": [1.]}))
