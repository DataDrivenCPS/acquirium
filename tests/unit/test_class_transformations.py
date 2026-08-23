"""Query-driven transformation declarations and execution."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pyarrow as pa

from acquirium.Materialization.api import OutputSpec, StatefulTransformation, Transformation, outputs
from acquirium.Materialization.compute import PythonArrowAdapter
from acquirium.Materialization.context import ComputeRequest, TransformContext
from acquirium.Materialization.definitions import definition_spec
from acquirium.Materialization.impact import TimeRange, pointwise
from acquirium.Materialization.topology import resolve_bindings


class AddOne(Transformation):
    name = "add-one"
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri="urn:out")}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="input")

    def transform(self, inputs, context):
        context.outputs.declare("output", for_input=inputs).write(
            inputs.values.select("time", (pl.col("value") + 1).alias("value"))
        )


class PerRow(AddOne):
    name = "per-row"
    invocation = "per_row"
    outputs = {"output": outputs.stream(value_kind="numeric", prefix="urn:per-row")}

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature")


class CalibratedTemperature(StatefulTransformation):
    name = "calibrated-temperature"
    invocation = "per_row"
    outputs = {"output": outputs.stream(value_kind="numeric", prefix="urn:calibrated")}

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature")

    def transform(self, inputs, state, context):
        context.outputs.declare("output", for_input=inputs).write(
            inputs.values.with_columns((pl.col("value") + state).alias("value"))
        )


class Graph:
    def __init__(self, refs=("urn:in",)):
        self.refs = tuple(refs)

    def sparql_query(self, query, **kwargs):
        return {
            "columns": ["v0", "ext0", "unit0", "extunit0"],
            "rows": [[f"urn:point:{i}", ref, None, None] for i, ref in enumerate(self.refs)],
        }


def _request(*, output_ref="urn:out"):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    interval = TimeRange(start, start + timedelta(seconds=1))
    inputs = pa.table({
        "operation": ["upsert"],
        "ref_uri": ["urn:in"],
        "ts": pa.array([start], type=pa.timestamp("us", tz="UTC")),
        "numeric_value": [2.0],
        "text_value": [None],
    })
    context = TransformContext(
        "binding", "execution", interval, interval, {"urn:in": 1},
        metadata={
            "logical_key": "urn:in",
            "input_streams": {"input": [{"ref_uri": "urn:in", "point_uri": "urn:point:0", "unit": None}]},
            "output_refs": {"output": (output_ref,)},
            "output_specs": {"output": {"value_kind": "numeric", "ref_uri": output_ref}},
        },
    )
    return ComputeRequest(inputs, context, frozenset({output_ref}), output_specs=context.metadata["output_specs"])


def test_class_transformation_serializes_query_and_output_declaration():
    spec = definition_spec(AddOne.__acquirium_definition__)
    assert spec["invocation"] == "whole_query"
    assert spec["outputs"] == {
        "output": {
            "value_kind": "numeric", "unit": None, "quantity_kind": None,
            "ref_uri": "urn:out", "prefix": None,
        }
    }


def test_per_row_query_resolves_one_binding_per_result_row():
    definition = PerRow.__acquirium_definition__
    bindings = resolve_bindings(definition, Graph(("urn:a", "urn:b")))
    assert len(bindings) == 2
    assert all(binding.inputs["temperature"] == (ref,) for binding, ref in zip(bindings, ("urn:a", "urn:b")))
    assert len({binding.outputs["output"][0] for binding in bindings}) == 2


def test_transformation_receives_normalized_inputs_and_writes_output_handles():
    result = PythonArrowAdapter().execute(AddOne, _request())
    assert result.column("ref_uri").to_pylist() == ["urn:out"]
    assert result.column("numeric_value").to_pylist() == [3.0]


def test_stateful_transformations_use_the_same_query_and_output_contract():
    definition = CalibratedTemperature.__acquirium_definition__
    assert definition.invocation == "per_row"
    assert isinstance(OutputSpec(value_kind="numeric"), OutputSpec)
    assert definition_spec(definition)["invocation"] == "per_row"
