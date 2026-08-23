"""Class-based transformation declarations and execution."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa

from acquirium.Materialization.api import MappedTransformation, StatefulTransformation, Transformation, outputs, select
from acquirium.Materialization.bindings import per_input
from acquirium.Materialization.compute import PythonArrowAdapter
from acquirium.Materialization.context import ComputeRequest, TransformContext
from acquirium.Materialization.definitions import definition_spec
from acquirium.Materialization.impact import TimeRange, pointwise
from acquirium.Materialization.topology import resolve_bindings


class AddOne(Transformation):
    inputs = {"input": "urn:in"}
    outputs = {"output": "urn:out"}
    impact = pointwise()

    def transform(self, batch, context):
        return pa.table({
            "ref_uri": [context.outputs["output"][0]],
            "ts": batch.column("ts"),
            "numeric_value": [batch.column("numeric_value")[0].as_py() + 1],
            "text_value": [None],
        })


class Temperatures(MappedTransformation):
    bind = per_input(select(quantity_kind="http://qudt.org/vocab/quantitykind/Temperature"))
    outputs = outputs.per_input(name="converted")
    impact = pointwise()

    def transform(self, batch, context):
        return batch


class CalibratedTemperature(StatefulTransformation):
    inputs = {"input": "urn:in"}
    outputs = {"output": "urn:calibrated"}
    impact = pointwise()

    def transform(self, batch, state, context):
        return batch


def test_class_transformation_compiles_an_explicit_batch_definition():
    spec = definition_spec(AddOne.__acquirium_definition__)
    assert spec["execution"] == "batch"
    assert spec["inputs"] == {"input": "urn:in"}
    assert spec["outputs"] == {"output": "urn:out"}


def test_stateful_class_compiles_from_a_class_declaration():
    definition = CalibratedTemperature.__acquirium_definition__
    assert definition.entrypoint.endswith(":CalibratedTemperature")
    assert definition_spec(definition)["inputs"] == {"input": "urn:in"}


def test_class_transformation_executes_with_binding_outputs_in_context():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    interval = TimeRange(start, start + timedelta(seconds=1))
    inputs = pa.table({
        "operation": ["upsert"], "ref_uri": ["urn:in"],
        "ts": pa.array([start], type=pa.timestamp("us", tz="UTC")),
        "numeric_value": [2.0], "text_value": [None],
    })
    context = TransformContext("binding", "execution", interval, interval, {"urn:in": 1},
                               outputs={"output": ("urn:out",)})
    result = PythonArrowAdapter().execute(
        AddOne, ComputeRequest(inputs, context, frozenset({"urn:out"}))
    )
    assert result.column("numeric_value").to_pylist() == [3.0]


def test_mapped_class_resolves_one_binding_per_semantic_match():
    class Graph:
        def sparql_query(self, query, **kwargs):
            assert "qudt.org/vocab/quantitykind/Temperature" in query
            return {"columns": ["ref_uri"], "rows": [["urn:a"], ["urn:b"]]}

    bindings = resolve_bindings(definition_spec(Temperatures.__acquirium_definition__), Graph())
    assert [binding.metadata["input_ref"] for binding in bindings] == ["urn:a", "urn:b"]
    assert len({binding.logical_key for binding in bindings}) == 2
