"""The class -> client -> HTTP registration payload is JSON-safe."""

import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from acquirium.Client.acquirium import Acquirium
from acquirium.Materialization.api import Experiment, Transformation, outputs
from acquirium.Materialization.impact import lookback, pointwise


class ToCelsius(Transformation):
    name = "to_celsius"
    invocation = "per_row"
    outputs = {"celsius": outputs.stream(value_kind="numeric", unit="Cel", prefix="urn:celsius")}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature")

    def transform(self, inputs, context):
        context.outputs.declare("celsius", for_input=inputs).write(inputs.values)


class Comfort(Transformation):
    name = "comfort"
    invocation = "whole_query"
    outputs = {"comfort": outputs.stream(value_kind="numeric", prefix="urn:comfort")}
    impact = lookback(timedelta(minutes=5))

    def build_query(self, aq):
        return aq.query().measurement(alias="comfort")

    def transform(self, inputs, context):
        context.outputs.declare("comfort").write(inputs.values.select(["time", "value"]))


class Identity(Transformation):
    name = "identity"
    invocation = "per_row"
    outputs = {"out": outputs.stream(value_kind="numeric", ref_uri="urn:identity")}

    def build_query(self, aq):
        return aq.query().measurement(alias="input")

    def transform(self, inputs, context):
        context.outputs.declare("out", for_input=inputs).write(inputs.values)


def _acquirium_with_mock_client() -> Acquirium:
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.deploy_transformation.return_value = {"ok": True}
    return aq


def test_register_transformation_serializes_query_invocation_and_outputs():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(ToCelsius)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["invocation"] == "per_row"
    assert payload["outputs"]["celsius"] == {
        "value_kind": "numeric", "unit": "Cel", "quantity_kind": None,
        "ref_uri": None, "prefix": "urn:celsius",
    }
    assert payload["impact"] == {"kind": "pointwise", "before_us": 0, "after_us": 0}
    assert payload["name"] == "to_celsius"


def test_register_transformation_preserves_whole_query_impact():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(Comfort)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["invocation"] == "whole_query"
    assert payload["impact"] == {"kind": "lookback", "before_us": 300_000_000, "after_us": 0}


def test_register_transformation_supports_fixed_output_uri():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(Identity)
    payload = aq.client.deploy_transformation.call_args.args[0]
    assert payload["outputs"]["out"]["ref_uri"] == "urn:identity"


def test_register_transformation_rejects_non_transformation_definitions():
    class Run(Experiment):
        def run(self, ctx):
            return None

    aq = _acquirium_with_mock_client()
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(Run)
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(object())
