"""The class -> client -> HTTP registration payload must be JSON-safe."""
from datetime import timedelta
import json
from unittest.mock import MagicMock

import pytest

from acquirium.Client.acquirium import Acquirium
from acquirium.Materialization.api import Experiment, MappedTransformation, Transformation, outputs, select
from acquirium.Materialization.bindings import by_entity, per_input
from acquirium.Materialization.impact import lookback, pointwise


class ToCelsius(MappedTransformation):
    name = "to_celsius"
    bind = per_input(select(quantity_kind="http://qudt.org/vocab/quantitykind/Temperature"))
    outputs = outputs.per_input(name="celsius", unit="Cel")
    impact = pointwise()

    def transform(self, batch, context):
        return batch


class Comfort(MappedTransformation):
    name = "comfort"
    bind = by_entity(
        {
            "temperature": select(quantity_kind="http://qudt.org/vocab/quantitykind/Temperature"),
            "humidity": select(quantity_kind="http://qudt.org/vocab/quantitykind/RelativeHumidity"),
        },
        entity_alias="ahu",
    )
    outputs = outputs.per_input(name="comfort")
    impact = lookback(timedelta(minutes=5))

    def transform(self, batch, context):
        return batch


class Identity(Transformation):
    name = "identity"
    inputs = select(ref_uris=["urn:in"])
    outputs = outputs.per_input(name="out")
    execution = "scalar"

    def transform(self, value):
        return value


def _acquirium_with_mock_client() -> Acquirium:
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.deploy_transformation.return_value = {"ok": True}
    return aq


def test_register_transformation_serializes_per_input_binding_helpers():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(ToCelsius)
    payload = aq.client.deploy_transformation.call_args.args[0]
    # requests.post(json=...) raises TypeError on helper dataclasses; the
    # payload must be plain JSON types end to end.
    json.dumps(payload)
    assert payload["bind"] == {"selector": {"criteria": {"quantity_kind": "http://qudt.org/vocab/quantitykind/Temperature"}}}
    assert payload["outputs"] == {"mode": "per_input", "name": "celsius", "unit": "Cel"}
    assert payload["impact"] == {"kind": "pointwise", "before_us": 0, "after_us": 0}
    assert payload["inputs"] is None
    assert payload["name"] == "to_celsius"


def test_register_transformation_serializes_entity_bindings_and_lookback_impact():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(Comfort)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["bind"]["entity_alias"] == "ahu"
    assert set(payload["bind"]["selectors"]) == {"temperature", "humidity"}
    assert payload["bind"]["selectors"]["temperature"] == {"criteria": {"quantity_kind": "http://qudt.org/vocab/quantitykind/Temperature"}}
    assert payload["impact"] == {"kind": "lookback", "before_us": 300_000_000, "after_us": 0}


def test_register_transformation_direct_inputs_payload():
    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(Identity)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["inputs"] == {"criteria": {"ref_uris": ["urn:in"]}}
    assert payload["bind"] is None
    assert payload["execution"] == "scalar"
    # Scalar per-input declarations default to pointwise impact.
    assert payload["impact"] == {"kind": "pointwise", "before_us": 0, "after_us": 0}


def test_register_transformation_rejects_non_transformation_definitions():
    class Run(Experiment):
        def run(self, ctx):
            return None

    aq = _acquirium_with_mock_client()
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(Run)
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(object())
