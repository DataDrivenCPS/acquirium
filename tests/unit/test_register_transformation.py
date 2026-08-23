"""The decorator -> client -> HTTP registration payload must be JSON-safe."""
import json
from unittest.mock import MagicMock

import pytest

from acquirium.Client.acquirium import Acquirium
from acquirium.Materialization.api import experiment, outputs, select, transform
from acquirium.Materialization.bindings import by_entity, per_input
from acquirium.Materialization.impact import lookback, pointwise
from datetime import timedelta


def _acquirium_with_mock_client() -> Acquirium:
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.deploy_transformation.return_value = {"ok": True}
    return aq


def test_register_transformation_serializes_per_input_binding_helpers():
    @transform(bind=per_input(select(quantity_kind="Temperature")),
               outputs=outputs.per_input(name="celsius", unit="Cel"),
               impact=pointwise())
    def to_celsius(batch, ctx):
        return batch

    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(to_celsius)
    payload = aq.client.deploy_transformation.call_args.args[0]
    # requests.post(json=...) raises TypeError on helper dataclasses; the
    # payload must be plain JSON types end to end.
    json.dumps(payload)
    assert payload["bind"] == {"selector": {"criteria": {"quantity_kind": "Temperature"}}}
    assert payload["outputs"] == {"mode": "per_input", "name": "celsius", "unit": "Cel"}
    assert payload["impact"] == {"kind": "pointwise", "before_us": 0, "after_us": 0}
    assert payload["inputs"] is None
    assert payload["name"] == "to_celsius"


def test_register_transformation_serializes_entity_bindings_and_lookback_impact():
    @transform(bind=by_entity({"temperature": select(quantity_kind="Temperature"),
                               "humidity": select(quantity_kind="RelativeHumidity")},
                              entity_alias="ahu"),
               outputs=outputs.per_input(name="comfort"),
               impact=lookback(timedelta(minutes=5)))
    def comfort(batch, ctx):
        return batch

    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(comfort)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["bind"]["entity_alias"] == "ahu"
    assert set(payload["bind"]["selectors"]) == {"temperature", "humidity"}
    assert payload["bind"]["selectors"]["temperature"] == {"criteria": {"quantity_kind": "Temperature"}}
    assert payload["impact"] == {"kind": "lookback", "before_us": 300_000_000, "after_us": 0}


def test_register_transformation_direct_inputs_payload():
    @transform(inputs=select(ref_uris=["urn:in"]), outputs=outputs.per_input(name="out"))
    def identity(batch, ctx):
        return batch

    aq = _acquirium_with_mock_client()
    aq.deploy_transformation(identity)
    payload = aq.client.deploy_transformation.call_args.args[0]
    json.dumps(payload)
    assert payload["inputs"] == {"criteria": {"ref_uris": ["urn:in"]}}
    assert payload["bind"] is None
    # Scalar per-input declarations default to pointwise impact.
    assert payload["impact"] == {"kind": "pointwise", "before_us": 0, "after_us": 0}


def test_register_transformation_rejects_non_transformation_definitions():
    @experiment()
    def run(ctx):
        return None

    aq = _acquirium_with_mock_client()
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(run)
    with pytest.raises(ValueError, match="transform"):
        aq.deploy_transformation(object())
