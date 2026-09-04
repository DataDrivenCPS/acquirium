from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

import acquirium as aq
from acquirium.Materialization import local as local_check

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Doubler(aq.App):
    name = "doubler"
    outputs = {"doubled": aq.output.per_input(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="input")

    def transform(self, inputs, output, context):
        frame = inputs["input"].df()
        output["doubled"] = frame.select("time", (pl.col("value") * 2).alias("value"))


class Exploder(Doubler):
    name = "exploder"

    def transform(self, inputs, output, context):
        raise ValueError("bad calibration")


class Celsius(Doubler):
    name = "celsius"

    def transform(self, inputs, output, context):
        frame = inputs["input"].in_unit("urn:unit:DEG_C").df()
        output["doubled"] = frame.select("time", "value")


class FakeClient:
    """The narrow client surface a local check uses."""

    def __init__(self, unit=None):
        self.unit = unit
        self.fetched = []

    def graph_status(self):
        return {"published_version": 4}

    def sparql_query(self, query, include_dependencies=True, *, wait_for_fresh=False):
        return {"columns": ["v0", "ext0", "unit0", "extunit0", "lbl0"],
                "rows": [["urn:point", "urn:input", self.unit, None, "Inlet temp"]]}

    def resolve(self, value, kind=None, **kwargs):
        return value

    def timeseries_df(self, uri):
        self.fetched.append(uri)
        return pl.DataFrame({
            "ts": [START, START + timedelta(minutes=1)],
            "value": [1.0, 2.0],
            "uri": [uri, uri],
        })

    def get_conversion_factors(self, from_unit, to_unit):
        # Fahrenheit -> Celsius, in the server's multiplier/offset form.
        return {"compatible": True, "from_multiplier": 5.0 / 9.0, "from_offset": -32.0,
                "to_multiplier": 1.0, "to_offset": 0.0}


def test_local_check_runs_the_app_here_and_reports_like_the_server():
    client = FakeClient()

    result = local_check.check_app(client, Doubler)

    assert result["app"] == "doubler" and result["graph_revision"] == 4
    (binding,) = result["bindings"]
    assert binding["error"] is None
    assert binding["inputs"]["input"][0]["label"] == "Inlet temp"
    assert binding["input_rows"] == {"input": 2}
    doubled = binding["outputs"]["doubled"]
    assert [row["value"] for row in doubled["values"]] == [2.0, 4.0]
    assert doubled["rows"] == 2 and doubled["truncated"] is False
    assert client.fetched == ["urn:input"]


def test_local_check_lets_a_failing_transform_raise():
    # The server-side check reports the error per binding; running locally the
    # traceback is the point, so it must reach the caller.
    with pytest.raises(ValueError, match="bad calibration"):
        local_check.check_app(FakeClient(), Exploder)


def test_local_check_limit_heads_the_output():
    result = local_check.check_app(FakeClient(), Doubler, limit=1)

    doubled = result["bindings"][0]["outputs"]["doubled"]
    assert [row["value"] for row in doubled["values"]] == [2.0]
    assert doubled["rows"] == 2 and doubled["truncated"] is True


def test_local_check_converts_units_through_the_server():
    result = local_check.check_app(FakeClient(unit="urn:unit:DEG_F"), Celsius)

    values = [row["value"] for row in result["bindings"][0]["outputs"]["doubled"]["values"]]
    assert values == pytest.approx([-17.2222222, -16.6666667])
