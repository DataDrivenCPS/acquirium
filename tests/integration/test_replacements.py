"""Authoritative reads and physical app inputs through a real server restart."""
import polars as pl
import pytest

import acquirium as aq
from examples.turbidity.apps import declarations
from examples.turbidity.run import LOCATIONS, SOURCE, TURBIDITY, readings, register

# Each test starts its own server with aq.init().
pytestmark = pytest.mark.usefixtures("separate_server")


@pytest.mark.integration
def test_replacement_discovery_physical_inputs_and_history_survive_restart(tmp_path):
    options = dict(data_dir=tmp_path / "server", exact_only=True, timeout=180)
    module = tmp_path / "calibrate.py"
    module.write_text('''import polars as pl

def calibrate(frame: pl.DataFrame, context):
    return {"calibrated": frame.select("time", pl.col("value") + 0.1)}
''')
    client = aq.init(**options)
    try:
        # Set up four turbidity streams and deploy the cleaner and its downstream alarm.
        register(client)
        clean, alarm = declarations(client)
        client.deploy_app(clean)
        client.deploy_app(alarm)
        receipt = client.insert_timeseries_batch(SOURCE, {
            location: readings(location, normal) for location, normal in LOCATIONS.items()
        })["receipts"][0]
        first_epoch = client.wait(receipt)["epoch"]
        raw = client.stream(SOURCE, "filter-3")
        original = raw.dataframe(view="raw", value_mode="coalesce")
        cleaned = raw.dataframe()
        assert cleaned.height == 25 and original.height == 48
        assert original["value"].to_list().count("Fault") == 1
        all_turbidity = client.query().measurement(alias="ntu", quantity_kind=TURBIDITY).data()
        assert len(all_turbidity.bindings) == 4
        assert all_turbidity.total_rows == 100
        # A calibration stage becomes the current view without adding a new measurement.
        calibrated = aq.App(
            "calibrate", {"clean": aq.Selection.outputs_of(clean, "turbidity")},
            {"calibrated": aq.ReplacementOutput()},
            aq.PythonFunction.from_file(module, "calibrate"),
        )
        client.deploy_app(calibrated)
        expected = cleaned.with_columns(pl.col("value") + .1)
        assert raw.dataframe().equals(expected)
        # Adding a matching downstream quantity did not rebind the cleaner.
        operators = client.inspect_app(clean.name)["app"]["operators"]
        assert {i["stream"] for op in operators for i in op["inputs"]} == {
            str(client.stream(SOURCE, location)) for location in LOCATIONS
        }
        assert raw.dataframe(epoch=first_epoch).equals(cleaned)
        specific = client.query().measurement(alias="clean", app=clean.name).data()
        assert len(specific.bindings) == 4 and specific.total_rows == 100
        assert len(client.query().measurement(quantity_kind=TURBIDITY).data().bindings) == 4
        # Reopen the same store and check the latest, historical, and raw views.
        aq.shutdown()
        client = aq.init(**options)
        raw = client.stream(SOURCE, "filter-3")
        assert raw.dataframe().equals(expected)
        assert raw.dataframe(epoch=first_epoch).equals(cleaned)
        assert raw.dataframe(view="raw", value_mode="coalesce").equals(original)
        # Removing the calibration stage makes the cleaner current again.
        client.remove_app(calibrated.name)
        assert raw.dataframe().equals(cleaned)
        client.remove_app(alarm.name)
        client.remove_app(clean.name)
        assert raw.dataframe(view="raw", value_mode="coalesce").equals(original)
    finally:
        aq.shutdown()
