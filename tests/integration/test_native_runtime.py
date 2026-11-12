"""A real local server, native Python worker, and semantic DataObject client."""

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import shutil
import time

import pytest

import acquirium as aq


@pytest.mark.parametrize("deployment", ["python", "config"])
def test_sine_wave_driver_and_python_threshold(tmp_path, deployment):
    example = Path(__file__).resolve().parents[2] / "examples/transformation"
    for name in ("acquirium.toml", "publish.py", "sine_threshold.py", "threshold_app.py"):
        shutil.copyfile(example / name, tmp_path / name)
    config = tmp_path / "acquirium.toml"
    contents = config.read_text().replace("interval = 1.0", "interval = 0.05")
    if deployment == "config":
        contents += '\n[[apps]]\nspec = "threshold_app.py:high_wave"\n'
    config.write_text(contents)

    client = aq.init(config, timeout=60)
    try:
        if deployment == "python":
            spec = importlib.util.spec_from_file_location(
                "threshold_declaration", tmp_path / "threshold_app.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            client.deploy_app(module.high_wave(client, {}))

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
            high = client.query().measurement(app="high-wave")
            if wave.dataframe().height >= 8 and high.dataframe().height >= 3:
                break
            time.sleep(0.1)
        else:
            pytest.fail("driver and Python threshold did not publish a full cycle")

        operator, = client.inspect_app("high-wave")["app"]["operators"]
        output = client.stream(operator["outputs"]["high"])

        def check_outputs(epoch):
            readings = client.stream("sine-wave", "signal").dataframe(epoch=epoch)
            raised = output.dataframe(epoch=epoch)
            expected = {
                timestamp: value for timestamp, value in readings.iter_rows() if value > 0.5
            }
            assert dict(raised.iter_rows()) == expected
            return readings, raised

        readings, raised = check_outputs(client.engine_snapshot()["epoch"])
        crest = 2 ** -0.5
        assert readings["value"][:8].to_list() == pytest.approx(
            [0, crest, 1, crest, 0, -crest, -1, -crest]
        )

        # Removing an input must clear the corresponding Python output.
        timestamp = raised["time"][0]
        receipt = client.delete_timeseries("sine-wave", "signal", [timestamp])
        epoch = client.wait(receipt)["epoch"]
        _, remaining = check_outputs(epoch)
        assert timestamp not in remaining["time"].to_list()

        # Redeployment snapshots the edited bytes at the same source path.
        revised = tmp_path / "sine_threshold.py"
        revised.write_text((tmp_path / "sine_threshold.py").read_text().replace(
            "value > 0.5", "value > 1.1"
        ))
        client.deploy_app(aq.App(
            name="high-wave",
            inputs=[aq.Selection.from_query(wave, alias="wave")],
            outputs={"high": aq.Output()},
            function=aq.PythonFunction.from_file(revised, "threshold"),
            window=aq.Pointwise(),
        ))
        assert high.dataframe().is_empty()
    finally:
        aq.shutdown()


def test_local_runtime_native_function_corrections_deletion_and_restart(tmp_path):
    module_path = (
        Path(__file__).resolve().parents[2]
        / "examples/transformation/temperature_conversion.py"
    )
    spec = importlib.util.spec_from_file_location("temperature_example", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        client.register_streams(
            [
                dict(
                    source_id="gateway",
                    ref_name="temperature",
                    value_kind="numeric",
                    unit="http://qudt.org/vocab/unit/DEG_F",
                    point_uri="urn:runtime:temperature",
                )
            ]
        )
        client.deploy_app(module.fahrenheit_to_celsius(client, {}))
        response = client.insert_timeseries(
            "gateway", "temperature", [(when, 68)], request_id="initial"
        )
        snapshot = client.wait(response["receipts"][0])
        data = (
            client.query()
            .measurement(alias="celsius", app="fahrenheit-to-celsius")
            .dataframe(shape="narrow")
        )
        assert data["value_numeric"].to_list() == [20.0]
        client.insert_timeseries(
            "gateway", "temperature", [(when, 86)], request_id="correction"
        )
        data = (
            client.query()
            .measurement(alias="celsius", app="fahrenheit-to-celsius")
            .dataframe(shape="narrow")
        )
        assert data["value_numeric"].to_list() == [30.0]
        client.delete_timeseries("gateway", "temperature", [when], request_id="delete")
        assert (
            client.query()
            .measurement(app="fahrenheit-to-celsius")
            .dataframe()
            .is_empty()
        )
    finally:
        aq.shutdown()
    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    try:
        assert client.engine_snapshot()["epoch"] >= snapshot["epoch"]
        assert (
            client.query()
            .measurement(app="fahrenheit-to-celsius")
            .dataframe()
            .is_empty()
        )
        assert not (tmp_path / "timeseries.duckdb").exists()
    finally:
        aq.shutdown()


def test_local_runtime_answers_kept_alive_requests_promptly(tmp_path):
    # Without TCP_NODELAY on the server's connections, each response on a
    # kept-alive connection waits about 40 ms for the client's delayed ACK.
    import http.client
    import statistics
    import time
    from urllib.parse import urlsplit

    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=60)
    try:
        url = urlsplit(client.address)
        connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
        times = []
        try:
            for _ in range(11):
                started = time.perf_counter()
                connection.request("GET", "/health")
                response = connection.getresponse()
                response.read()
                assert response.status == 200
                times.append(time.perf_counter() - started)
        finally:
            connection.close()
        assert statistics.median(times[1:]) < 0.02
    finally:
        aq.shutdown()
