"""A real local server, native Python worker, and semantic DataObject client."""

from pathlib import Path
import re
import shutil
from textwrap import dedent
import time

import pytest

import acquirium as aq

# Each test starts its own server with aq.init().
pytestmark = pytest.mark.usefixtures("separate_server")


def test_config_deploys_the_sine_driver_and_python_threshold(tmp_path):
    # Copy the example and speed up its config-loaded driver.
    example = Path(__file__).resolve().parents[2] / "examples/transformation"
    for name in ("acquirium.toml", "publish.py", "sine_threshold.py"):
        shutil.copyfile(example / name, tmp_path / name)

    # Keep the startup factory local to the test that exercises the config loader.
    (tmp_path / "configured_threshold.py").write_text(dedent("""\
        from pathlib import Path

        import acquirium as aq


        def build_app(client, parameters):
            wave = client.query().measurement(alias="wave", uri="urn:example:sine-wave")
            return aq.App(
                name="high-wave",
                inputs={"wave": aq.Selection.from_query(wave, measurement="wave")},
                outputs={"high": aq.Output()},
                function=aq.PythonFunction.from_file(
                    Path(__file__).with_name("sine_threshold.py"), "threshold"
                ),
                window=aq.Pointwise(),
            )
        """))
    config = tmp_path / "acquirium.toml"
    contents = config.read_text().replace("interval = 1.0", "interval = 0.05")
    config.write_text(contents + '\n[[apps]]\nspec = "configured_threshold.py:build_app"\n')

    client = aq.init(config, timeout=60)
    try:
        # Each phase reports its start and duration while Uvicorn is waiting.
        server_log = (tmp_path / ".data" / ".runtime" / "server.log").read_text()
        startup = server_log.split("Waiting for application startup.", 1)[1]
        startup = startup.split("Application startup complete.", 1)[0]
        started = re.findall(r"^INFO: +\((\d+)/6\) (.+)\.\.\.$", startup, re.MULTILINE)
        finished = re.findall(r"^INFO: +\((\d+)/6\) (.+) \(done in \d+\.\d+s\)$", startup, re.MULTILINE)
        assert [number for number, _ in started] == ["1", "2", "3", "4", "5", "6"]
        assert finished == started

        deadline = time.monotonic() + 30
        # Wait for a full cycle so the comparison exercises both sides of the threshold.
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

        # Compare the driver and app output at the same published epoch.
        epoch = client.engine_snapshot()["epoch"]
        readings = client.stream("sine-wave", "signal").dataframe(epoch=epoch)
        raised = output.dataframe(epoch=epoch)
        assert dict(raised.iter_rows()) == {
            timestamp: value for timestamp, value in readings.iter_rows() if value > 0.5
        }
        crest = 2 ** -0.5
        assert readings["value"][:8].to_list() == pytest.approx(
            [0, crest, 1, crest, 0, -crest, -1, -crest]
        )
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
