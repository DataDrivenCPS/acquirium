"""A real local server, native Python worker, and semantic DataObject client."""

from datetime import datetime, timezone
import importlib.util
from pathlib import Path

import acquirium as aq


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
