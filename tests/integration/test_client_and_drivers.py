"""The real client and driver processes against a real local server.

These cover the paths drivers take in production: inserts without a caller
request id, frames larger than the server's per-request row cap, error
messages surfacing to the client, and drivers running as child processes.
No Docker services are required.
"""

from __future__ import annotations

import os
import textwrap
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
import requests

import acquirium as aq
from acquirium.Drivers.Driver import MAX_REQUEST_ROWS, PollingIngestDriver

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _poll(check, *, timeout: float, message: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result is not None:
            return result
        time.sleep(0.25)
    raise AssertionError(f"timed out after {timeout}s: {message}")


def _frame(rows: int, ref_name: str) -> pl.DataFrame:
    return pl.DataFrame({
        "ts": [START + timedelta(seconds=i) for i in range(rows)],
        "ref_name": [ref_name] * rows,
        "value": [float(i) for i in range(rows)],
    })


class _BulkDriver(PollingIngestDriver):
    """Returns one frame larger than the server's per-request cap."""

    rows = 2 * MAX_REQUEST_ROWS + 5

    def setup(self) -> None:
        self.source_id = "bulk"
        self.declare("level", point_uri="urn:test:bulk-level")

    def collect(self) -> pl.DataFrame:
        return _frame(self.rows, "level")


def test_client_inserts_round_trip_without_request_ids(tmp_path):
    client = aq.init(data_dir=tmp_path, exact_only=True, timeout=120)
    try:
        client.register_streams([
            {"source_id": "gateway", "ref_name": "flow", "point_uri": "urn:test:flow",
             "value_kind": "numeric"},
            {"source_id": "gateway", "ref_name": "pressure", "point_uri": "urn:test:pressure",
             "value_kind": "numeric"},
        ])

        # No request_id: the client must mint one itself, as drivers rely on.
        client.insert_timeseries("gateway", "flow", [(START, 1.5)])
        client.insert_timeseries_arrow("gateway", _frame(3, "pressure").to_arrow())

        flow = client.client.timeseries_df("urn:test:flow")
        assert flow["value"].to_list() == [1.5]
        pressure = client.client.timeseries_df("urn:test:pressure")
        assert pressure["value"].to_list() == [0.0, 1.0, 2.0]

        # The server's reason must reach the caller, not just the status code.
        with pytest.raises(requests.HTTPError, match="at most 10,000 rows"):
            client.insert_timeseries_arrow(
                "gateway", _frame(MAX_REQUEST_ROWS + 1, "pressure").to_arrow()
            )

        # Drivers split frames over the cap into several requests.
        driver = _BulkDriver(client, {"server": {"data_dir": str(tmp_path)}})
        driver.setup()
        driver.tick()
        level = client.client.timeseries_df("urn:test:bulk-level")
        assert level.height == _BulkDriver.rows
    finally:
        aq.shutdown()


DRIVER_SOURCE = """
from datetime import datetime, timedelta, timezone

from acquirium.Drivers.Driver import PollingIngestDriver
from counter_values import VALUE  # a sibling module: file specs put their directory on sys.path

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


class CounterDriver(PollingIngestDriver):
    def setup(self):
        # Driver output on stdout must not corrupt the runner's status line.
        print("counter driver starting")
        self.source_id = "counter"
        self.declare("count", point_uri="urn:test:counter")
        self.n = 0

    def read(self):
        self.n += 1
        self.add("count", VALUE, START + timedelta(seconds=self.n))


class BrokenDriver(PollingIngestDriver):
    def setup(self):
        raise RuntimeError("setup exploded")

    def read(self):
        pass
"""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_drivers_run_as_child_processes(tmp_path):
    driver_dir = tmp_path / "drivers"
    driver_dir.mkdir()
    (driver_dir / "counter_driver.py").write_text(textwrap.dedent(DRIVER_SOURCE))
    (driver_dir / "counter_values.py").write_text("VALUE = 7.0\n")
    config = tmp_path / "acquirium.toml"
    config.write_text(textwrap.dedent("""
        [server]
        data_dir = "data"
        port = 0
        exact_only = true

        [[drivers]]
        spec = "./drivers/counter_driver.py:CounterDriver"
        name = "counter"
        interval = 0.2
    """))

    client = aq.init(config=config, timeout=120)
    base = client.client.base_url
    try:
        def _running():
            drivers = requests.get(f"{base}/drivers/list", timeout=10).json()["drivers"]
            mine = [d for d in drivers if d["name"] == "counter" and d["status"] == "running"]
            return mine[0] if mine else None

        info = _poll(_running, timeout=60, message="config driver running")
        pid = info["pid"]
        assert pid != os.getpid() and _pid_alive(pid)

        def _ingested():
            frame = client.client.timeseries_df("urn:test:counter")
            return frame if frame.height >= 3 else None

        frame = _poll(_ingested, timeout=30, message="driver observations")
        assert set(frame["value"].to_list()) == {7.0}

        # A setup failure is reported to the caller with its reason.
        start = {
            "spec": "./drivers/counter_driver.py:BrokenDriver",
            "name": "broken",
            "config": {"__config_dir": str(tmp_path), "driver": {"interval": 1.0}},
        }
        response = requests.post(f"{base}/drivers/start", json=start, timeout=60)
        assert response.status_code == 400
        assert "setup exploded" in response.json()["detail"]
        assert [d for d in _running_names(base) if d == "broken"] == []

        response = requests.post(f"{base}/drivers/stop", json={"name": "counter"}, timeout=30)
        assert response.status_code == 200
        assert response.json()["stopped"] is True
        assert not _pid_alive(pid)

        # Restart through the API; shutting the server down stops it too.
        start = {
            "spec": "./drivers/counter_driver.py:CounterDriver",
            "name": "counter",
            "config": {"__config_dir": str(tmp_path), "driver": {"interval": 0.2}},
        }
        response = requests.post(f"{base}/drivers/start", json=start, timeout=60)
        assert response.status_code == 200, response.text
        restarted = response.json()["driver"]["pid"]
    finally:
        aq.shutdown()
    _poll(lambda: True if not _pid_alive(restarted) else None,
          timeout=15, message="driver process exits with its server")


def _running_names(base: str) -> list[str]:
    return [d["name"] for d in requests.get(f"{base}/drivers/list", timeout=10).json()["drivers"]]
