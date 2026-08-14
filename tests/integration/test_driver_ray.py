"""Integration test for Ray-hosted drivers, end to end.

Unlike the other integration tests (which talk to the dockerized server from
``make testing-up``), this test starts its own server subprocess on this
host. That is deliberate: driver specs are imported on the server host, so a
file-based spec like ``tests/dummy/dummy_csv_driver.py:DummyCSVDriver`` only
resolves when the server and the spec share a filesystem.

Covered flow:
  - server boots with a [[drivers]] entry -> DriverSupervisor starts a
    DriverRunner actor once /health answers
  - the driver ingests generated CSVs back through the HTTP API
  - a file dropped into watch_dir while running is picked up on a later tick
  - /drivers/stop and /drivers/start manage the actor at runtime

First run on a machine is slow (embedding model download + index build,
several minutes). The built embedding cache is harvested into
``tests/.cache/embedding_cache`` and re-seeded on later runs, which brings
server startup down to tens of seconds.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.ipc as ipc
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
DUMMY_DIR = REPO_ROOT / "tests" / "dummy"
DRIVER_SPEC = f"{(DUMMY_DIR / 'dummy_csv_driver.py').as_posix()}:DummyCSVDriver"
DRIVER_NAME = "dummy-csv"

PORT = int(os.getenv("ACQUIRIUM_TEST_DRIVER_PORT", "8323"))
BASE_URL = f"http://localhost:{PORT}"

# Embedding cache persisted across runs so only the first run pays the build.
EMBEDDING_CACHE = REPO_ROOT / "tests" / ".cache" / "embedding_cache"

STARTUP_TIMEOUT_S = float(os.getenv("ACQUIRIUM_TEST_STARTUP_TIMEOUT", "900"))

ROWS_PER_FILE = 30
INITIAL_FILES = 2

FLOW_POINT = "urn:dummy/intake-pump-flow-rate"
STATUS_POINT = "urn:dummy/intake-pump-status"


def _generate_csvs(out_dir: Path, *, num_files: int, start: str) -> None:
    subprocess.run(
        [
            sys.executable,
            str(DUMMY_DIR / "generate_data.py"),
            "--output-dir", str(out_dir),
            "--num-files", str(num_files),
            "--rows-per-file", str(ROWS_PER_FILE),
            "--nan-rate", "0",
            "--start", start,
        ],
        check=True,
        cwd=REPO_ROOT,
    )


def _poll(check, *, timeout: float, interval: float = 1.0, message: str = ""):
    """Call *check* until it returns a non-None value or *timeout* passes."""
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result is not None:
                return result
        except Exception as exc:
            last_exc = exc
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s: {message} (last error: {last_exc})")


def _timeseries_rows(point_uri: str) -> int:
    resp = requests.get(f"{BASE_URL}/timeseries", params={"uri": point_uri}, timeout=30)
    resp.raise_for_status()
    return ipc.open_stream(resp.content).read_all().num_rows


def _list_drivers() -> list[dict]:
    resp = requests.get(f"{BASE_URL}/drivers/list", timeout=30)
    resp.raise_for_status()
    return resp.json()["drivers"]


@pytest.fixture(scope="module")
def server(tmp_path_factory: pytest.TempPathFactory):
    tmp = tmp_path_factory.mktemp("driver_ray")
    data_dir = tmp / "data"
    watch_dir = tmp / "raw"
    watch_dir.mkdir()
    data_dir.mkdir()

    _generate_csvs(watch_dir, num_files=INITIAL_FILES, start="2026-06-01 09:00")

    # Seed the embedding cache so startup skips the multi-minute index build.
    if EMBEDDING_CACHE.exists():
        shutil.copytree(EMBEDDING_CACHE, data_dir / "embedding_cache")

    config_path = tmp / "acquirium.toml"
    config_path.write_text(
        f"""
[server]
enabled = true
host = "127.0.0.1"
port = {PORT}
data_dir = "{data_dir.as_posix()}"
timeseries_backend = "duckdb"

[driver]
server_url = "localhost"
server_port = {PORT}
interval = 1.0

[[drivers]]
spec = "{DRIVER_SPEC}"
name = "{DRIVER_NAME}"
interval = 1.0
watch_dir = "{watch_dir.as_posix()}"
glob = "*.csv"
"""
    )

    log_path = tmp / "server.log"
    env = dict(os.environ)
    # Ray's uv-run hook requires cwd inside the uv project; we set cwd to the
    # repo root AND disable the hook so the test works however pytest was run.
    env["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [sys.executable, "-m", "acquirium.cli", "server", "--config", str(config_path)],
            cwd=REPO_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    try:
        def _healthy():
            if proc.poll() is not None:
                raise AssertionError(
                    f"server exited early (code {proc.returncode}); log: {log_path}"
                )
            r = requests.get(f"{BASE_URL}/health", timeout=2)
            return True if r.ok else None

        _poll(_healthy, timeout=STARTUP_TIMEOUT_S, interval=2.0, message=f"server health at {BASE_URL}")

        # Harvest the embedding cache for future runs.
        built = data_dir / "embedding_cache"
        if built.exists() and not EMBEDDING_CACHE.exists():
            EMBEDDING_CACHE.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(built, EMBEDDING_CACHE)

        yield {"watch_dir": watch_dir, "config_dir": tmp, "log_path": log_path}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


class TestRayDriver:
    def test_config_driver_starts(self, server):
        def _running():
            drivers = _list_drivers()
            mine = [d for d in drivers if d["name"] == DRIVER_NAME]
            if mine and mine[0]["status"] == "running":
                return mine[0]
            return None

        info = _poll(_running, timeout=120, message="config driver running in /drivers/list")
        assert info["spec"] == DRIVER_SPEC
        assert info["interval"] == 1.0

    def test_csvs_ingested(self, server):
        expected = INITIAL_FILES * ROWS_PER_FILE

        def _ingested():
            rows = _timeseries_rows(FLOW_POINT)
            return rows if rows >= expected else None

        rows = _poll(_ingested, timeout=120, message=f"{expected} rows for {FLOW_POINT}")
        assert rows == expected
        # Status columns carry ON/OFF text; they must land too (text value kind).
        assert _timeseries_rows(STATUS_POINT) == expected

    def test_new_file_picked_up_live(self, server):
        # Distinct start time so the new rows don't collide with existing ts.
        _generate_csvs(server["watch_dir"], num_files=1, start="2026-06-02 09:00")
        expected = (INITIAL_FILES + 1) * ROWS_PER_FILE

        def _ingested():
            rows = _timeseries_rows(FLOW_POINT)
            return rows if rows >= expected else None

        rows = _poll(_ingested, timeout=60, message=f"{expected} rows after live file drop")
        assert rows == expected

    def test_stop_and_restart(self, server):
        resp = requests.post(f"{BASE_URL}/drivers/stop", json={"name": DRIVER_NAME}, timeout=60)
        assert resp.status_code == 200
        assert resp.json()["stopped"] is True
        assert [d for d in _list_drivers() if d["name"] == DRIVER_NAME] == []

        # Stopping an unknown driver is a 404.
        resp = requests.post(f"{BASE_URL}/drivers/stop", json={"name": DRIVER_NAME}, timeout=60)
        assert resp.status_code == 404

        # Restart through the API, the way `acquirium driver start` does it.
        payload = {
            "spec": DRIVER_SPEC,
            "name": DRIVER_NAME,
            "interval": 1.0,
            "config": {
                "__config_dir": str(server["config_dir"]),
                "driver": {
                    "server_url": "localhost",
                    "server_port": PORT,
                    "interval": 1.0,
                    "watch_dir": server["watch_dir"].as_posix(),
                    "glob": "*.csv",
                },
            },
        }
        resp = requests.post(f"{BASE_URL}/drivers/start", json=payload, timeout=300)
        assert resp.status_code == 200, resp.text
        assert resp.json()["driver"]["status"] == "running"

        # Duplicate start must be rejected.
        resp = requests.post(f"{BASE_URL}/drivers/start", json=payload, timeout=300)
        assert resp.status_code == 400
        assert "already running" in resp.json()["detail"]
