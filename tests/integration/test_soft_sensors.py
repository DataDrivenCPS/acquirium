"""Semantic discovery and derived reads through a real local server."""

import json
import os
import time
from dataclasses import replace

import pandas as pd
import polars as pl
import pytest

import acquirium as aq
from acquirium.apps import output_reference
from examples.soft_sensors.declarations import (
    ALARM,
    DIRECTORY,
    HOTTEST,
    MEAN,
    PLANT,
    SOURCE,
    TANK_A,
    TANK_B,
    declarations,
    deploy,
    register,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("separate_server")]
SECOND = 1_000_000_000
MEAN_A = output_reference(MEAN, TANK_A, "mean")
MEAN_B = output_reference(MEAN, TANK_B, "mean")
HIGHEST = output_reference(HOTTEST, PLANT, "hottest")
ALARM_STREAM = output_reference(ALARM, PLANT, "alarm")


def assert_outputs(client, expected, *, epoch=None):
    """Compare timestamped outputs at one epoch, with expected times in seconds."""
    if epoch is None:
        epoch = client.engine_snapshot()["epoch"]
    for reference, rows in expected.items():
        frame = client.client.timeseries_df(reference, epoch=epoch)
        assert frame["ts"].cast(pl.Int64).to_list() == [seconds * SECOND for seconds, _ in rows], reference
        assert frame["value"].to_list() == pytest.approx([value for _, value in rows]), reference
    return epoch


def test_semantic_python_chain_repairs_the_discovered_outputs(tmp_path):
    client = aq.init(data_dir=tmp_path / "data", exact_only=True, timeout=180)
    try:
        # Register three probes and deploy tank means, the plant maximum, and an alarm.
        register(client)
        deploy(client)
        assert [
            len(client.inspect_app(name)["app"]["operators"])
            for name in (MEAN, HOTTEST, ALARM)
        ] == [2, 1, 1]
        client.insert_timeseries_batch(SOURCE, {
            "a1": [(SECOND, 20), (11 * SECOND, 22)],
            "a2": [(2 * SECOND, 40), (12 * SECOND, 36)],
            "b1": [(3 * SECOND, 24), (13 * SECOND, 25)],
        })
        initial = {
            MEAN_A: [(0, 30), (10, 29)],
            MEAN_B: [(0, 24), (10, 25)],
            HIGHEST: [(0, 30), (10, 29)],
            ALARM_STREAM: [(0, 30), (10, 29)],
        }
        first_epoch = assert_outputs(client, initial)

        # The calculated property is discoverable on tank A.
        query = client.query().entity(uri=TANK_A, alias="tank").measurement(app=MEAN, alias="mean")
        discovered = query.data().metadata(include_stream_uris=True)
        assert discovered["stream_uri"].unique().to_list() == [MEAN_A]
        selection = aq.Selection.from_query(query, measurement="mean", group="tank")
        assert selection.group != selection.stream

        # A late negative timestamp belongs to the preceding bucket. Correcting two
        # later readings retracts the alarm and leaves the original epoch readable.
        client.insert_timeseries_batch(SOURCE, {
            "a1": [(-SECOND // 2, 18), (SECOND // 2, 26)],
            "a2": [(2 * SECOND, 20), (12 * SECOND, 20)],
        })
        assert_outputs(client, {
            MEAN_A: [(-10, 18), (0, 22), (10, 21)],
            HIGHEST: [(-10, 18), (0, 24), (10, 25)],
            ALARM_STREAM: [],
        })
        assert_outputs(client, initial, epoch=first_epoch)

        # Deleting the only tank B sample in the first bucket removes its mean.
        client.wait(client.delete_timeseries(SOURCE, "b1", [3 * SECOND]))
        assert_outputs(client, {MEAN_B: [(10, 25)], HIGHEST: [(-10, 18), (0, 22), (10, 25)]})

        # Move probe a2 to tank B. Both tanks' historical means follow that change.
        client.sparql_update(
            f"DELETE DATA {{ <{TANK_A}> <http://data.ashrae.org/standard223#hasProperty> "
            "<urn:soft-sensors:probe-a2> }; "
            f"INSERT DATA {{ <{TANK_B}> <http://data.ashrae.org/standard223#hasProperty> "
            "<urn:soft-sensors:probe-a2> }",
            source_id="soft-sensor-layout",
        )
        assert_outputs(client, {
            MEAN_A: [(-10, 18), (0, 23), (10, 22)],
            MEAN_B: [(0, 20), (10, 22.5)],
            HIGHEST: [(-10, 18), (0, 23), (10, 22.5)],
            ALARM_STREAM: [],
        })
        # Updating the alarm parameter also evaluates previously published windows.
        client.deploy_app(declarations(threshold=22)[-1])
        assert_outputs(client, {ALARM_STREAM: [(0, 23), (10, 22.5)]})
    finally:
        aq.shutdown()


def test_rolling_app_repairs_overlapping_windows_through_restart(tmp_path):
    app = replace(declarations()[0], window=aq.Rolling(lookback=pd.Timedelta("10s"), step=pd.Timedelta("5s")))
    assert aq.App.from_dict(app.to_dict()).to_dict() == app.to_dict()
    client = aq.init(data_dir=tmp_path / "data", exact_only=True, timeout=180)
    try:
        # Three readings fall on the grid. The 10 s window includes both endpoints.
        register(client)
        assert client.deploy_app(app)["app"]["status"] == "active"
        client.insert_timeseries(SOURCE, "a1", [(0, 4), (5 * SECOND, 8), (10 * SECOND, 10)])
        assert_outputs(client, {MEAN_A: [(0, 4), (5, 6), (10, 22 / 3), (15, 9), (20, 10)]})

        # Correcting 5 s repairs three windows. A late reading at -5 s stops contributing after 5 s.
        client.insert_timeseries(SOURCE, "a1", [(5 * SECOND, 14), (-5 * SECOND, 2)])
        assert_outputs(client, {
            MEAN_A: [(-5, 2), (0, 3), (5, 20 / 3), (10, 28 / 3), (15, 12), (20, 10)],
        })

        # Deleting the last reading retracts 20 s while leaving the earlier windows intact.
        client.wait(client.delete_timeseries(SOURCE, "a1", [10 * SECOND]))
        remaining = [(-5, 2), (0, 3), (5, 20 / 3), (10, 9), (15, 14)]
        assert_outputs(client, {MEAN_A: remaining})
        plan = client.engine_snapshot()["plan"]
    finally:
        aq.shutdown()

    client = aq.init(data_dir=tmp_path / "data", exact_only=True, timeout=180)
    try:
        assert client.engine_snapshot()["plan"] == plan
        assert_outputs(client, {MEAN_A: remaining})
        client.insert_timeseries(SOURCE, "a1", [(5 * SECOND, 8)])
        assert_outputs(client, {MEAN_A: [(-5, 2), (0, 3), (5, 14 / 3), (10, 6), (15, 8)]})
    finally:
        aq.shutdown()


def test_native_warm_worker_failure_is_isolated_and_retryable(tmp_path, monkeypatch):
    # The worker imports this dependency once and can fail later without changing the app source.
    environment = tmp_path / "environment"
    environment.mkdir()
    pid_log = tmp_path / "workers.jsonl"
    failure = tmp_path / "fail-worker"
    (environment / "soft_sensor_test_dependency.py").write_text(
        "import json, os\nfrom pathlib import Path\n"
        f"with open({str(pid_log)!r}, 'a') as handle:\n"
        "    handle.write(json.dumps([os.getpid(), os.getppid()]) + '\\n')\n"
        "def check():\n"
        f"    if Path({str(failure)!r}).exists():\n"
        "        raise RuntimeError('test dependency unavailable')\n"
    )
    monkeypatch.setenv("PYTHONPATH", str(environment) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    module = tmp_path / "calculations.py"
    module.write_text(
        "import soft_sensor_test_dependency\n"
        + (DIRECTORY / "transforms.py").read_text().replace(
            "def tank_mean(frame, context):",
            "def tank_mean(frame, context):\n    soft_sensor_test_dependency.check()",
        )
    )
    config = tmp_path / "acquirium.toml"
    config.write_text('[server]\ndata_dir = "data"\nengine_workers = 1\n')
    client = aq.init(config, exact_only=True, timeout=180)
    try:
        register(client)
        deploy(client, module=module)
        client.insert_timeseries_batch(SOURCE, {
            "a1": [(SECOND, 20)], "a2": [(2 * SECOND, 40)], "b1": [(3 * SECOND, 24)],
        })
        before = {MEAN_A: [(0, 30)], MEAN_B: [(0, 24)], HIGHEST: [(0, 30)], ALARM_STREAM: [(0, 30)]}
        epoch = assert_outputs(client, before)
        workers = [json.loads(line) for line in pid_log.read_text().splitlines()]
        assert len({pid for pid, _ in workers}) == 1
        assert all(pid != os.getpid() and parent != os.getpid() and pid != parent for pid, parent in workers)

        # A correction publishes while the warm calculation fails and leaves derived values stale.
        failure.touch()
        correction = [(2 * SECOND, 20)]
        receipt, = client.insert_timeseries(SOURCE, "a2", correction, request_id="blocked")["receipts"]
        assert receipt["accepted"] and receipt["published"]
        status = client.engine_status()
        snapshot = client.engine_snapshot()
        assert status["accepted"] == status["applied"]
        assert status["published"] == snapshot["epoch"] > epoch
        assert status["pending"] == 0 and status["fault"] is None
        assert any("test dependency unavailable" in item.get("message", "") for item in snapshot["failures"].values())
        assert_outputs(client, before)
        assert client.stream(SOURCE, "a2").dataframe()["value"].to_list() == [20]
        assert client.inspect_app(MEAN)["app"]["status"] == "failed"
        assert client.inspect_app(ALARM)["app"]["status"] == "blocked"

        again, = client.insert_timeseries(SOURCE, "a2", correction, request_id="blocked")["receipts"]
        assert again["sequence"] == receipt["sequence"]

        # Restoring the dependency repairs the pending windows without another reading.
        failure.unlink()
        client.retry_engine()
        deadline = time.monotonic() + 30
        while client.engine_snapshot()["failures"]:
            assert time.monotonic() < deadline, "soft sensor recovery timed out"
            time.sleep(0.05)
        assert_outputs(client, {
            MEAN_A: [(0, 20)], MEAN_B: [(0, 24)], HIGHEST: [(0, 24)], ALARM_STREAM: [],
        })
    finally:
        failure.unlink(missing_ok=True)
        aq.shutdown()
