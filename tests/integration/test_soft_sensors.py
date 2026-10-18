"""Real server, semantic discovery, and native Python subprocesses; no Docker."""

import json
import os
from collections import defaultdict
from statistics import mean

import polars as pl
import pytest

import acquirium as aq
from acquirium.apps import output_reference
from acquirium.internals.models import compute_ref_uri
from examples.soft_sensors.declarations import (
    ALARM,
    BUCKET_NS,
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

SECOND = 1_000_000_000


def recompute(observations, membership, *, threshold=27, hottest_offset=0):
    """Independent whole-history calculation, with no native function calls."""
    buckets = defaultdict(list)
    for probe, readings in observations.items():
        for timestamp, value in readings.items():
            buckets[membership[probe], timestamp // BUCKET_NS * BUCKET_NS].append(value)
    means = {
        tank: {
            timestamp: mean(values)
            for (group, timestamp), values in buckets.items()
            if group == tank
        }
        for tank in (TANK_A, TANK_B)
    }
    timestamps = set().union(*(values.keys() for values in means.values()))
    hottest = {
        timestamp: max(
            values[timestamp] for values in means.values() if timestamp in values
        )
        + hottest_offset
        for timestamp in timestamps
    }
    return {
        output_reference(MEAN, tank, "mean"): values for tank, values in means.items()
    } | {
        output_reference(HOTTEST, PLANT, "hottest"): hottest,
        output_reference(ALARM, PLANT, "alarm"): {
            timestamp: value
            for timestamp, value in hottest.items()
            if value > threshold
        },
    }


def assert_outputs(client, expected, *, epoch=None):
    epoch = client.engine_snapshot()["epoch"] if epoch is None else epoch
    for reference, values in expected.items():
        frame = client.client.timeseries_df(reference, epoch=epoch)
        actual = dict(zip(frame["ts"].cast(pl.Int64), frame["value"]))
        assert actual.keys() == values.keys(), (reference, actual, values)
        for timestamp, value in values.items():
            assert actual[timestamp] == pytest.approx(value), (reference, timestamp)
    return epoch


@pytest.mark.integration
def test_semantic_python_chain_matches_full_recomputation_and_restart(tmp_path):
    module = tmp_path / "calculations.py"
    source = (DIRECTORY / "transforms.py").read_text()
    module.write_text(source)
    client = aq.init(data_dir=tmp_path / "data", exact_only=True, timeout=180)
    observations = {probe: {} for probe in ("a1", "a2", "b1")}
    membership = {"a1": TANK_A, "a2": TANK_A, "b1": TANK_B}
    try:
        register(client)
        deploy(client, module=module)
        assert [
            len(client.inspect_app(name)["app"]["operators"])
            for name in (MEAN, HOTTEST, ALARM)
        ] == [2, 1, 1]

        def submit(request_id, readings):
            for probe, rows in readings.items():
                observations[probe].update(rows)
            response = client.insert_timeseries_batch(
                SOURCE, readings, request_id=request_id
            )
            client.wait(response["receipts"][0])
            return assert_outputs(client, recompute(observations, membership))

        initial = {
            "a1": [(SECOND, 20), (11 * SECOND, 22)],
            "a2": [(2 * SECOND, 40), (12 * SECOND, 36)],
            "b1": [(3 * SECOND, 24), (13 * SECOND, 25)],
        }
        initial_epoch = submit("initial", initial)
        initial_result = recompute(observations, membership)
        # Repeated delivery changes neither source nor derived results.
        response = client.insert_timeseries_batch(SOURCE, initial, request_id="initial")
        assert client.wait(response["receipts"][0])["epoch"] == initial_epoch
        query = (
            client.query()
            .entity(uri=TANK_A, alias="tank")
            .measurement(app=MEAN, alias="mean")
        )
        discovered = query.data().metadata(include_ref_uris=True)
        assert discovered["ref_uri"].unique().to_list() == [
            output_reference(MEAN, TANK_A, "mean")
        ]
        # The query DSL also compiles the group attachment for a downstream app.
        selection = aq.Selection.from_query(query, alias="mean", group="tank")
        assert selection.group != selection.stream

        submit(
            "late",
            {"a1": [(-SECOND // 2, 18), (SECOND // 2, 26)], "b1": [(10 * SECOND, 28)]},
        )
        submit("correction", {"a2": [(2 * SECOND, 20), (12 * SECOND, 20)]})
        assert_outputs(client, initial_result, epoch=initial_epoch)
        assert client.query().measurement(app=ALARM).dataframe().is_empty()

        receipt = client.delete_timeseries(
            SOURCE, "b1", [3 * SECOND], request_id="delete"
        )
        client.wait(receipt)
        del observations["b1"][3 * SECOND]
        assert_outputs(client, recompute(observations, membership))

        client.sparql_update(
            f"DELETE DATA {{ <{TANK_A}> <http://data.ashrae.org/standard223#hasProperty> "
            "<urn:soft-sensors:probe-a2> }; "
            f"INSERT DATA {{ <{TANK_B}> <http://data.ashrae.org/standard223#hasProperty> "
            "<urn:soft-sensors:probe-a2> }",
            source_id="soft-sensor-layout",
        )
        membership["a2"] = TANK_B
        assert_outputs(client, recompute(observations, membership))

        # Frozen parameters change membership/context, repairing retained results.
        client.deploy_app(declarations(threshold=22, module=module)[-1])
        assert_outputs(client, recompute(observations, membership, threshold=22))
        # New pinned calculation bytes create new versions and rebuild history.
        module = tmp_path / "calculations-v2.py"
        module.write_text(
            source.replace(
                'value = frame["value"].max()',
                'value = frame["value"].max()\n    if value is not None:\n        value += 1',
            )
        )
        deploy(client, threshold=22, module=module)
        expected = recompute(observations, membership, threshold=22, hottest_offset=1)
        final_epoch = assert_outputs(client, expected)
        final_plan = client.engine_snapshot()["plan"]
    finally:
        aq.shutdown()

    client = aq.init(data_dir=tmp_path / "data", exact_only=True, timeout=180)
    try:
        assert client.engine_snapshot()["plan"] == final_plan
        assert client.engine_snapshot()["epoch"] == final_epoch
        assert_outputs(client, expected)
        response = client.insert_timeseries(
            SOURCE, "b1", [(13 * SECOND, 50)], request_id="after-restart"
        )
        client.wait(response["receipts"][0])
        observations["b1"][13 * SECOND] = 50
        assert_outputs(
            client, recompute(observations, membership, threshold=22, hottest_offset=1)
        )
        assert not (tmp_path / "data" / "timeseries.duckdb").exists()
    finally:
        aq.shutdown()


@pytest.mark.integration
def test_native_warm_worker_failure_is_atomic_and_retryable(tmp_path, monkeypatch):
    # Test-only dependency instrumentation records module initialization and
    # injects a runtime failure into an already-warm worker. Production functions
    # keep dependencies immutable and do not read external failure flags.
    environment = tmp_path / "environment"
    environment.mkdir()
    helper = environment / "soft_sensor_test_dependency.py"
    pid_log = tmp_path / "workers.jsonl"
    failure = tmp_path / "fail-worker"
    dependency = (
        "import json, os\nfrom pathlib import Path\n"
        f"with open({str(pid_log)!r}, 'a') as handle:\n"
        "    handle.write(json.dumps([os.getpid(), os.getppid()]) + '\\n')\n"
        "def check():\n"
        f"    if Path({str(failure)!r}).exists():\n"
        "        raise RuntimeError('test dependency unavailable')\n"
    )
    helper.write_text(dependency)
    monkeypatch.setenv(
        "PYTHONPATH", str(environment) + os.pathsep + os.environ.get("PYTHONPATH", "")
    )
    module = tmp_path / "calculations.py"
    module.write_text(
        "import soft_sensor_test_dependency\n"
        + (DIRECTORY / "transforms.py")
        .read_text()
        .replace(
            "def tank_mean(frame, context):",
            "def tank_mean(frame, context):\n    soft_sensor_test_dependency.check()",
        )
    )
    config = tmp_path / "acquirium.toml"
    config.write_text(
        '[server]\ndata_dir = "data"\nengine_workers = 2\npython_pool_size = 1\n'
        "python_max_worker_jobs = 100\npython_max_worker_age = 300.0\n"
    )
    client = aq.init(config, exact_only=True, timeout=180)
    try:
        register(client)
        deploy(client, module=module)
        response = client.insert_timeseries_batch(
            SOURCE,
            {"a1": [(SECOND, 20)], "a2": [(2 * SECOND, 40)], "b1": [(3 * SECOND, 24)]},
            request_id="initial",
        )
        snapshot = client.wait(response["receipts"][0])
        original = {"a1": {SECOND: 20}, "a2": {2 * SECOND: 40}, "b1": {3 * SECOND: 24}}
        membership = {"a1": TANK_A, "a2": TANK_A, "b1": TANK_B}
        assert_outputs(client, recompute(original, membership))
        workers = [json.loads(line) for line in pid_log.read_text().splitlines()]
        original_pids = {pid for pid, parent in workers}
        assert len(original_pids) == 1
        stats = client.engine_status()["python_workers"]
        assert stats["spawned"] == len(original_pids)
        assert stats["resident"] == stats["peak_active"] == 1
        assert stats["reused"] >= 3
        assert all(
            pid != os.getpid() and parent != os.getpid() and pid != parent
            for pid, parent in workers
        )

        failure.touch()
        response = client.insert_timeseries(
            SOURCE, "a2", [(2 * SECOND, 20)], request_id="blocked"
        )
        receipt = response["receipts"][0]
        assert receipt["accepted"] and not receipt["published"]
        status = client.engine_status()
        assert "test dependency unavailable" in status["error"]
        assert status["python_workers"]["resident"] == 0
        assert status["engine"]["accepted"] > status["engine"]["applied"]
        assert client.engine_snapshot()["epoch"] == snapshot["epoch"]
        assert_outputs(client, recompute(original, membership))
        raw_reference = str(compute_ref_uri(SOURCE, "a2"))
        assert client.client.timeseries_df(raw_reference)["value"].to_list() == [40]
        with pytest.raises(RuntimeError, match="blocked"):
            client.wait(receipt)
        # Redelivery has the same receipt; it cannot publish the failed prefix.
        again = client.insert_timeseries(
            SOURCE, "a2", [(2 * SECOND, 20)], request_id="blocked"
        )
        assert again["receipts"][0]["sequence"] == receipt["sequence"]

        failure.unlink()
        client.retry_engine()
        client.wait(receipt)
        original["a2"][2 * SECOND] = 20
        assert_outputs(client, recompute(original, membership))
        assert client.client.timeseries_df(raw_reference)["value"].to_list() == [20]
        assert client.engine_status()["error"] is None
        final_pids = {json.loads(line)[0] for line in pid_log.read_text().splitlines()}
        assert final_pids - original_pids
        assert client.query().measurement(app=ALARM).dataframe().is_empty()
    finally:
        failure.unlink(missing_ok=True)
        aq.shutdown()
