"""Shared durable experiment lifecycle contract."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from acquirium.Materialization.experiments import ExperimentRunRequest, ExperimentRunner
from acquirium.Materialization.api import Experiment
from acquirium.Materialization.executor import LocalExecutorPool
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.postgres import MaterializationPostgres


class ContractExperiment(Experiment):
    parameters_schema = {"type": "object", "required": ["limit"]}

    def run(self, context):
        context.metric("objective", {"limit": context.params["limit"]})
        return context.output_ref("schedule")


@pytest.fixture(params=["duckdb", "postgres"])
def experiment_store(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "experiments.duckdb", recreate=True)
        try: yield MaterializationDuckDB(store)
        finally: store.close()
    else:
        try: runtime = MaterializationPostgres(pg_dsn)
        except Exception as error: pytest.skip(f"PostgreSQL unavailable: {error}")
        try: yield runtime
        finally: runtime.close()


def test_experiment_snapshot_lifecycle_and_retention(experiment_store):
    marker = uuid4().hex
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    request = ExperimentRunRequest(f"run:{marker}", f"definition:{marker}", 7,
        TimeRange(start, start + timedelta(hours=1)), {"limit": 4},
        {"type": "object", "properties": {"limit": {"type": "integer"}}, "required": ["limit"]},
        {"scenario": {"name": "summer", "options": [1, 2]}}, {"urn:input": 9},
        ({"binding_id": "b", "metadata": {"zone": "A"}},))
    started = experiment_store.start_experiment(request)
    assert started.status == "running" and started.binding_snapshot == list(request.binding_snapshot)
    # The record is a JSON snapshot, not a retained pointer to caller or graph data.
    request.metadata["scenario"]["name"] = "changed-after-start"
    request.binding_snapshot[0]["metadata"]["zone"] = "B"
    frozen = experiment_store.experiment_run(request.run_id)
    assert frozen.metadata["scenario"]["name"] == "summer"
    assert frozen.binding_snapshot[0]["metadata"]["zone"] == "A"
    experiment_store.record_experiment_metric(request.run_id, "objective", {"cost": 1.25})
    assert experiment_store.experiment_metrics(request.run_id) == {"objective": {"cost": 1.25}}
    assert experiment_store.declare_experiment_output(request.run_id, "schedule") == f"urn:acquirium:experiment:{request.run_id}:output:schedule"
    assert experiment_store.finish_experiment(request.run_id, status="succeeded").status == "succeeded"
    assert request.run_id in [run.run_id for run in experiment_store.list_experiments(metadata={"scenario": {"name": "summer", "options": [1, 2]}})]
    replay = experiment_store.rerun_experiment(request.run_id, f"replay:{marker}")
    assert replay.params == frozen.params and replay.binding_snapshot == frozen.binding_snapshot
    assert experiment_store.collect_experiment(request.run_id).status == "collected"
    assert experiment_store.declare_experiment_output(replay.run_id, "schedule") != f"urn:acquirium:experiment:{request.run_id}:output:schedule"


def test_kept_experiment_cannot_be_collected_and_params_are_validated(experiment_store):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="parameters"):
        ExperimentRunRequest("bad", "definition", 1, TimeRange(start, start + timedelta(seconds=1)),
            {"limit": "wrong"}, {"properties": {"limit": {"type": "integer"}}})
    request = ExperimentRunRequest(f"keep:{uuid4().hex}", "definition", 1,
        TimeRange(start, start + timedelta(seconds=1)), {}, {})
    experiment_store.start_experiment(request)
    experiment_store.keep_experiment(request.run_id, "comparison baseline")
    with pytest.raises(ValueError, match="kept"):
        experiment_store.collect_experiment(request.run_id)


def test_failed_and_cancelled_runs_are_durable(experiment_store):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for status in ("failed", "cancelled"):
        request = ExperimentRunRequest(f"{status}:{uuid4().hex}", "definition", 1,
            TimeRange(start, start + timedelta(seconds=1)), {}, {})
        experiment_store.start_experiment(request)
        run = experiment_store.finish_experiment(request.run_id, status=status,
            error={"message": "boom"} if status == "failed" else None)
        assert run.status == status
        assert run.error == ({"message": "boom"} if status == "failed" else None)


def test_experiment_declaration_is_an_immutable_experiment_definition():
    class Optimize(Experiment):
        parameters_schema = {"type": "object", "required": ["limit"]}

        def run(self, run):
            return run.params["limit"]

    definition = Optimize.__acquirium_definition__
    assert definition.kind == "experiment"
    assert definition.parameters_schema == {"type": "object", "required": ["limit"]}


def test_experiment_runner_uses_frozen_definition_and_bounded_pool(experiment_store):
    definition_id = experiment_store.register_definition(ContractExperiment.__acquirium_definition__)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    request = ExperimentRunRequest(f"execute:{uuid4().hex}", definition_id, 3,
        TimeRange(start, start + timedelta(seconds=1)), {"limit": 9},
        {"type": "object", "required": ["limit"]})
    experiment_store.start_experiment(request)
    executor = LocalExecutorPool(workers=1)
    try:
        result = ExperimentRunner(experiment_store, executor).run(request.run_id)
    finally:
        executor.close()
    assert result == f"urn:acquirium:experiment:{request.run_id}:output:schedule"
    assert experiment_store.experiment_run(request.run_id).status == "succeeded"


def test_running_experiment_has_one_durable_execution_claim(experiment_store):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    request = ExperimentRunRequest(f"claim:{uuid4().hex}", "definition", 1,
        TimeRange(start, start + timedelta(seconds=1)), {}, {})
    experiment_store.start_experiment(request)

    assert experiment_store.claim_experiment_execution(request.run_id, "worker-a") is True
    assert experiment_store.claim_experiment_execution(request.run_id, "worker-b") is False
    with pytest.raises(ValueError, match="stale"):
        experiment_store.finish_experiment(
            request.run_id, status="succeeded",
            execution_claim="worker-b",
        )
    with pytest.raises(ValueError, match="terminal"):
        experiment_store.collect_experiment(request.run_id)
