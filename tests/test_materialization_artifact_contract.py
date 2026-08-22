"""Shared durable artifact lifecycle contract for materialization backends."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pyarrow as pa

from acquirium.Materialization.impact import TimeRange
from acquirium.Materialization.state import ArtifactCandidate, ArtifactRequest
from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.executor import LocalExecutorPool
from acquirium.Materialization.scheduler import MaterializationScheduler
from acquirium.Storage.publication.postgres import PublicationPostgres
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest
from acquirium.Storage.artifacts import FilesystemArtifactStore
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.postgres import MaterializationPostgres
from unit.test_materialization_primitives import DurableOffsetTransformation


@pytest.fixture(params=["duckdb", "postgres"])
def artifact_runtime(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "artifact-contract.duckdb", recreate=True)
        try:
            yield MaterializationDuckDB(store)
        finally:
            store.close()
    else:
        try:
            runtime = MaterializationPostgres(pg_dsn)
        except Exception as error:
            pytest.skip(f"PostgreSQL unavailable: {error}")
        try:
            yield runtime
        finally:
            runtime.close()


def test_artifact_request_completion_retry_and_promotion(artifact_runtime, tmp_path):
    marker = uuid4().hex
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    request = ArtifactRequest(f"request:{marker}", "calibration", f"deployment:{marker}",
                              f"binding:{marker}", {"urn:input": 1},
                              TimeRange(start, start + timedelta(minutes=1)))
    assert artifact_runtime.create_artifact_request(request) == request.request_id
    lease = artifact_runtime.lease_artifact_request("contract-worker")
    assert lease is not None
    candidate = ArtifactCandidate(b"artifact contract", metrics={"score": 1.0})
    record = artifacts.put(candidate.data)
    revision = artifact_runtime.complete_artifact_request(lease, record, candidate)
    assert artifact_runtime.complete_artifact_request(lease, record, candidate).revision_id == revision.revision_id
    assert artifact_runtime.promote_state_revision(revision.revision_id).status == "active"
    # The binding is intentionally absent in this lifecycle-only contract;
    # both backends must still persist and safely inspect a recompute request.
    artifact_runtime.promote_state_revision(revision.revision_id, policy="recompute_all")
    assert artifact_runtime.pending_state_invalidations() == ()


def test_failed_artifact_production_retries_without_losing_active_revision(artifact_runtime, tmp_path):
    marker = uuid4().hex
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = ArtifactRequest(f"active:{marker}", "calibration", f"deployment:{marker}",
                            f"binding:{marker}", {}, TimeRange(start, start + timedelta(minutes=1)),
                            metadata={"revision": "active"})
    artifact_runtime.create_artifact_request(first)
    initial_lease = artifact_runtime.lease_artifact_request("producer")
    initial_candidate = ArtifactCandidate(b"known-good")
    active = artifact_runtime.complete_artifact_request(
        initial_lease, artifacts.put(initial_candidate.data), initial_candidate
    )
    artifact_runtime.promote_state_revision(active.revision_id)

    retry = ArtifactRequest(f"retry:{marker}", "calibration", f"deployment:{marker}",
                            f"binding:{marker}", {}, TimeRange(start, start + timedelta(minutes=1)),
                            previous_revision=active.revision_id, metadata={"revision": "retry"})
    artifact_runtime.create_artifact_request(retry)
    failed_lease = artifact_runtime.lease_artifact_request("producer")
    artifact_runtime.fail_artifact_request(failed_lease, {"message": "producer lost"})
    retry_lease = artifact_runtime.lease_artifact_request("replacement-producer")
    assert retry_lease is not None and retry_lease.attempt == failed_lease.attempt + 1
    retry_candidate = ArtifactCandidate(b"replacement")
    candidate = artifact_runtime.complete_artifact_request(
        retry_lease, artifacts.put(retry_candidate.data), retry_candidate
    )
    assert candidate.parent_revision == active.revision_id
    assert artifact_runtime.active_state_revision(first.binding_id).revision_id == active.revision_id
    assert artifacts.get(active.artifact.digest) == b"known-good"
    artifact_runtime.promote_state_revision(candidate.revision_id)
    assert artifact_runtime.active_state_revision(first.binding_id).revision_id == candidate.revision_id
    assert artifact_runtime.state_revision(active.revision_id).status == "retired"


def test_postgres_stateful_transform_recovers_after_runtime_restart(pg_dsn, tmp_path):
    """A PostgreSQL reopen and fresh executor reproduce a pinned artifact revision."""
    marker = uuid4().hex
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    input_ref, output_ref = f"urn:state:{marker}:in", f"urn:state:{marker}:out"
    deployment = f"stateful-{marker}"
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        continuous = PublicationPostgres(pg_dsn)
        runtime = MaterializationPostgres(pg_dsn)
    except Exception as error:
        pytest.skip(f"PostgreSQL unavailable: {error}")
    try:
        continuous.publish(PublicationRequest(f"first:{marker}", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": input_ref, "ts": start,
             "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        definition = DurableOffsetTransformation.__acquirium_definition__
        definition_id = runtime.register_definition(definition)
        generation = runtime.deploy(deployment, definition_id, graph_revision=1)
        binding = BindingSpec(f"one-{marker}", {"input": (input_ref,)}, {"output": (output_ref,)},
                              {"output_ref": output_ref})
        binding_id = binding.binding_id(definition_id)
        runtime.persist_bindings(deployment, generation, 1, definition_id, (binding,))
        runtime.activate_bindings(deployment, generation)
        runtime.set_deployment_status(deployment, "active")
        request = ArtifactRequest(f"artifact:{marker}", "calibration", deployment, binding_id,
                                  {input_ref: 1}, TimeRange(start, start + timedelta(minutes=2)))
        runtime.create_artifact_request(request)
        lease = runtime.lease_artifact_request("producer")
        candidate = ArtifactCandidate(f"2:{marker}".encode())
        revision = runtime.complete_artifact_request(lease, artifacts.put(candidate.data), candidate)
        runtime.promote_state_revision(revision.revision_id)
        runtime.create_plan(binding_id=binding_id, generation=generation, graph_revision=1,
            input_vector={input_ref: 1}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        executor = LocalExecutorPool(workers=1)
        try:
            assert MaterializationScheduler(runtime).run_next_registered("worker-a", executor=executor,
                                                                          deployment_name=deployment)
        finally:
            executor.close()
    finally:
        runtime.close()
        continuous.close()

    continuous = PublicationPostgres(pg_dsn)
    runtime = MaterializationPostgres(pg_dsn)
    try:
        continuous.publish(PublicationRequest(f"second:{marker}", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": input_ref, "ts": start + timedelta(minutes=1),
             "numeric_value": 5.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime.create_plan(binding_id=binding_id, generation=generation, graph_revision=1,
            input_vector={input_ref: 2}, ranges=(TimeRange(start + timedelta(minutes=1), start + timedelta(minutes=2)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        executor = LocalExecutorPool(workers=1)
        try:
            assert MaterializationScheduler(runtime).run_next_registered("worker-b", executor=executor,
                                                                          deployment_name=deployment)
        finally:
            executor.close()
        with runtime._pool.connection() as conn:
            values = conn.execute("SELECT numeric_value FROM timeseries WHERE ref_uri = %s AND NOT deleted ORDER BY ts", [output_ref]).fetchall()
            receipts = conn.execute("SELECT DISTINCT state_revision FROM materialization_execution_receipts "
                                    "WHERE partition_id IN (SELECT part.partition_id FROM materialization_plan_partitions part "
                                    "JOIN materialization_plans plan ON plan.plan_id = part.plan_id WHERE plan.binding_id = %s)",
                                    [binding_id]).fetchall()
        assert values == [(4.0,), (8.0,)]
        assert receipts == [(revision.revision_id,)]
        replacement_request = ArtifactRequest(f"replacement:{marker}", "calibration", deployment, binding_id,
            {input_ref: 2}, TimeRange(start, start + timedelta(minutes=2)), previous_revision=revision.revision_id,
            metadata={"revision": "replacement"})
        runtime.create_artifact_request(replacement_request)
        replacement_lease = runtime.lease_artifact_request("producer")
        replacement_candidate = ArtifactCandidate(f"3:{marker}".encode())
        replacement = runtime.complete_artifact_request(
            replacement_lease, artifacts.put(replacement_candidate.data), replacement_candidate
        )
        runtime.promote_state_revision(replacement.revision_id, policy="recompute_from",
                                       effective_from=start + timedelta(minutes=1))
        plans = MaterializationScheduler(runtime).plan_state_invalidations(maximum_partition_duration=timedelta(hours=1))
        assert len(plans) == 1
        with runtime._pool.connection() as conn:
            begin, end = conn.execute("SELECT start_ts, end_ts FROM materialization_plan_partitions WHERE plan_id = %s", [plans[0]]).fetchone()
        assert begin == start + timedelta(minutes=1)
        assert end == start + timedelta(minutes=1, microseconds=1)
    finally:
        runtime.close()
        continuous.close()
