from datetime import datetime, timedelta, timezone
import threading
from acquirium.Materialization.bindings import BindingSpec, diff_bindings, validate_binding_topology
from acquirium.Materialization.impact import TimeRange, coalesce_ranges, full_history, lookback, window
from acquirium.Storage.materialization.ids import normalize_change_ranges
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.duckdb import StaleAttemptError
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest
from acquirium.Storage.duckdb_store import DuckDBStore
import pyarrow as pa
from acquirium.Materialization.definitions import definition_for
from acquirium.Materialization.impact import pointwise
from acquirium.Server.manager import Manager
from acquirium.Materialization.context import ComputeRequest, TransformContext
from acquirium.Materialization.api import StatefulTransformation, stateful
from acquirium.Materialization.executor import LocalExecutorPool
from acquirium.Materialization.compute import PythonArrowAdapter
from acquirium.Materialization.validation import OutputValidationError
from acquirium.Materialization.worker import DefinitionCache
from acquirium.Materialization.state import ArtifactCandidate, ArtifactRequest
from acquirium.Materialization.experiments import ExperimentRunRequest
from acquirium.Storage.materialization.types import MAX_PARTITION_ATTEMPTS
from acquirium.Storage.artifacts import FilesystemArtifactStore
from acquirium.Materialization.scheduler import MaterializationScheduler
from acquirium.Materialization.rebinding import MaterializationRebinder, resolve_bindings
UTC = timezone.utc


@stateful(inputs="input", outputs={"mode": "per_input"})
class DurableOffsetTransformation(StatefulTransformation):
    """Importable fixture proving persisted artifacts, not class memory, drive output."""
    setup_calls = 0
    load_calls = 0

    def setup_worker(self):
        type(self).setup_calls += 1
        return object()

    def load_artifact(self, artifact, worker):
        type(self).load_calls += 1
        return {"offset": float(artifact.decode().split(":", 1)[0]), "uses": 0}

    def transform(self, batch, state, context):
        state["uses"] += 1  # A worker-local mutation must not become durable state.
        return pa.table({"ref_uri": [context.metadata["output_ref"]] * batch.num_rows,
                         "ts": batch.column("ts"),
                         "numeric_value": [value + state["offset"] + state["uses"]
                                           for value in batch.column("numeric_value").to_pylist()],
                         "text_value": [None] * batch.num_rows})

def test_ranges_are_half_open_and_adjacent_ranges_coalesce():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    assert coalesce_ranges((TimeRange(start, start + timedelta(seconds=1)), TimeRange(start + timedelta(seconds=1), start + timedelta(seconds=2)))) == (TimeRange(start, start + timedelta(seconds=2)),)

def test_impact_expands_exact_boundaries():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    changed = TimeRange(start, start + timedelta(seconds=1))
    assert lookback(timedelta(minutes=5)).affected(changed) == TimeRange(start, start + timedelta(minutes=5, seconds=1))
    assert window(before=timedelta(minutes=2), after=timedelta(minutes=3)).affected(changed) == TimeRange(start - timedelta(minutes=3), start + timedelta(minutes=2, seconds=1))

def test_binding_identity_is_stable_but_content_digest_changes():
    first = BindingSpec("ahu-1", {"temperature": ("in",)}, {"out": ("out",)}, {"unit": "K"})
    second = BindingSpec("ahu-1", {"temperature": ("in",)}, {"out": ("out",)}, {"unit": "Cel"})
    assert first.binding_id("definition") == second.binding_id("definition")
    assert first.content_digest != second.content_digest

def test_binding_diff_and_topology_validation():
    first = BindingSpec("a", {"in": ("source",)}, {"out": ("derived-a",)})
    changed = BindingSpec("a", {"in": ("source",)}, {"out": ("derived-a",)}, {"unit": "Cel"})
    second = BindingSpec("b", {"in": ("derived-a",)}, {"out": ("derived-b",)})
    diff = diff_bindings("definition", (first,), (changed, second))
    assert diff.changed == (changed,)
    assert diff.added == (second,)
    validate_binding_topology((first, second), definition_id="definition")
    cyclic_first = BindingSpec("cycle-a", {"in": ("cycle-b",)}, {"out": ("cycle-a",)})
    cyclic_second = BindingSpec("cycle-b", {"in": ("cycle-a",)}, {"out": ("cycle-b",)})
    import pytest
    with pytest.raises(ValueError, match="cycle"):
        validate_binding_topology((cyclic_first, cyclic_second), definition_id="definition")

def test_range_manifests_bucket_and_coalesce_changes():
    start = datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)
    manifests = normalize_change_ranges(publication_id="p1", stream_versions={"input": 3}, changes=(("input", start, "upsert"), ("input", start + timedelta(seconds=30), "delete")))
    assert len(manifests) == 1
    assert manifests[0].change_kind == "mixed"
    assert manifests[0].interval == TimeRange(start.replace(second=0, microsecond=0), start.replace(second=0, microsecond=0) + timedelta(minutes=1))

def test_canonical_duckdb_publication_dual_writes_range_manifest(tmp_path):
    store = DuckDBStore(tmp_path / "ranges.duckdb", recreate=True)
    try:
        timestamp = datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)
        mutations = pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:input", "ts": timestamp, "numeric_value": 1.0, "text_value": None},
            {"operation": "delete", "ref_uri": "urn:input", "ts": timestamp + timedelta(seconds=30), "numeric_value": None, "text_value": None},
        ], schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("range-publication", mutations))
        ranges = MaterializationDuckDB(store).change_ranges("urn:input", after_version=0, through_version=1)
        assert len(ranges) == 1
        assert ranges[0].change_kind == "mixed"
        assert ranges[0].row_count == 2
    finally:
        store.close()

def test_duckdb_persists_definition_and_staging_bindings(tmp_path):
    store = DuckDBStore(tmp_path / "definitions.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        def convert(value: float) -> float:
            return value - 273.15
        definition = definition_for(convert, inputs="temperature", outputs="celsius", impact=pointwise())
        definition_id = runtime.register_definition(definition)
        with store._own_conn() as conn:
            spec = conn.execute("SELECT spec_json FROM materialization_definitions").fetchone()[0]
        assert '"outputs": "celsius"' in spec
        generation = runtime.deploy("convert-temperature", definition_id, graph_revision=7)
        runtime.persist_bindings("convert-temperature", generation, 7, definition_id,
                                 (BindingSpec("sensor", {"input": ("urn:in",)}, {"output": ("urn:out",)}),))
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_bindings").fetchone() == ("staging",)
    finally:
        store.close()

def test_newer_rebind_supersedes_old_work_and_failure_preserves_staging(tmp_path):
    store = DuckDBStore(tmp_path / "rebinds.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        runtime.request_rebind("deployment", 1)
        runtime.request_rebind("deployment", 2)
        assert runtime.lease_rebind("worker") == ("deployment", 2)
        runtime.finish_rebind("deployment", 2, error={"message": "resolver failed"})
        with store._own_conn() as conn:
            rows = conn.execute("SELECT graph_revision, status FROM materialization_rebind_requests ORDER BY graph_revision").fetchall()
        assert rows == [(1, "superseded"), (2, "failed")]
        runtime.request_rebind("deployment", 2)
        assert runtime.lease_rebind("retry") == ("deployment", 2)
        runtime.finish_rebind("deployment", 2)
        runtime.request_rebind("deployment", 2, force=True)
        assert runtime.lease_rebind("forced") == ("deployment", 2)
    finally:
        store.close()

def test_completed_graph_publication_records_revision_and_queues_every_deployment():
    class Graph:
        def graph_status(self):
            return {"source_version": 9, "published_version": 8}
        def published_query_digest(self):
            return "published-digest"
    class Runtime:
        def __init__(self):
            self.revision = None
            self.requests = []
        def record_graph_revision(self, *args):
            self.revision = args
        def deployment_names(self):
            return ("one", "two")
        def services(self, status=None):
            return ()
        def request_rebind(self, *args):
            self.requests.append(args)
    manager = Manager.__new__(Manager)
    manager.graph_store, manager.materialization = Graph(), Runtime()
    manager._record_materialization_graph_revision()
    assert manager.materialization.revision == (8, 9, "published-digest")
    assert manager.materialization.requests == [("one", 8), ("two", 8)]

def test_plan_partitions_lease_retry_and_commit_idempotently(tmp_path):
    store = DuckDBStore(tmp_path / "plans.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        plan_id, partitions = runtime.create_plan(
            binding_id="binding", generation=1, graph_revision=1, input_vector={"urn:in": 4},
            ranges=(TimeRange(start, start + timedelta(minutes=5)),), reason={"kind": "data"},
            maximum_partition_duration=timedelta(minutes=2),
        )
        assert len(partitions) == 3
        leases = [runtime.lease_partition("worker") for _ in partitions]
        assert [lease.partition.plan_id for lease in leases if lease] == [plan_id] * 3
        assert runtime.commit_partition(leases[0], output_publication_id="output-1")
        assert not runtime.commit_partition(leases[0], output_publication_id="output-1")
        assert runtime.commit_partition(leases[1], output_publication_id="output-2")
        assert runtime.commit_partition(leases[2], output_publication_id="output-3")
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_plans WHERE plan_id = ?", [plan_id]).fetchone() == ("committed",)
    finally:
        store.close()

def test_expired_lease_is_retried_and_old_attempt_cannot_commit(tmp_path):
    store = DuckDBStore(tmp_path / "lease-expiry.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        runtime.create_plan(binding_id="binding", generation=1, graph_revision=1, input_vector={},
                            ranges=(TimeRange(start, start + timedelta(seconds=1)),), reason={},
                            maximum_partition_duration=timedelta(minutes=1))
        expired = runtime.lease_partition("first", duration=-timedelta(microseconds=1))
        retried = runtime.lease_partition("second")
        assert retried and retried.attempt == expired.attempt + 1
        import pytest
        with pytest.raises(ValueError, match="stale"):
            runtime.commit_partition(expired, output_publication_id="old")
        assert runtime.commit_partition(retried, output_publication_id="new")
    finally:
        store.close()

def test_partitioning_preserves_disjoint_and_adjacent_dirty_ranges(tmp_path):
    store = DuckDBStore(tmp_path / "partition-algebra.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        _, partitions = runtime.create_plan(binding_id="binding", generation=1, graph_revision=1, input_vector={},
            ranges=(TimeRange(start, start + timedelta(minutes=2)), TimeRange(start + timedelta(minutes=2), start + timedelta(minutes=4)), TimeRange(start + timedelta(minutes=10), start + timedelta(minutes=11))),
            reason={}, maximum_partition_duration=timedelta(minutes=1))
        intervals = [partition.interval for partition in partitions]
        assert intervals == [TimeRange(start + timedelta(minutes=index), start + timedelta(minutes=index + 1)) for index in (0, 1, 2, 3, 10)]
    finally:
        store.close()

def test_semantically_identical_plan_is_idempotent(tmp_path):
    store = DuckDBStore(tmp_path / "plan-idempotency.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        kwargs = dict(binding_id="binding", generation=1, graph_revision=1, input_vector={"input": 1},
                      ranges=(TimeRange(start, start + timedelta(minutes=2)),), reason={"kind": "tail"},
                      maximum_partition_duration=timedelta(minutes=1))
        assert runtime.create_plan(**kwargs) == runtime.create_plan(**kwargs)
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_plans").fetchone() == (1,)
            assert conn.execute("SELECT count(*) FROM materialization_plan_partitions").fetchone() == (2,)
    finally:
        store.close()

def test_snapshot_pins_live_arrow_rows_and_current_input_vector(tmp_path):
    store = DuckDBStore(tmp_path / "snapshot.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        mutations = pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:in", "ts": start, "numeric_value": 1.0, "text_value": None},
            {"operation": "delete", "ref_uri": "urn:in", "ts": start + timedelta(seconds=1), "numeric_value": None, "text_value": None},
        ], schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("snapshot-input", mutations))
        runtime = MaterializationDuckDB(store)
        _, partitions = runtime.create_plan(binding_id="binding", generation=1, graph_revision=1, input_vector={"urn:in": 1},
            ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
        lease = runtime.lease_partition("worker")
        snapshot = runtime.snapshot_partition(lease, ("urn:in",))
        assert snapshot.input_versions == {"urn:in": 1}
        assert snapshot.inputs.column("numeric_value").to_pylist() == [1.0]
        assert snapshot.inputs.schema == MUTATION_SCHEMA
    finally:
        store.close()

def test_scheduler_creates_coalesced_manifest_driven_plan(tmp_path):
    store = DuckDBStore(tmp_path / "scheduler.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        rows = pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:in", "ts": start, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:in", "ts": start + timedelta(seconds=30), "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("schedule-input", rows))
        runtime = MaterializationDuckDB(store)
        scheduler = MaterializationScheduler(runtime)
        plan, partitions = scheduler.create_plan_for_binding(binding_id="binding", generation=1, graph_revision=1,
            progress={"urn:in": 0}, heads={"urn:in": 1}, impact=pointwise(), reason={"kind": "tail"}, maximum_partition_duration=timedelta(minutes=1))
        assert plan and len(partitions) == 1
        assert partitions[0].interval == TimeRange(start.replace(second=0, microsecond=0), start.replace(second=0, microsecond=0) + timedelta(minutes=1))
    finally:
        store.close()

def test_scheduler_runs_one_partition_and_retries_failures(tmp_path):
    store = DuckDBStore(tmp_path / "scheduler-run.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        # Persist a binding so partition_refs derives ownership durably.
        definition = definition_for(lambda value: value, inputs="input", outputs="output", impact=pointwise())
        did = runtime.register_definition(definition); generation = runtime.deploy("run", did)
        binding = BindingSpec("one", {"input": ("urn:in",)}, {"output": ("urn:out",)})
        runtime.persist_bindings("run", generation, 1, did, (binding,))
        runtime.create_plan(binding_id=binding.binding_id(did), generation=generation, graph_revision=1, input_vector={}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
        scheduler = MaterializationScheduler(runtime)
        import pytest
        with pytest.raises(RuntimeError):
            scheduler.run_once("worker", lambda snapshot, outputs: (_ for _ in ()).throw(RuntimeError("bad transform")))
        assert scheduler.run_once("worker", lambda snapshot, outputs: pa.table({"ref_uri": [], "ts": pa.array([], type=pa.timestamp("us", tz="UTC")), "numeric_value": [], "text_value": []}))
    finally:
        store.close()

def test_scheduler_discovers_progress_lag_from_durable_binding_state(tmp_path):
    store = DuckDBStore(tmp_path / "discover.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("discover-input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:discover:in", "ts": start, "numeric_value": 1.0, "text_value": None}], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(lambda value: value, inputs="in", outputs="out", impact=pointwise()); did = runtime.register_definition(definition); gen = runtime.deploy("discover", did)
        binding = BindingSpec("one", {"input": ("urn:discover:in",)}, {"output": ("urn:discover:out",)})
        runtime.persist_bindings("discover", gen, 1, did, (binding,))
        plans = MaterializationScheduler(runtime).discover_and_plan(impact=pointwise(), maximum_partition_duration=timedelta(minutes=1))
        assert len(plans) == 1
    finally:
        store.close()

def test_registered_entrypoint_executes_through_cached_local_pool(tmp_path):
    store = DuckDBStore(tmp_path / "registered-execution.duckdb", recreate=True)
    pool = LocalExecutorPool(workers=1)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("registered-input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:registered:in", "ts": start, "numeric_value": 2.0, "text_value": None}], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        did = runtime.register_definition(definition); generation = runtime.deploy("registered", did)
        binding = BindingSpec("one", {"input": ("urn:registered:in",)}, {"output": ("urn:registered:out",)})
        runtime.persist_bindings("registered", generation, 1, did, (binding,))
        MaterializationScheduler(runtime).create_plan_for_binding(binding_id=binding.binding_id(did), generation=generation, graph_revision=1,
            progress={"urn:registered:in": 0}, heads={"urn:registered:in": 1}, impact=pointwise(), reason={}, maximum_partition_duration=timedelta(minutes=1))
        assert not MaterializationScheduler(runtime).run_next_registered("worker", executor=pool)
        runtime.set_deployment_status("registered", "active")
        runtime.activate_bindings("registered", generation)
        assert MaterializationScheduler(runtime).run_next_registered("worker", executor=pool)
        assert [batch.column("value").to_pylist() for batch in store.timeseries("urn:registered:out")] == [[2.0]]
    finally:
        pool.close(); store.close()

def test_registered_preview_uses_production_compute_without_committing(tmp_path):
    store = DuckDBStore(tmp_path / "preview.duckdb", recreate=True); pool = LocalExecutorPool(workers=1)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("preview-input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:preview:in", "ts": start, "numeric_value": -3.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, name="preview", inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("preview", definition_id)
        binding = BindingSpec("one", {"input": ("urn:preview:in",)}, {"output": ("urn:preview:out",)})
        runtime.persist_bindings("preview", generation, 1, definition_id, (binding,)); runtime.activate_bindings("preview", generation)
        runtime.set_deployment_status("preview", "active")
        MaterializationScheduler(runtime).create_plan_for_binding(binding_id=binding.binding_id(definition_id), generation=generation, graph_revision=1,
            progress={"urn:preview:in": 0}, heads={"urn:preview:in": 1}, impact=pointwise(), reason={}, maximum_partition_duration=timedelta(minutes=1))
        result = MaterializationScheduler(runtime).preview_registered("previewer", executor=pool, deployment_name="preview")
        assert result and result[0].column("numeric_value").to_pylist() == [3.0]
        assert list(store.timeseries("urn:preview:out")) == []
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_plan_partitions").fetchone() == ("pending",)
    finally:
        pool.close(); store.close()

def test_pending_registered_partition_recovers_after_duckdb_restart(tmp_path):
    path = tmp_path / "restart.duckdb"; start = datetime(2026, 1, 1, tzinfo=UTC)
    store = DuckDBStore(path, recreate=True)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("restart-input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:restart:in", "ts": start, "numeric_value": -4.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, name="restart", inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("restart", definition_id)
        binding = BindingSpec("one", {"input": ("urn:restart:in",)}, {"output": ("urn:restart:out",)})
        runtime.persist_bindings("restart", generation, 1, definition_id, (binding,)); runtime.activate_bindings("restart", generation)
        runtime.set_deployment_status("restart", "active")
        MaterializationScheduler(runtime).create_plan_for_binding(binding_id=binding.binding_id(definition_id), generation=generation, graph_revision=1,
            progress={"urn:restart:in": 0}, heads={"urn:restart:in": 1}, impact=pointwise(), reason={}, maximum_partition_duration=timedelta(minutes=1))
    finally:
        store.close()
    store = DuckDBStore(path)
    pool = LocalExecutorPool(workers=1)
    try:
        assert MaterializationScheduler(MaterializationDuckDB(store)).run_next_registered("restart", executor=pool)
        assert [batch.column("value").to_pylist() for batch in store.timeseries("urn:restart:out")] == [[4.0]]
    finally:
        pool.close(); store.close()

def test_rebind_worker_persists_direct_binding_and_plans_current_lag(tmp_path):
    store = DuckDBStore(tmp_path / "rebind-worker.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:rebind:in", "ts": start, "numeric_value": 3.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, name="rebindable", inputs={"input": "urn:rebind:in"},
                                    outputs={"output": "urn:rebind:out"}, impact=pointwise())
        definition_id = runtime.register_definition(definition)
        runtime.deploy("rebindable", definition_id, graph_revision=1)
        runtime.request_rebind("rebindable", 1)
        result = MaterializationRebinder(runtime, object()).run_once("worker")
        assert result and result.deployment_name == "rebindable"
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_rebind_requests").fetchone() == ("completed",)
            assert conn.execute("SELECT count(*) FROM materialization_bindings").fetchone() == (1,)
        plans = MaterializationScheduler(runtime).discover_and_plan(
            impact=result.impact, deployment_name=result.deployment_name,
            maximum_partition_duration=timedelta(minutes=1),
        )
        assert len(plans) == 1
    finally:
        store.close()

def test_staged_bootstrap_plans_all_retained_input_history(tmp_path):
    store = DuckDBStore(tmp_path / "bootstrap-history.duckdb", recreate=True)
    try:
        start = datetime(2020, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("history", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:history", "ts": start, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:history", "ts": start + timedelta(days=365), "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="in", outputs="out", impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("history", definition_id)
        binding = BindingSpec("one", {"input": ("urn:history",)}, {"output": ("urn:derived",)})
        generation = runtime.stage_bindings("history", 1, definition_id, (binding,))
        plans = MaterializationScheduler(runtime).bootstrap_staged(deployment_name="history", generation=generation, graph_revision=1, impact=pointwise(), maximum_partition_duration=timedelta(days=400))
        assert len(plans) == 1
        with store._own_conn() as conn:
            start_ts, end_ts = conn.execute("SELECT start_ts, end_ts FROM materialization_plan_partitions").fetchone()
            reason = conn.execute("SELECT reason_json FROM materialization_plans").fetchone()[0]
        assert start_ts.replace(tzinfo=UTC) == start
        assert end_ts.replace(tzinfo=UTC) == start + timedelta(days=365, microseconds=1)
        import json
        assert json.loads(reason)["retained_from"] == start.isoformat()
    finally:
        store.close()

def test_per_input_selector_expands_published_graph_rows_deterministically():
    class Graph:
        def sparql_query(self, query, **kwargs):
            assert query == "SELECT ?ref_uri WHERE {}"
            return {"columns": ["ref_uri"], "rows": [["urn:b"], ["urn:a"]]}
    bindings = resolve_bindings({
        "inputs": {"criteria": {"sparql": "SELECT ?ref_uri WHERE {}"}},
        "outputs": {"mode": "per_input", "name": "celsius"},
    }, Graph())
    assert [binding.logical_key for binding in bindings] == ["urn:a", "urn:b"]
    assert all(binding.outputs["output"][0].startswith("urn:acquirium:derived:celsius:") for binding in bindings)

def test_by_entity_selector_joins_roles_on_the_published_entity_alias():
    class Graph:
        def sparql_query(self, query, **kwargs):
            rows = {
                "temperature": [["urn:ahu-1", "urn:temp-1"], ["urn:ahu-2", "urn:temp-2"]],
                "humidity": [["urn:ahu-1", "urn:humidity-1"], ["urn:ahu-3", "urn:humidity-3"]],
            }
            return {"columns": ["entity", "ref_uri"], "rows": rows[query]}
    bindings = resolve_bindings({"bind": {"entity_alias": "entity", "selectors": {
        "temperature": {"criteria": {"sparql": "temperature"}},
        "humidity": {"criteria": {"sparql": "humidity"}},
    }}, "outputs": {"name": "comfort"}}, Graph())
    assert len(bindings) == 1
    assert bindings[0].logical_key == "urn:ahu-1"
    assert bindings[0].inputs == {"temperature": ("urn:temp-1",), "humidity": ("urn:humidity-1",)}

def test_single_binding_declaration_uses_its_explicit_input_roles():
    bindings = resolve_bindings({"bind": {"inputs": {"temperature": "urn:temp", "humidity": "urn:humidity"}},
                                 "outputs": {"output": "urn:comfort"}}, object())
    assert bindings == (BindingSpec("default", {"temperature": ("urn:temp",), "humidity": ("urn:humidity",)}, {"output": ("urn:comfort",)}),)

def test_binding_activation_is_atomic_and_rejects_active_output_conflicts(tmp_path):
    store = DuckDBStore(tmp_path / "activation.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        first = definition_for(abs, name="first", inputs="urn:in", outputs="urn:out", impact=pointwise())
        first_id = runtime.register_definition(first); first_gen = runtime.deploy("first", first_id)
        runtime.persist_bindings("first", first_gen, 1, first_id, (BindingSpec("one", {"input": ("urn:in",)}, {"output": ("urn:out",)}),))
        runtime.activate_bindings("first", first_gen)
        second = definition_for(abs, name="second", inputs="urn:other", outputs="urn:out", impact=pointwise())
        second_id = runtime.register_definition(second); second_gen = runtime.deploy("second", second_id)
        runtime.persist_bindings("second", second_gen, 1, second_id, (BindingSpec("two", {"input": ("urn:other",)}, {"output": ("urn:out",)}),))
        import pytest
        with pytest.raises(ValueError, match="owned"):
            runtime.activate_bindings("second", second_gen)
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_bindings WHERE deployment_name = 'second'").fetchone() == ("staging",)
    finally:
        store.close()

def test_staged_generation_keeps_active_pointer_until_atomic_activation(tmp_path):
    store = DuckDBStore(tmp_path / "generations.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="urn:in", outputs="urn:out", impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("convert", definition_id)
        runtime.persist_bindings("convert", generation, 1, definition_id, (BindingSpec("old", {"input": ("urn:in",)}, {"output": ("urn:out",)}),))
        runtime.activate_bindings("convert", generation)
        staged = runtime.stage_bindings("convert", 2, definition_id, (BindingSpec("new", {"input": ("urn:new",)}, {"output": ("urn:new-out",)}),))
        assert staged == generation + 1
        with store._own_conn() as conn:
            assert conn.execute("SELECT generation, staged_generation FROM materialization_deployments WHERE name = 'convert'").fetchone() == (generation, staged)
        runtime.activate_bindings("convert", staged)
        with store._own_conn() as conn:
            assert conn.execute("SELECT generation, staged_generation FROM materialization_deployments WHERE name = 'convert'").fetchone() == (staged, None)
    finally:
        store.close()

def test_pending_staged_plan_does_not_replace_active_topology(tmp_path):
    store = DuckDBStore(tmp_path / "pending-staged-topology.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="urn:in", outputs="urn:out", impact=pointwise())
        definition_id = runtime.register_definition(definition); active = runtime.deploy("convert", definition_id)
        old = BindingSpec("old", {"input": ("urn:old",)}, {"output": ("urn:old-out",)})
        runtime.persist_bindings("convert", active, 1, definition_id, (old,))
        runtime.activate_bindings("convert", active)
        staged = runtime.stage_bindings("convert", 2, definition_id, (BindingSpec("new", {"input": ("urn:new",)}, {"output": ("urn:new-out",)}),))
        runtime.set_deployment_status("convert", "active")
        runtime.create_plan(binding_id=BindingSpec("new", {"input": ("urn:new",)}, {"output": ("urn:new-out",)}).binding_id(definition_id), generation=staged, graph_revision=2,
            input_vector={}, ranges=(TimeRange(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
        assert runtime.activate_ready_bindings() == ()
        with store._own_conn() as conn:
            assert conn.execute("SELECT generation FROM materialization_deployments WHERE name = 'convert'").fetchone() == (active,)
            assert conn.execute("SELECT status FROM materialization_bindings WHERE logical_key = 'old'").fetchone() == ("active",)
    finally:
        store.close()

def test_activation_retracts_outputs_owned_only_by_retired_bindings(tmp_path):
    store = DuckDBStore(tmp_path / "retire-output.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="urn:in", outputs="urn:out", impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("convert", definition_id)
        runtime.persist_bindings("convert", generation, 1, definition_id, (BindingSpec("old", {"input": ("urn:in",)}, {"output": ("urn:retired",)}),))
        runtime.activate_bindings("convert", generation)
        PublicationDuckDB(store).publish(PublicationRequest("old-output", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:retired", "ts": start, "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime.stage_bindings("convert", 2, definition_id, (BindingSpec("new", {"input": ("urn:in",)}, {"output": ("urn:replacement",)}),))
        runtime.set_deployment_status("convert", "active")
        assert runtime.activate_ready_bindings() == ("convert",)
        assert list(store.timeseries("urn:retired")) == []
    finally:
        store.close()

def test_empty_selector_result_is_a_valid_retiring_topology(tmp_path):
    class Graph:
        def sparql_query(self, *args, **kwargs):
            return {"columns": ["ref_uri"], "rows": []}
    assert resolve_bindings({"bind": {"selector": {"criteria": {"sparql": "SELECT ?ref_uri WHERE {}"}}},
                             "outputs": {"mode": "per_input", "name": "derived"}}, Graph()) == ()
    store = DuckDBStore(tmp_path / "empty-topology.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="urn:in", outputs="urn:out", impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("convert", definition_id)
        runtime.persist_bindings("convert", generation, 1, definition_id, (BindingSpec("old", {"input": ("urn:in",)}, {"output": ("urn:out",)}),))
        runtime.activate_bindings("convert", generation)
        staged = runtime.stage_bindings("convert", 2, definition_id, ())
        runtime.set_deployment_status("convert", "active")
        assert runtime.activate_ready_bindings() == ("convert",)
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_bindings WHERE logical_key = 'old'").fetchone() == ("retiring",)
    finally:
        store.close()

def test_code_replacement_stages_new_definition_without_moving_active_pointer(tmp_path):
    store = DuckDBStore(tmp_path / "code-replacement.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        first = definition_for(abs, name="convert", inputs="urn:in", outputs="urn:out", impact=pointwise())
        first_id = runtime.register_definition(first); active = runtime.deploy("convert", first_id)
        runtime.persist_bindings("convert", active, 1, first_id, (BindingSpec("old", {"input": ("urn:in",)}, {"output": ("urn:out",)}),))
        runtime.activate_bindings("convert", active)
        second = definition_for(round, name="convert", inputs="urn:in", outputs="urn:out", impact=pointwise())
        second_id = runtime.register_definition(second); staged = runtime.deploy("convert", second_id)
        _, partitions = runtime.create_plan(binding_id=BindingSpec("old", {"input": ("urn:in",)}, {"output": ("urn:out",)}).binding_id(first_id), generation=active,
            graph_revision=1, input_vector={}, ranges=(TimeRange(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
        assert runtime.partition_definition(partitions[0].partition_id)["entrypoint"] == "builtins:abs"
        with store._own_conn() as conn:
            assert conn.execute("SELECT definition_id, generation, staged_definition_id, staged_generation FROM materialization_deployments WHERE name = 'convert'").fetchone() == (first_id, active, second_id, staged)
    finally:
        store.close()

def test_staging_execution_writes_isolated_rows_not_canonical_output(tmp_path):
    store = DuckDBStore(tmp_path / "staged-output.duckdb", recreate=True)
    pool = LocalExecutorPool(workers=1)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:stage:in", "ts": start, "numeric_value": -2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, name="stage", inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("stage", definition_id)
        binding = BindingSpec("one", {"input": ("urn:stage:in",)}, {"output": ("urn:stage:out",)})
        generation = runtime.stage_bindings("stage", 1, definition_id, (binding,))
        runtime.set_deployment_status("stage", "active")
        MaterializationScheduler(runtime).create_plan_for_binding(binding_id=binding.binding_id(definition_id), generation=generation, graph_revision=1,
            progress={"urn:stage:in": 0}, heads={"urn:stage:in": 1}, impact=pointwise(), reason={}, maximum_partition_duration=timedelta(minutes=1))
        assert MaterializationScheduler(runtime).run_next_registered("worker", executor=pool)
        assert list(store.timeseries("urn:stage:out")) == []
        with store._own_conn() as conn:
            assert conn.execute("SELECT numeric_value FROM materialization_staged_outputs").fetchone() == (2.0,)
        assert runtime.activate_ready_bindings() == ("stage",)
        assert [batch.column("value").to_pylist() for batch in store.timeseries("urn:stage:out")] == [[2.0]]
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_staged_outputs").fetchone() == (0,)
    finally:
        pool.close(); store.close()

def test_pending_work_recovers_after_storage_reopen(tmp_path):
    path = tmp_path / "recovery.duckdb"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    first_store = DuckDBStore(path, recreate=True)
    runtime = MaterializationDuckDB(first_store)
    runtime.create_plan(binding_id="recovery", generation=1, graph_revision=1, input_vector={},
                        ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
    runtime.lease_partition("lost-worker", duration=-timedelta(microseconds=1))
    first_store.close()
    second_store = DuckDBStore(path)
    try:
        recovered = MaterializationDuckDB(second_store).lease_partition("new-worker")
        assert recovered is not None and recovered.attempt == 2
    finally:
        second_store.close()

def test_range_commit_replaces_missing_rows_and_rejects_stale_snapshot(tmp_path):
    store = DuckDBStore(tmp_path / "range-commit.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        continuous = PublicationDuckDB(store)
        input_table = pa.Table.from_pylist([{"operation": "upsert", "ref_uri": "urn:in", "ts": start, "numeric_value": 1.0, "text_value": None}], schema=MUTATION_SCHEMA)
        continuous.publish(PublicationRequest("input-1", input_table))
        runtime = MaterializationDuckDB(store)
        def lease_snapshot(*, duration=timedelta(minutes=5)):
            runtime.create_plan(binding_id="binding", generation=1, graph_revision=1, input_vector={"urn:in": 1}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
            return runtime.snapshot_partition(runtime.lease_partition("worker", duration=duration), ("urn:in",))
        stale = lease_snapshot(duration=-timedelta(microseconds=1))
        continuous.publish(PublicationRequest("input-2", input_table))
        output = pa.table({"ref_uri": ["urn:out"], "ts": [start], "numeric_value": [10.0], "text_value": [None]})
        import pytest
        with pytest.raises(StaleAttemptError):
            runtime.commit_replacement(stale, input_refs=("urn:in",), output_refs=("urn:out",), replacement=output)
        # A fresh plan commits a complete replacement. An empty later result
        # tombstones that prior owned key in the same range.
        fresh = lease_snapshot()
        runtime.commit_replacement(fresh, input_refs=("urn:in",), output_refs=("urn:out",), replacement=output)
        runtime.create_plan(binding_id="binding-2", generation=1, graph_revision=1, input_vector={"urn:in": 2}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={}, maximum_partition_duration=timedelta(minutes=1))
        empty = runtime.snapshot_partition(runtime.lease_partition("worker"), ("urn:in",))
        runtime.commit_replacement(empty, input_refs=("urn:in",), output_refs=("urn:out",), replacement=pa.table({"ref_uri": [], "ts": pa.array([], type=pa.timestamp("us", tz="UTC")), "numeric_value": [], "text_value": []}))
        assert list(store.timeseries("urn:out")) == []
        with store._own_conn() as conn:
            assert conn.execute("SELECT stream_version FROM materialization_binding_progress WHERE binding_id = 'binding'").fetchone() == (1,)
            assert conn.execute("SELECT count(*) FROM materialization_execution_receipts WHERE status = 'committed'").fetchone() == (2,)
    finally:
        store.close()

def test_lookback_stale_check_only_rejects_changes_that_affect_partition(tmp_path):
    store = DuckDBStore(tmp_path / "lookback-stale.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        initial = pa.Table.from_pylist([{"operation": "upsert", "ref_uri": "urn:in", "ts": start, "numeric_value": 1.0, "text_value": None}], schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("lookback-1", initial))
        runtime = MaterializationDuckDB(store)
        scheduler = MaterializationScheduler(runtime)
        _, parts = scheduler.create_plan_for_binding(binding_id="binding", generation=1, graph_revision=1, progress={"urn:in": 0}, heads={"urn:in": 1}, impact=lookback(timedelta(minutes=5)), reason={}, maximum_partition_duration=timedelta(minutes=10))
        snapshot = runtime.snapshot_partition(runtime.lease_partition("worker"), ("urn:in",))
        # A newer source change ten minutes later cannot affect the owned output range.
        later = pa.Table.from_pylist([{"operation": "upsert", "ref_uri": "urn:in", "ts": start + timedelta(minutes=10), "numeric_value": 2.0, "text_value": None}], schema=MUTATION_SCHEMA)
        PublicationDuckDB(store).publish(PublicationRequest("lookback-2", later))
        runtime.commit_replacement(snapshot, input_refs=("urn:in",), output_refs=("urn:out",), replacement=pa.table({"ref_uri": [], "ts": pa.array([], type=pa.timestamp("us", tz="UTC")), "numeric_value": [], "text_value": []}))
    finally:
        store.close()

def _compute_request(*, refs=frozenset({"urn:out"})):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    inputs = pa.table({"ref_uri": ["urn:in", "urn:in"], "ts": [start, start + timedelta(seconds=1)],
                       "numeric_value": [273.15, 274.15], "text_value": [None, None]})
    return ComputeRequest(inputs, TransformContext("binding", "execution", TimeRange(start, start + timedelta(minutes=1)), {}), refs)

def test_scalar_arrow_adapter_and_bounded_pool():
    request = _compute_request()
    with_pool = LocalExecutorPool(workers=1)
    try:
        result = with_pool.submit(lambda value: value - 273.15, ComputeRequest(request.inputs, request.context, request.output_refs, scalar=True)).result()
    finally:
        with_pool.close()
    assert result.column("numeric_value").to_pylist() == [0.0, 1.0]

def test_batch_adapter_rejects_out_of_range_or_unowned_output():
    request = _compute_request()
    def invalid(batch, context):
        return pa.table({"ref_uri": ["urn:not-owned"], "ts": [context.interval.end], "numeric_value": [1.0], "text_value": [None]})
    import pytest
    with pytest.raises(OutputValidationError):
        PythonArrowAdapter().execute(invalid, request)

def test_scalar_adapter_handles_text_and_null_and_rejects_bad_return_values():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    inputs = pa.table({"ref_uri": ["urn:in", "urn:in"], "ts": [start, start + timedelta(seconds=1)],
                       "numeric_value": [None, None], "text_value": ["open", None]})
    request = ComputeRequest(inputs, TransformContext("binding", "execution", TimeRange(start, start + timedelta(minutes=1)), {}), frozenset({"urn:out"}), scalar=True)
    result = PythonArrowAdapter().execute(lambda value: None if value is None else value.upper(), request)
    assert result.column("text_value").to_pylist() == ["OPEN", None]
    import pytest
    with pytest.raises(TypeError, match="scalar transformations"):
        PythonArrowAdapter().execute(lambda value: {"invalid": value}, request)

def test_batch_adapter_selects_required_schema_and_rejects_missing_columns():
    request = _compute_request()
    def valid(batch, context):
        return pa.table({"ref_uri": ["urn:out"], "ts": [context.interval.start], "numeric_value": [2.0], "text_value": [None], "debug": ["ignored"]})
    result = PythonArrowAdapter().execute(valid, request)
    assert result.column_names == ["ref_uri", "ts", "numeric_value", "text_value"]
    import pytest
    with pytest.raises(OutputValidationError, match="missing"):
        PythonArrowAdapter().execute(lambda batch, context: pa.table({"ref_uri": [], "ts": []}), request)

def test_pool_is_bounded_and_recovers_after_a_failed_transform():
    import threading
    request = _compute_request()
    pool = LocalExecutorPool(workers=2)
    thread_ids = set()
    def work(batch, context):
        thread_ids.add(threading.get_ident())
        return pa.table({"ref_uri": ["urn:out"], "ts": [context.interval.start], "numeric_value": [1.0], "text_value": [None]})
    try:
        futures = [pool.submit(work, request) for _ in range(20)]
        assert all(future.result().num_rows == 1 for future in futures)
        failed = pool.submit(lambda batch, context: (_ for _ in ()).throw(RuntimeError("boom")), request)
        import pytest
        with pytest.raises(RuntimeError, match="boom"):
            failed.result()
        assert pool.submit(work, request).result().num_rows == 1
    finally:
        pool.close()
    assert len(thread_ids) <= 2

def test_definition_cache_reuses_digest_and_reloads_after_clear():
    cache = DefinitionCache()
    calls = []
    assert cache.load("digest", lambda: calls.append(1) or object()) is cache.load("digest", lambda: calls.append(2) or object())
    assert calls == [1]
    cache.clear()
    cache.load("digest", lambda: calls.append(3) or object())
    assert calls == [1, 3]


def test_long_backfill_compute_does_not_hold_the_canonical_write_lock(tmp_path):
    """A singleton publication can commit while a materialization computes."""
    store = DuckDBStore(tmp_path / "nonblocking-backfill.duckdb", recreate=True)
    entered = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    errors = []
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        continuous = PublicationDuckDB(store)
        row_count = 10_000
        continuous.publish(PublicationRequest("backfill-input", pa.table({
            "operation": ["upsert"] * row_count,
            "ref_uri": ["urn:backfill:in"] * row_count,
            "ts": [start + timedelta(microseconds=index) for index in range(row_count)],
            "numeric_value": [1.0] * row_count,
            "text_value": [None] * row_count,
        }, schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="in", outputs="out", impact=pointwise())
        definition_id = runtime.register_definition(definition)
        generation = runtime.deploy("backfill", definition_id)
        binding = BindingSpec("one", {"input": ("urn:backfill:in",)}, {"output": ("urn:backfill:out",)})
        runtime.persist_bindings("backfill", generation, 1, definition_id, (binding,))
        MaterializationScheduler(runtime).create_plan_for_binding(
            binding_id=binding.binding_id(definition_id), generation=generation, graph_revision=1,
            progress={"urn:backfill:in": 0}, heads={"urn:backfill:in": 1}, impact=pointwise(),
            reason={}, maximum_partition_duration=timedelta(minutes=1),
        )

        def compute(snapshot, outputs):
            entered.set()
            assert release.wait(timeout=5)
            return pa.table({"ref_uri": [outputs[0]], "ts": [start], "numeric_value": [1.0], "text_value": [None]})

        def run_backfill():
            try:
                MaterializationScheduler(runtime).run_once("backfill-worker", compute)
            except Exception as error:  # surfaced by the main test thread
                errors.append(error)
            finally:
                completed.set()

        worker = threading.Thread(target=run_backfill)
        worker.start()
        assert entered.wait(timeout=5)
        # This would block until ``release`` if Python compute held the write lock.
        receipt = continuous.publish(PublicationRequest("singleton-ingest", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:singleton", "ts": start,
             "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        assert receipt.row_count == 1
        assert not completed.is_set()
        release.set()
        worker.join(timeout=5)
        assert completed.is_set() and not errors
    finally:
        release.set()
        store.close()


def test_two_hop_registered_transformations_converge_from_durable_progress(tmp_path):
    """A derived output becomes input to the next active deployment."""
    store = DuckDBStore(tmp_path / "two-hop.duckdb", recreate=True)
    pool = LocalExecutorPool(workers=1)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        continuous = PublicationDuckDB(store)
        continuous.publish(PublicationRequest("two-hop-source", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:hop:source", "ts": start,
             "numeric_value": -7.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        scheduler = MaterializationScheduler(runtime)
        first = definition_for(abs, name="first-hop", inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        first_id = runtime.register_definition(first)
        first_generation = runtime.deploy("first-hop", first_id)
        first_binding = BindingSpec("first", {"input": ("urn:hop:source",)}, {"output": ("urn:hop:middle",)})
        runtime.persist_bindings("first-hop", first_generation, 1, first_id, (first_binding,))
        runtime.activate_bindings("first-hop", first_generation)
        runtime.set_deployment_status("first-hop", "active")
        scheduler.create_plan_for_binding(
            binding_id=first_binding.binding_id(first_id), generation=first_generation, graph_revision=1,
            progress={"urn:hop:source": 0}, heads={"urn:hop:source": 1}, impact=pointwise(),
            reason={}, maximum_partition_duration=timedelta(minutes=1),
        )
        assert scheduler.run_next_registered("first-worker", executor=pool)

        second = definition_for(abs, name="second-hop", inputs="in", outputs={"mode": "per_input"}, impact=pointwise())
        second_id = runtime.register_definition(second)
        second_generation = runtime.deploy("second-hop", second_id)
        second_binding = BindingSpec("second", {"input": ("urn:hop:middle",)}, {"output": ("urn:hop:final",)})
        runtime.persist_bindings("second-hop", second_generation, 1, second_id, (second_binding,))
        runtime.activate_bindings("second-hop", second_generation)
        runtime.set_deployment_status("second-hop", "active")
        assert scheduler.discover_all()
        assert scheduler.run_next_registered("second-worker", executor=pool)
        assert [batch.column("value").to_pylist() for batch in store.timeseries("urn:hop:final")] == [[7.0]]
    finally:
        pool.close()
        store.close()


def test_thousand_idle_bindings_share_a_bounded_executor_pool(tmp_path):
    """Logical bindings are durable rows, not executor workers or Ray actors."""
    store = DuckDBStore(tmp_path / "thousand-bindings.duckdb", recreate=True)
    pool = LocalExecutorPool(workers=2)
    try:
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, name="thousand", inputs="in", outputs="out", impact=pointwise())
        definition_id = runtime.register_definition(definition)
        generation = runtime.deploy("thousand", definition_id)
        bindings = tuple(
            BindingSpec(str(index), {"input": (f"urn:thousand:in:{index}",)},
                        {"output": (f"urn:thousand:out:{index}",)})
            for index in range(1_000)
        )
        runtime.persist_bindings("thousand", generation, 1, definition_id, bindings)
        runtime.activate_bindings("thousand", generation)
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM materialization_bindings").fetchone() == (1_000,)
        # ThreadPoolExecutor lazily creates workers; idle bindings create none.
        assert not [thread for thread in threading.enumerate()
                    if thread.name.startswith("acquirium-materialization")]
        assert pool._executor._max_workers == 2
    finally:
        pool.close()
        store.close()


def test_generic_artifact_production_is_durable_idempotent_and_promotable(tmp_path):
    store = DuckDBStore(tmp_path / "artifact-state.duckdb", recreate=True)
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        request = ArtifactRequest("produce-1", "calibration", "demo", "binding", {"urn:in": 1},
                                  TimeRange(start, start + timedelta(hours=1)), metadata={"source": "operator"})
        assert runtime.create_artifact_request(request) == request.request_id
        assert runtime.create_artifact_request(request) == request.request_id
        lease = runtime.lease_artifact_request("worker")
        assert lease is not None
        candidate = ArtifactCandidate(b"calibration-v1", media_type="application/x-calibration", metrics={"rmse": 0.1})
        revision = runtime.complete_artifact_request(lease, artifacts.put(candidate.data, media_type=candidate.media_type), candidate)
        assert runtime.complete_artifact_request(lease, artifacts.put(candidate.data, media_type=candidate.media_type), candidate).revision_id == revision.revision_id
        assert runtime.promote_state_revision(revision.revision_id).status == "active"
        assert artifacts.get(revision.artifact.digest) == b"calibration-v1"
    finally:
        store.close()


def test_artifact_files_are_digest_verified(tmp_path):
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    record = artifacts.put(b"immutable")
    assert artifacts.get(record.digest) == b"immutable"
    artifacts._path(record.digest).write_bytes(b"corrupt")
    import pytest
    with pytest.raises(ValueError, match="digest verification"):
        artifacts.get(record.digest)


def test_artifact_sweeper_removes_only_abandoned_temporary_files(tmp_path):
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    record = artifacts.put(b"retain")
    temporary = artifacts.root / ".tmp-abandoned"
    temporary.write_bytes(b"temporary")
    assert artifacts.sweep_temporary_files(older_than_seconds=-1) == 1
    assert artifacts.get(record.digest) == b"retain"


def test_artifact_sweeper_preserves_durable_references_and_collects_orphans(tmp_path):
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    retained = artifacts.put(b"retained")
    orphan = artifacts.put(b"orphan")
    assert artifacts.sweep_orphans({retained.digest}, older_than_seconds=-1) == 1
    assert artifacts.get(retained.digest) == b"retained"
    import pytest
    with pytest.raises(KeyError):
        artifacts.get(orphan.digest)


def test_stateful_adapter_caches_ephemeral_worker_and_decoded_artifact():
    calls = {"setup": 0, "load": 0}
    class Offset(StatefulTransformation):
        def setup_worker(self):
            calls["setup"] += 1
            return object()
        def load_artifact(self, artifact, worker):
            calls["load"] += 1
            return float(artifact.decode())
        def transform(self, batch, state, context):
            return pa.table({"ref_uri": ["urn:out"], "ts": [context.interval.start],
                             "numeric_value": [state], "text_value": [None]})
    request = _compute_request()
    request = ComputeRequest(request.inputs, TransformContext("binding", "run", request.context.interval, {}, state_revision="revision-1"), request.output_refs, artifact_bytes=b"3")
    adapter = PythonArrowAdapter()
    assert adapter.execute(Offset, request).column("numeric_value").to_pylist() == [3.0]
    assert adapter.execute(Offset, request).column("numeric_value").to_pylist() == [3.0]
    assert calls == {"setup": 1, "load": 1}


def test_stateful_decorator_creates_a_normal_durable_definition():
    @stateful(inputs="input", outputs={"mode": "per_input"})
    class Offset(StatefulTransformation):
        def transform(self, batch, state, context):
            return pa.table({"ref_uri": [], "ts": [], "numeric_value": [], "text_value": []})

    definition = Offset.__acquirium_definition__
    assert definition.name == "Offset"
    assert definition.entrypoint.endswith(".Offset")
    assert definition.impact == pointwise()


def test_prospective_promotion_pins_old_and_new_plans_to_their_revisions(tmp_path):
    store = DuckDBStore(tmp_path / "pinned-revisions.duckdb", recreate=True)
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        def produce(request_id, payload):
            request = ArtifactRequest(request_id, "calibration", "deployment", "binding", {},
                                      TimeRange(start, start + timedelta(minutes=1)), metadata={"tag": request_id})
            runtime.create_artifact_request(request)
            lease = runtime.lease_artifact_request("worker")
            candidate = ArtifactCandidate(payload)
            return runtime.complete_artifact_request(lease, artifacts.put(payload), candidate)
        first = produce("first", b"one")
        runtime.promote_state_revision(first.revision_id)
        old_plan, _ = runtime.create_plan(binding_id="binding", generation=1, graph_revision=1,
            input_vector={}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        second = produce("second", b"two")
        runtime.promote_state_revision(second.revision_id)
        new_plan, _ = runtime.create_plan(binding_id="binding", generation=1, graph_revision=1,
            input_vector={}, ranges=(TimeRange(start + timedelta(minutes=1), start + timedelta(minutes=2)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        with store._own_conn() as conn:
            rows = dict(conn.execute("SELECT plan_id, state_revision FROM materialization_plans WHERE plan_id IN (?, ?)", [old_plan, new_plan]).fetchall())
        assert rows == {old_plan: first.revision_id, new_plan: second.revision_id}
    finally:
        store.close()


def test_recompute_promotion_records_a_durable_state_invalidation(tmp_path):
    store = DuckDBStore(tmp_path / "state-invalidation.duckdb", recreate=True)
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        runtime = MaterializationDuckDB(store); start = datetime(2026, 1, 1, tzinfo=UTC)
        request = ArtifactRequest("invalidate", "calibration", "deployment", "binding", {}, TimeRange(start, start + timedelta(minutes=1)))
        runtime.create_artifact_request(request); lease = runtime.lease_artifact_request("worker")
        candidate = ArtifactCandidate(b"revision")
        revision = runtime.complete_artifact_request(lease, artifacts.put(candidate.data), candidate)
        runtime.promote_state_revision(revision.revision_id, policy="recompute_from", effective_from=start)
        with store._own_conn() as conn:
            assert conn.execute("SELECT binding_id, policy FROM materialization_state_invalidations").fetchone() == ("binding", "recompute_from")
    finally:
        store.close()


def test_recompute_promotion_supersedes_uncommitted_old_state_work(tmp_path):
    store = DuckDBStore(tmp_path / "superseded-state-work.duckdb", recreate=True)
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        def produce(request_id, payload):
            request = ArtifactRequest(request_id, "calibration", "deployment", "binding", {},
                                      TimeRange(start, start + timedelta(minutes=1)), metadata={"revision": request_id})
            runtime.create_artifact_request(request)
            lease = runtime.lease_artifact_request("worker")
            candidate = ArtifactCandidate(payload)
            return runtime.complete_artifact_request(lease, artifacts.put(payload), candidate)

        first = produce("old", b"old")
        runtime.promote_state_revision(first.revision_id)
        plan_id, _ = runtime.create_plan(binding_id="binding", generation=1, graph_revision=1,
            input_vector={}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        second = produce("new", b"new")
        runtime.promote_state_revision(second.revision_id, policy="recompute_all")
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_plan_partitions WHERE plan_id = ?", [plan_id]).fetchone() == ("superseded",)
        assert runtime.lease_partition("worker") is None
    finally:
        store.close()


def test_recompute_from_invalidation_creates_a_bounded_durable_plan(tmp_path):
    store = DuckDBStore(tmp_path / "invalidation-plan.duckdb", recreate=True)
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("input", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:state:in", "ts": start, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:state:in", "ts": start + timedelta(minutes=2), "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = definition_for(abs, inputs="in", outputs="out", impact=pointwise())
        definition_id = runtime.register_definition(definition); generation = runtime.deploy("state", definition_id)
        binding = BindingSpec("one", {"input": ("urn:state:in",)}, {"output": ("urn:state:out",)},
                              {"output_ref": "urn:state:out"})
        runtime.persist_bindings("state", generation, 1, definition_id, (binding,)); runtime.activate_bindings("state", generation)
        request = ArtifactRequest("recompute", "calibration", "state", binding.binding_id(definition_id), {}, TimeRange(start, start + timedelta(minutes=3)))
        runtime.create_artifact_request(request); lease = runtime.lease_artifact_request("worker")
        candidate = ArtifactCandidate(b"v1")
        revision = runtime.complete_artifact_request(lease, artifacts.put(candidate.data), candidate)
        runtime.promote_state_revision(revision.revision_id, policy="recompute_from", effective_from=start + timedelta(minutes=1))
        plans = MaterializationScheduler(runtime).plan_state_invalidations(maximum_partition_duration=timedelta(hours=1))
        assert len(plans) == 1
        with store._own_conn() as conn:
            begin, end = conn.execute("SELECT start_ts, end_ts FROM materialization_plan_partitions").fetchone()
        assert begin.replace(tzinfo=UTC) == start + timedelta(minutes=1)
        assert end.replace(tzinfo=UTC) == start + timedelta(minutes=2, microseconds=1)

        all_request = ArtifactRequest("recompute-all", "calibration", "state", binding.binding_id(definition_id), {},
                                      TimeRange(start, start + timedelta(minutes=3)), metadata={"revision": "all"})
        runtime.create_artifact_request(all_request)
        all_lease = runtime.lease_artifact_request("worker")
        all_candidate = ArtifactCandidate(b"v2")
        all_revision = runtime.complete_artifact_request(all_lease, artifacts.put(all_candidate.data), all_candidate)
        runtime.promote_state_revision(all_revision.revision_id, policy="recompute_all")
        all_plans = MaterializationScheduler(runtime).plan_state_invalidations(maximum_partition_duration=timedelta(hours=1))
        assert len(all_plans) == 1
        with store._own_conn() as conn:
            begin, end = conn.execute("SELECT start_ts, end_ts FROM materialization_plan_partitions WHERE plan_id = ?", [all_plans[0]]).fetchone()
        assert begin.replace(tzinfo=UTC) == start
        assert end.replace(tzinfo=UTC) == start + timedelta(minutes=2, microseconds=1)
    finally:
        store.close()


def test_stateful_transform_recovers_after_storage_and_worker_restart(tmp_path):
    database = tmp_path / "stateful-restart.duckdb"
    artifacts = FilesystemArtifactStore(tmp_path / "artifacts")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    DurableOffsetTransformation.setup_calls = 0
    DurableOffsetTransformation.load_calls = 0

    store = DuckDBStore(database, recreate=True)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("first", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:state:in", "ts": start,
             "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        definition = DurableOffsetTransformation.__acquirium_definition__
        definition_id = runtime.register_definition(definition)
        generation = runtime.deploy("stateful-restart", definition_id, graph_revision=1)
        binding = BindingSpec("one", {"input": ("urn:state:in",)}, {"output": ("urn:state:out",)},
                              {"output_ref": "urn:state:out"})
        binding_id = binding.binding_id(definition_id)
        runtime.persist_bindings("stateful-restart", generation, 1, definition_id, (binding,))
        runtime.activate_bindings("stateful-restart", generation)
        runtime.set_deployment_status("stateful-restart", "active")
        request = ArtifactRequest("offset-v1", "calibration", "stateful-restart", binding_id, {"urn:state:in": 1},
                                  TimeRange(start, start + timedelta(minutes=2)))
        runtime.create_artifact_request(request)
        artifact_lease = runtime.lease_artifact_request("producer")
        candidate = ArtifactCandidate(b"2")
        revision = runtime.complete_artifact_request(artifact_lease, artifacts.put(candidate.data), candidate)
        runtime.promote_state_revision(revision.revision_id)
        runtime.create_plan(binding_id=binding_id, generation=generation, graph_revision=1,
            input_vector={"urn:state:in": 1}, ranges=(TimeRange(start, start + timedelta(minutes=1)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        scheduler = MaterializationScheduler(runtime)
        executor = LocalExecutorPool(workers=1)
        try:
            assert scheduler.run_next_registered("worker-a", executor=executor)
        finally:
            executor.close()
    finally:
        store.close()

    # This is the server/storage and fixed worker-pool restart boundary.
    store = DuckDBStore(database)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("second", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:state:in", "ts": start + timedelta(minutes=1),
             "numeric_value": 5.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        runtime.create_plan(binding_id=binding_id, generation=generation, graph_revision=1,
            input_vector={"urn:state:in": 2}, ranges=(TimeRange(start + timedelta(minutes=1), start + timedelta(minutes=2)),), reason={},
            maximum_partition_duration=timedelta(minutes=1))
        executor = LocalExecutorPool(workers=1)
        try:
            assert MaterializationScheduler(runtime).run_next_registered("worker-b", executor=executor)
        finally:
            executor.close()
        with store._own_conn() as conn:
            values = conn.execute("""SELECT value.numeric_value FROM timeseries value
                JOIN ref_ids refs ON refs.ref_id = value.ref_id
                WHERE refs.ref_uri = 'urn:state:out' AND NOT value.deleted ORDER BY value.ts""").fetchall()
            receipts = conn.execute("SELECT DISTINCT state_revision FROM materialization_execution_receipts").fetchall()
        assert values == [(4.0,), (8.0,)]
        assert receipts == [(revision.revision_id,)]
        # The recreated worker decodes the immutable bytes again, so the local
        # ``uses`` mutation from the old worker cannot be authoritative.
        assert DurableOffsetTransformation.setup_calls == 2
        assert DurableOffsetTransformation.load_calls == 2
    finally:
        store.close()

def _stage_full_history_binding(runtime, *, name, inputs, outputs, impact):
    definition = definition_for(abs, name=name, inputs={"input": inputs},
                                outputs={"output": outputs}, impact=impact)
    definition_id = runtime.register_definition(definition)
    runtime.deploy(name, definition_id, graph_revision=1)
    return runtime.stage_bindings(name, 1, definition_id,
        (BindingSpec("one", {"input": (inputs,)}, {"output": (outputs,)}),))

def test_full_history_safety_scan_plans_the_entire_retained_span(tmp_path):
    store = DuckDBStore(tmp_path / "full-history-plan.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("fh-1", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:fh:in", "ts": start, "numeric_value": 1.0, "text_value": None},
            {"operation": "upsert", "ref_uri": "urn:fh:in", "ts": start + timedelta(hours=2), "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        _stage_full_history_binding(runtime, name="fh", inputs="urn:fh:in",
                                    outputs="urn:fh:out", impact=full_history())
        plans = MaterializationScheduler(runtime).discover_all(maximum_partition_duration=timedelta(hours=1))
        assert len(plans) == 1
        import json
        with store._own_conn() as conn:
            rows = conn.execute("SELECT start_ts, end_ts FROM materialization_plan_partitions ORDER BY start_ts").fetchall()
            reason = json.loads(conn.execute("SELECT reason_json FROM materialization_plans").fetchone()[0])
        # Any change dirties the whole retained span, partitioned hourly.
        assert [(row[0].replace(tzinfo=UTC), row[1].replace(tzinfo=UTC)) for row in rows] == [
            (start, start + timedelta(hours=1)),
            (start + timedelta(hours=1), start + timedelta(hours=2)),
            (start + timedelta(hours=2), start + timedelta(hours=2, microseconds=1)),
        ]
        assert reason["impact"] == full_history().to_json()
    finally:
        store.close()

def test_full_history_commit_rejects_any_newer_input_change(tmp_path):
    store = DuckDBStore(tmp_path / "full-history-stale.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("fh-2", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:fh2:in", "ts": start, "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        _stage_full_history_binding(runtime, name="fh2", inputs="urn:fh2:in",
                                    outputs="urn:fh2:out", impact=full_history())
        scheduler = MaterializationScheduler(runtime)
        assert scheduler.discover_all(maximum_partition_duration=timedelta(minutes=10))
        snapshot = runtime.snapshot_partition(runtime.lease_partition("worker"), ("urn:fh2:in",))
        # A change far outside the leased partition still dirties full-history output.
        PublicationDuckDB(store).publish(PublicationRequest("fh-3", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:fh2:in", "ts": start + timedelta(hours=5), "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        empty = pa.table({"ref_uri": [], "ts": pa.array([], type=pa.timestamp("us", tz="UTC")),
                          "numeric_value": [], "text_value": []})
        import pytest
        with pytest.raises(StaleAttemptError):
            runtime.commit_replacement(snapshot, input_refs=("urn:fh2:in",), output_refs=("urn:fh2:out",), replacement=empty)
    finally:
        store.close()

def test_full_history_without_retained_input_data_plans_nothing(tmp_path):
    store = DuckDBStore(tmp_path / "full-history-empty.duckdb", recreate=True)
    try:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        PublicationDuckDB(store).publish(PublicationRequest("fh-4", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:fh3:in", "ts": start, "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = MaterializationDuckDB(store)
        _stage_full_history_binding(runtime, name="fh3", inputs="urn:fh3:in",
                                    outputs="urn:fh3:out", impact=full_history())
        # Tombstone the only row: heads advance, but nothing is retained yet.
        PublicationDuckDB(store).publish(PublicationRequest("fh-5", pa.Table.from_pylist([
            {"operation": "delete", "ref_uri": "urn:fh3:in", "ts": start, "numeric_value": None, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        assert MaterializationScheduler(runtime).discover_all(maximum_partition_duration=timedelta(minutes=10)) == ()
    finally:
        store.close()

def test_experiment_run_id_replay_is_idempotent_but_conflict_rejected(tmp_path):
    store = DuckDBStore(tmp_path / "experiment-replay.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        interval = TimeRange(start, start + timedelta(hours=1))
        request = ExperimentRunRequest("run-1", "def-1", 5, interval,
            {"p": 1}, {"type": "object"}, {"m": "x"}, {"urn:in": 3}, [], None)
        assert runtime.start_experiment(request).status == "running"
        # An identical request is an idempotent replay of the same run.
        assert runtime.start_experiment(request).run_id == "run-1"
        # A different frozen input under the same run_id is rejected.
        conflicting = ExperimentRunRequest("run-1", "def-1", 5, interval,
            {"p": 2}, {"type": "object"}, {"m": "x"}, {"urn:in": 3}, [], None)
        import pytest
        with pytest.raises(ValueError, match="already exists"):
            runtime.start_experiment(conflicting)
    finally:
        store.close()

def test_partition_is_dead_lettered_after_max_attempts(tmp_path):
    store = DuckDBStore(tmp_path / "retry-bound.duckdb", recreate=True)
    try:
        runtime = MaterializationDuckDB(store)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        runtime.create_plan(binding_id="b", generation=1, graph_revision=1,
            input_vector={"urn:in": 1}, ranges=(TimeRange(start, start + timedelta(minutes=1)),),
            reason={}, maximum_partition_duration=timedelta(minutes=1))
        # Each lease+fail bumps the attempt until the bound dead-letters it.
        for _ in range(MAX_PARTITION_ATTEMPTS):
            lease = runtime.lease_partition("worker")
            assert lease is not None
            runtime.fail_partition(lease, {"type": "X", "message": "boom"})
        assert runtime.lease_partition("worker") is None
        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM materialization_plan_partitions").fetchone() == ("failed",)
    finally:
        store.close()
