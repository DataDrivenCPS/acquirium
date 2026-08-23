"""Focused invariants for the topology-epoch control plane."""
from datetime import datetime, timedelta, timezone
import time

import pyarrow as pa
import pytest

from acquirium.Materialization.definitions import definition_for
from acquirium.Materialization.epochs import EpochClaimError, StaleEpochError
from acquirium.Materialization.impact import lookback, pointwise
from acquirium.Materialization.epoch_reconciler import TopologyEpochReconciler
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


UTC = timezone.utc


def add_one(inputs, context):
    values = inputs.column("numeric_value").to_pylist()
    return pa.table({
        "ref_uri": ["urn:derived"] * len(values),
        "ts": inputs.column("ts"),
        "numeric_value": [value + 1 if value is not None else None for value in values],
        "text_value": [None] * len(values),
    })


def add_two(inputs, context):
    values = inputs.column("numeric_value").to_pylist()
    return pa.table({
        "ref_uri": ["urn:derived"] * len(values),
        "ts": inputs.column("ts"),
        "numeric_value": [value + 2 if value is not None else None for value in values],
        "text_value": [None] * len(values),
    })


def add_one_a(inputs, context):
    return pa.table({"ref_uri": ["urn:a"] * inputs.num_rows, "ts": inputs.column("ts"),
                     "numeric_value": [value + 1 for value in inputs.column("numeric_value").to_pylist()],
                     "text_value": [None] * inputs.num_rows})


def add_one_b(inputs, context):
    return pa.table({"ref_uri": ["urn:b"] * inputs.num_rows, "ts": inputs.column("ts"),
                     "numeric_value": [value + 1 for value in inputs.column("numeric_value").to_pylist()],
                     "text_value": [None] * inputs.num_rows})


def rolling_five_minutes(inputs, context):
    rows = inputs.select(["ts", "numeric_value"]).to_pylist()
    output = []
    for row in rows:
        ts = row["ts"]
        if not (context.write_interval.start <= ts < context.write_interval.end):
            continue
        lower = ts - timedelta(minutes=5)
        output.append({
            "ref_uri": "urn:rolling",
            "ts": ts,
            "numeric_value": sum(
                candidate["numeric_value"]
                for candidate in rows
                if lower <= candidate["ts"] <= ts
            ),
            "text_value": None,
        })
    return pa.Table.from_pylist(output)


class Graph:
    def __init__(self, refs):
        self.refs = refs

    def sparql_query(self, query, **kwargs):
        return {"columns": ["ref_uri"], "rows": [[ref] for ref in self.refs]}


def _deploy(runtime, definition, graph=None):
    definition_id = runtime.register_definition(definition)
    runtime.deploy_definition(definition.name, definition_id, graph or Graph(("urn:raw",)))
    return definition_id


def _runtime(tmp_path):
    store = DuckDBStore(tmp_path / "epochs.duckdb", recreate=True)
    publisher = PublicationDuckDB(store)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publisher.publish(PublicationRequest("raw", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": "urn:raw", "ts": start,
         "numeric_value": 2.0, "text_value": None},
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    definition = definition_for(add_one, name="add-one", inputs={"input": "urn:raw"},
                                outputs={"output": "urn:derived"}, impact=pointwise())
    _deploy(runtime, definition)
    return store, runtime, definition, start


def test_epoch_persists_resolved_binding_dag_and_claims(tmp_path):
    store, runtime, definition, _ = _runtime(tmp_path)
    try:
        epoch = runtime.ensure_epoch(7, "graph-digest")
        summary = runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        assert summary.status == "reconciling"
        binding = runtime.epoch_bindings(epoch)[0]
        assert binding.definition_id == definition.definition_id
        assert binding.inputs == {"input": ["urn:raw"]}
        claim = runtime.claim_next_work("manager", duration=timedelta(seconds=1))
        assert claim and claim.kind == "reconcile"
        assert runtime.claim_next_work("other") is None
        runtime.release_claim(claim)
        recovered = runtime.claim_next_work("other")
        assert recovered and recovered.attempt == 2
    finally:
        store.close()


def test_two_managers_interleave_without_duplicate_control_plane_work(tmp_path):
    store, first, definition, _ = _runtime(tmp_path)
    second = TopologyEpochDuckDB(store)
    try:
        epoch = first.ensure_epoch(7, "interleave")
        assert second.ensure_epoch(7, "interleave") == epoch
        first.construct_epoch(epoch, Graph(("urn:raw",)))
        assert second.construct_epoch(epoch, Graph(("urn:raw",))).status == "reconciling"
        claim_a = first.claim_next_work("manager-a")
        assert claim_a is not None and second.claim_next_work("manager-b") is None
        first.release_claim(claim_a)
        claim_b = second.claim_next_work("manager-b")
        assert claim_b is not None and claim_b.attempt == claim_a.attempt + 1
        snapshot = second.snapshot(claim_b)
        second.commit_work(snapshot, add_one(snapshot.inputs, None), claim_b)
        seal = second.claim_next_component("manager-b")
        assert seal is not None and first.claim_next_component("manager-a") is None
        second.seal_component(seal)
        assert first.active_epoch_id() == epoch
        assert definition.definition_id == first.epoch_bindings(epoch)[0].definition_id
    finally:
        store.close()


def test_epoch_pins_state_revision_before_construction(tmp_path):
    store, _, definition, _ = _runtime(tmp_path)
    state = ["state-a"]
    runtime = TopologyEpochDuckDB(store, state_revision_resolver=lambda _binding: state[0])
    try:
        _deploy(runtime, definition)
        epoch = runtime.ensure_epoch(7, "state-pinned")
        state[0] = "state-b"
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        assert runtime.epoch_bindings(epoch)[0].state_revision == "state-a"
    finally:
        store.close()


def test_expired_manager_claim_recovers_without_partial_visibility(tmp_path):
    store, runtime, _, start = _runtime(tmp_path)
    try:
        epoch = runtime.ensure_epoch(1, "crash")
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        abandoned = runtime.claim_next_work("manager-a", duration=timedelta(milliseconds=100))
        snapshot = runtime.snapshot(abandoned)
        time.sleep(0.2)
        manager_b = TopologyEpochDuckDB(store)
        recovered = manager_b.claim_next_work("manager-b")
        assert recovered and recovered.attempt == abandoned.attempt + 1
        with pytest.raises(EpochClaimError, match="stale"):
            runtime.commit_work(snapshot, pa.Table.from_pylist([{
                "ref_uri": "urn:derived", "ts": start, "numeric_value": 9.0, "text_value": None,
            }]), abandoned)
        assert runtime.epoch_summary(epoch).sealed_component_count == 0
    finally:
        store.close()


def test_independent_partitions_can_be_claimed_concurrently(tmp_path):
    store = DuckDBStore(tmp_path / "parallel.duckdb", recreate=True)
    publisher = PublicationDuckDB(store)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publisher.publish(PublicationRequest("parallel", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=minute),
         "numeric_value": 1.0, "text_value": None}
        for minute in range(21)
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    _deploy(runtime, definition_for(add_one, name="parallel", inputs={"input": "urn:raw"},
                                    outputs={"output": "urn:derived"}, impact=pointwise()))
    try:
        epoch = runtime.ensure_epoch(1, "parallel")
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        first = runtime.claim_next_work("worker-a")
        second = runtime.claim_next_work("worker-b")
        assert first is not None and second is not None
        assert first.target_id != second.target_id
    finally:
        store.close()


def test_failed_partition_yields_to_fresh_work_and_is_dead_lettered(tmp_path):
    store = DuckDBStore(tmp_path / "retry.duckdb", recreate=True)
    publisher = PublicationDuckDB(store)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publisher.publish(PublicationRequest("retry", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=minute),
         "numeric_value": 1.0, "text_value": None}
        for minute in range(21)
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    _deploy(runtime, definition_for(add_one, name="retry", inputs={"input": "urn:raw"},
                                    outputs={"output": "urn:derived"}, impact=pointwise()))
    try:
        epoch = runtime.ensure_epoch(1, "retry")
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        poison = runtime.claim_next_work("worker")
        runtime.fail_work(poison, {"type": "deterministic"}, retry_after=timedelta(0), max_attempts=2)

        fresh = runtime.claim_next_work("worker")
        assert fresh is not None and fresh.target_id != poison.target_id
        runtime.release_claim(fresh)
        retry = runtime.claim_next_work("worker")
        assert retry is not None and retry.target_id == poison.target_id
        runtime.fail_work(retry, {"type": "deterministic"}, retry_after=timedelta(0), max_attempts=2)

        with store._own_conn() as conn:
            assert conn.execute("SELECT status FROM topology_epoch_work WHERE work_id = ?",
                                [poison.target_id]).fetchone() == ("failed",)
    finally:
        store.close()


def test_claim_renewal_preserves_owner_fence(tmp_path):
    store, runtime, _, _ = _runtime(tmp_path)
    try:
        epoch = runtime.ensure_epoch(1, "renew")
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        claim = runtime.claim_next_work("slow-worker", duration=timedelta(milliseconds=80))
        time.sleep(0.04)
        renewed = runtime.renew_claim(claim, duration=timedelta(milliseconds=100))
        time.sleep(0.06)
        assert runtime.claim_next_work("other") is None
        assert renewed.expires_at > claim.expires_at
        runtime.release_claim(renewed)
    finally:
        store.close()


def test_epoch_execution_stages_then_seals_through_publication(tmp_path):
    store, runtime, _, start = _runtime(tmp_path)
    reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
    try:
        epoch = reconciler.ensure_graph_epoch(1, "first")
        assert reconciler.run_until_idle("worker") == 3
        assert runtime.active_epoch_id() == epoch
        with store._own_conn() as conn:
            assert conn.execute("""SELECT t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:derived' AND NOT t.deleted""").fetchall() == [(3.0,)]
            assert conn.execute("SELECT count(*) FROM topology_epoch_outputs").fetchone() == (1,)
    finally:
        reconciler.close()
        store.close()


def test_lookback_reads_across_partition_boundaries(tmp_path):
    store = DuckDBStore(tmp_path / "window.duckdb", recreate=True)
    publisher = PublicationDuckDB(store)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publisher.publish(PublicationRequest("window-raw", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=minute),
         "numeric_value": 1.0, "text_value": None}
        for minute in range(21)
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    _deploy(runtime, definition_for(
        rolling_five_minutes, name="rolling-five", inputs={"input": "urn:raw"},
        outputs={"output": "urn:rolling"}, impact=lookback(timedelta(minutes=5)),
    ))
    reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
    try:
        reconciler.ensure_graph_epoch(1, "window")
        reconciler.run_until_idle("window-worker")
        with store._own_conn() as conn:
            value = conn.execute("""SELECT t.numeric_value FROM timeseries t
                JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:rolling' AND t.ts = ? AND NOT t.deleted""",
                [(start + timedelta(minutes=15)).replace(tzinfo=None)]).fetchone()
        assert value == (6.0,)
    finally:
        reconciler.close()
        store.close()


def test_late_change_propagates_window_through_managed_input(tmp_path):
    store = DuckDBStore(tmp_path / "window-dag.duckdb", recreate=True)
    publisher = PublicationDuckDB(store)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publisher.publish(PublicationRequest("dag-raw", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=minute),
         "numeric_value": 1.0, "text_value": None}
        for minute in range(21)
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    _deploy(runtime, definition_for(
        add_one_a, name="upstream", inputs={"input": "urn:raw"},
        outputs={"output": "urn:a"}, impact=pointwise(),
    ))
    _deploy(runtime, definition_for(
        rolling_five_minutes, name="downstream", inputs={"input": "urn:a"},
        outputs={"output": "urn:rolling"}, impact=lookback(timedelta(minutes=5)),
    ))
    reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
    try:
        reconciler.ensure_graph_epoch(1, "window-dag")
        reconciler.run_until_idle("initial-worker")
        publisher.publish(PublicationRequest("late-correction", pa.Table.from_pylist([{
            "operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=15),
            "numeric_value": 10.0, "text_value": None,
        }], schema=MUTATION_SCHEMA)))
        assert runtime.plan_data_changes() == 2
        reconciler.run_until_idle("late-worker")
        with store._own_conn() as conn:
            value = conn.execute("""SELECT t.numeric_value FROM timeseries t
                JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:rolling' AND t.ts = ? AND NOT t.deleted""",
                [(start + timedelta(minutes=15)).replace(tzinfo=None)]).fetchone()
        assert value == (21.0,)
    finally:
        reconciler.close()
        store.close()


def test_superseded_claim_cannot_publish_and_new_epoch_replaces_component(tmp_path):
    store, runtime, _, start = _runtime(tmp_path)
    try:
        old = runtime.ensure_epoch(1, "old")
        runtime.construct_epoch(old, Graph(("urn:raw",)))
        claim = runtime.claim_next_work("old-worker")
        snapshot = runtime.snapshot(claim)
        new = runtime.ensure_epoch(2, "new")
        runtime.construct_epoch(new, Graph(("urn:raw",)))
        with pytest.raises(StaleEpochError):
            runtime.commit_work(snapshot, pa.table({
                "ref_uri": ["urn:derived"], "ts": [start],
                "numeric_value": [99.0], "text_value": [None],
            }), claim)
        with store._own_conn() as conn:
            assert conn.execute("""SELECT count(*) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:derived' AND NOT t.deleted""").fetchone() == (0,)
        reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
        try:
            assert reconciler.run_until_idle("new-worker") == 2
            assert runtime.active_epoch_id() == new
        finally:
            reconciler.close()
    finally:
        store.close()


def test_data_publication_appends_manifest_work_to_the_active_epoch(tmp_path):
    store, runtime, _, start = _runtime(tmp_path)
    reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
    try:
        epoch = reconciler.ensure_graph_epoch(1, "data")
        reconciler.run_until_idle("worker")
        PublicationDuckDB(store).publish(PublicationRequest("raw-2", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=1),
             "numeric_value": 5.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        assert runtime.plan_data_changes() == 1
        assert reconciler.run_until_idle("worker") == 2
        assert runtime.active_epoch_id() == epoch
        with store._own_conn() as conn:
            assert conn.execute("""SELECT t.ts, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:derived' AND NOT t.deleted ORDER BY t.ts""").fetchall() == [
                    (start.replace(tzinfo=None), 3.0),
                    ((start + timedelta(minutes=1)).replace(tzinfo=None), 6.0),
                ]
    finally:
        reconciler.close()
        store.close()


def test_restart_recovers_publication_committed_before_planning(tmp_path):
    store, runtime, _, start = _runtime(tmp_path)
    graph = Graph(("urn:raw",))
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        reconciler.ensure_graph_epoch(1, "recovery")
        reconciler.run_until_idle("initial")
        PublicationDuckDB(store).publish(PublicationRequest("unplanned", pa.Table.from_pylist([{
            "operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=1),
            "numeric_value": 8.0, "text_value": None,
        }], schema=MUTATION_SCHEMA)))

        recovered = TopologyEpochDuckDB(store)
        recovered_reconciler = TopologyEpochReconciler(recovered, graph)
        try:
            assert recovered.plan_data_changes() == 1
            assert recovered_reconciler.run_until_idle("recovered") == 2
        finally:
            recovered_reconciler.close()
    finally:
        reconciler.close()
        store.close()


def test_startup_graph_recovery_does_not_reopen_active_epoch(tmp_path):
    store, runtime, _, _ = _runtime(tmp_path)
    reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
    try:
        epoch = reconciler.ensure_graph_epoch(1, "stable-graph")
        reconciler.run_until_idle("initial")
        assert runtime.ensure_epoch(1, "stable-graph") == epoch
        assert runtime.candidate_epoch_id() is None
        assert runtime.current_epoch_id() == epoch
    finally:
        reconciler.close()
        store.close()


def test_cycles_are_rejected_before_any_epoch_binding_is_visible(tmp_path):
    store = DuckDBStore(tmp_path / "cycle.duckdb", recreate=True)
    runtime = TopologyEpochDuckDB(store)
    try:
        first = definition_for(add_one, name="first", inputs={"input": "urn:b"}, outputs={"output": "urn:a"}, impact=pointwise())
        second = definition_for(add_one, name="second", inputs={"input": "urn:a"}, outputs={"output": "urn:b"}, impact=pointwise())
        _deploy(runtime, first, Graph(()))
        with pytest.raises(ValueError, match="cycle"):
            _deploy(runtime, second, Graph(()))
        with store._own_conn() as conn:
            assert conn.execute("SELECT name FROM topology_deployments ORDER BY name").fetchall() == [("first",)]
    finally:
        store.close()


def test_deployment_update_is_validated_before_it_changes_desired_topology(tmp_path):
    store, runtime, first, _ = _runtime(tmp_path)
    graph = Graph(("urn:raw",))
    try:
        old = runtime.ensure_epoch(1, "old")
        runtime.construct_epoch(old, graph)
        invalid = definition_for(
            add_two, name=first.name, inputs={"input": "urn:derived"},
            outputs={"output": "urn:derived"}, impact=pointwise(),
        )
        invalid_id = runtime.register_definition(invalid)
        with pytest.raises(ValueError, match="self-cycle"):
            runtime.deploy_definition(first.name, invalid_id, graph)
        with store._own_conn() as conn:
            deployed = conn.execute(
                "SELECT definition_id, generation FROM topology_deployments WHERE name = ?",
                [first.name],
            ).fetchone()
        assert deployed == (first.definition_id, 1)
        assert runtime.current_epoch_id() == old

        second = definition_for(
            add_two, name=first.name, inputs={"input": "urn:raw"},
            outputs={"output": "urn:derived"}, impact=pointwise(),
        )
        second_id = runtime.register_definition(second)
        assert runtime.deploy_definition(first.name, second_id, graph) == 2
        candidate = runtime.ensure_epoch(1, "same-graph")
        assert runtime.current_epoch_id() == old
        runtime.construct_epoch(candidate, graph)
        assert runtime.current_epoch_id() == candidate
        assert runtime.epoch_summary(old).status == "superseded"
    finally:
        store.close()


def test_removing_deployment_retires_its_output(tmp_path):
    store, runtime, definition, _ = _runtime(tmp_path)
    graph = Graph(("urn:raw",))
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        reconciler.ensure_graph_epoch(1, "with-deployment")
        reconciler.run_until_idle("initial")
        runtime.remove_deployment(definition.name, graph)
        removed = reconciler.ensure_graph_epoch(1, "without-deployment")
        reconciler.run_until_idle("remove")
        assert runtime.active_epoch_id() == removed
        with store._own_conn() as conn:
            assert conn.execute("""SELECT count(*) FROM timeseries t JOIN ref_ids r
                ON r.ref_id = t.ref_id WHERE r.ref_uri = 'urn:derived' AND NOT t.deleted""").fetchone() == (0,)
    finally:
        reconciler.close()
        store.close()


def test_dependency_frontier_executes_upstream_before_downstream_and_seals_together(tmp_path):
    store = DuckDBStore(tmp_path / "dag.duckdb", recreate=True)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("dag-raw", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:raw", "ts": start,
             "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = TopologyEpochDuckDB(store)
        _deploy(runtime, definition_for(add_one_a, name="a", inputs={"input": "urn:raw"}, outputs={"output": "urn:a"}, impact=pointwise()), Graph(()))
        _deploy(runtime, definition_for(add_one_b, name="b", inputs={"input": "urn:a"}, outputs={"output": "urn:b"}, impact=pointwise()), Graph(()))
        reconciler = TopologyEpochReconciler(runtime, Graph(()))
        try:
            epoch = reconciler.ensure_graph_epoch(1, "dag")
            assert reconciler.run_until_idle("dag-worker") == 4
            summary = runtime.epoch_summary(epoch)
            assert summary.component_count == 1 and summary.sealed_component_count == 1
            with store._own_conn() as conn:
                assert conn.execute("""SELECT r.ref_uri, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                    WHERE r.ref_uri IN ('urn:a', 'urn:b') AND NOT t.deleted ORDER BY r.ref_uri""").fetchall() == [("urn:a", 3.0), ("urn:b", 4.0)]
        finally:
            reconciler.close()
    finally:
        store.close()


def test_dependency_path_never_exposes_mixed_epoch_outputs(tmp_path):
    store = DuckDBStore(tmp_path / "epoch-boundary.duckdb", recreate=True)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("boundary-raw", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:raw", "ts": start,
             "numeric_value": 2.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = TopologyEpochDuckDB(store)
        _deploy(runtime, definition_for(add_one_a, name="boundary-a", inputs={"input": "urn:raw"}, outputs={"output": "urn:a"}, impact=pointwise()), Graph(()))
        _deploy(runtime, definition_for(add_one_b, name="boundary-b", inputs={"input": "urn:a"}, outputs={"output": "urn:b"}, impact=pointwise()), Graph(()))
        graph = Graph(())
        first = TopologyEpochReconciler(runtime, graph)
        first.ensure_graph_epoch(1, "boundary-old")
        first.run_until_idle("old")
        PublicationDuckDB(store).publish(PublicationRequest("boundary-update", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:raw", "ts": start,
             "numeric_value": 10.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime.plan_data_changes()
        second = TopologyEpochReconciler(runtime, graph)
        try:
            epoch = second.ensure_graph_epoch(2, "boundary-new")
            runtime.construct_epoch(epoch, graph)
            source_claim = runtime.claim_next_work("new-source")
            source_snapshot = runtime.snapshot(source_claim)
            runtime.commit_work(source_snapshot, add_one_a(source_snapshot.inputs, None), source_claim)
            with store._own_conn() as conn:
                assert conn.execute("""SELECT r.ref_uri, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                    WHERE r.ref_uri IN ('urn:a', 'urn:b') AND NOT t.deleted ORDER BY r.ref_uri""").fetchall() == [
                    ("urn:a", 3.0), ("urn:b", 4.0)]
            target_claim = runtime.claim_next_work("new-target")
            target_snapshot = runtime.snapshot(target_claim)
            runtime.commit_work(target_snapshot, add_one_b(target_snapshot.inputs, None), target_claim)
            runtime.seal_component(runtime.claim_next_component("new-sealer"))
            with store._own_conn() as conn:
                assert conn.execute("""SELECT r.ref_uri, t.numeric_value FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                    WHERE r.ref_uri IN ('urn:a', 'urn:b') AND NOT t.deleted ORDER BY r.ref_uri""").fetchall() == [
                    ("urn:a", 11.0), ("urn:b", 12.0)]
        finally:
            second.close()
        first.close()
    finally:
        store.close()


def test_superseding_epoch_retires_outputs_removed_by_late_binding(tmp_path):
    store = DuckDBStore(tmp_path / "retire.duckdb", recreate=True)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        PublicationDuckDB(store).publish(PublicationRequest("retire-raw", pa.Table.from_pylist([
            {"operation": "upsert", "ref_uri": "urn:raw", "ts": start,
             "numeric_value": 1.0, "text_value": None},
        ], schema=MUTATION_SCHEMA)))
        runtime = TopologyEpochDuckDB(store)
        definition = definition_for(add_one, name="late", inputs={"criteria": {"sparql": "q"}},
                                    outputs={"name": "late"}, impact=pointwise())
        _deploy(runtime, definition, Graph(("urn:raw",)))
        old_reconciler = TopologyEpochReconciler(runtime, Graph(("urn:raw",)))
        try:
            old = old_reconciler.ensure_graph_epoch(1, "retire-old")
            runtime.construct_epoch(old, Graph(("urn:raw",)))
            work_claim = runtime.claim_next_work("old")
            snapshot = runtime.snapshot(work_claim)
            old_ref = snapshot.binding.output_refs[0]
            runtime.commit_work(snapshot, pa.Table.from_pylist([{
                "ref_uri": old_ref, "ts": start, "numeric_value": 2.0, "text_value": None,
            }]), work_claim)
            runtime.seal_component(runtime.claim_next_component("old-sealer"))
        finally:
            old_reconciler.close()
        new_reconciler = TopologyEpochReconciler(runtime, Graph(()))
        try:
            new = new_reconciler.ensure_graph_epoch(2, "retire-new")
            assert new_reconciler.run_until_idle("new") == 2
            assert runtime.active_epoch_id() == new
            with store._own_conn() as conn:
                assert conn.execute("""SELECT count(*) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                    WHERE r.ref_uri = ? AND NOT t.deleted""", [old_ref]).fetchone() == (0,)
        finally:
            new_reconciler.close()
    finally:
        store.close()


def test_fault_after_commit_leaves_a_recoverable_seal(tmp_path):
    store, runtime, _, _ = _runtime(tmp_path)
    fired = []

    def fail_after_seal(name):
        fired.append(name)
        if name == "component_sealed":
            raise RuntimeError("simulated process stop")

    runtime._transition_hook = fail_after_seal
    try:
        epoch = runtime.ensure_epoch(1, "fault-seal")
        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        claim = runtime.claim_next_work("fault-worker")
        snapshot = runtime.snapshot(claim)
        runtime.commit_work(snapshot, add_one(snapshot.inputs, None), claim)
        seal = runtime.claim_next_component("fault-sealer")
        with pytest.raises(RuntimeError, match="simulated process stop"):
            runtime.seal_component(seal)

        recovered = TopologyEpochDuckDB(store)
        assert recovered.active_epoch_id() == epoch
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM topology_epoch_components WHERE status = 'sealed'").fetchone() == (1,)
            assert conn.execute("""SELECT count(*) FROM timeseries t JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:derived' AND NOT t.deleted""").fetchone() == (1,)
        assert "component_sealed" in fired
    finally:
        store.close()


@pytest.mark.parametrize("transition", [
    "definition_registered",
    "epoch_ensured",
    "epoch_constructed",
    "claim_acquired",
    "claim_released",
    "work_claimed",
    "work_failed",
    "work_committed",
    "component_sealed",
    "data_frontier_planned",
    "epochs_compacted",
])
def test_fault_after_each_epoch_transition_recovers_without_partial_visibility(tmp_path, transition):
    """A process stop after any committed transition leaves durable work recoverable."""
    case = tmp_path / transition
    case.mkdir()

    def crash_after(name):
        if name == transition:
            raise RuntimeError(f"crash after {name}")

    if transition == "definition_registered":
        store = DuckDBStore(case / "epochs.duckdb", recreate=True)
        definition = definition_for(add_one, name="fault-definition", inputs={"input": "urn:raw"},
                                    outputs={"output": "urn:derived"}, impact=pointwise())
        runtime = TopologyEpochDuckDB(store, transition_hook=crash_after)
        try:
            with pytest.raises(RuntimeError, match=transition):
                runtime.register_definition(definition)
            recovered = TopologyEpochDuckDB(store)
            with store._own_conn() as conn:
                assert conn.execute("SELECT count(*) FROM topology_epoch_definitions WHERE definition_id = ?",
                                    [definition.definition_id]).fetchone() == (1,)
            recovered.close()
        finally:
            store.close()
        return

    store, runtime, definition, start = _runtime(case)
    try:
        if transition == "epoch_ensured":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.ensure_epoch(1, transition)
            recovered = TopologyEpochDuckDB(store)
            assert recovered.ensure_epoch(1, transition) == recovered.candidate_epoch_id()
            return

        epoch = runtime.ensure_epoch(1, transition)
        if transition == "epoch_constructed":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.construct_epoch(epoch, Graph(("urn:raw",)))
            recovered = TopologyEpochDuckDB(store)
            assert recovered.epoch_summary(epoch).status == "reconciling"
            return
        if transition == "epochs_compacted":
            newer = runtime.ensure_epoch(2, "newer")
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.compact(2)
            recovered = TopologyEpochDuckDB(store)
            assert recovered.epoch_summary(epoch).status == "compacted"
            assert recovered.candidate_epoch_id() == newer
            return

        runtime.construct_epoch(epoch, Graph(("urn:raw",)))
        if transition in {"claim_acquired", "work_claimed"}:
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.claim_next_work("crash-worker", duration=timedelta(milliseconds=20))
            time.sleep(0.05)
            recovered = TopologyEpochDuckDB(store)
            assert recovered.claim_next_work("recovery-worker") is not None
            return

        claim = runtime.claim_next_work("worker")
        assert claim is not None
        if transition == "claim_released":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.release_claim(claim)
            recovered = TopologyEpochDuckDB(store)
            assert recovered.claim_next_work("recovery-worker") is not None
            return

        if transition == "work_failed":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.fail_work(claim, {"type": "simulated"}, retry_after=timedelta(0))
            recovered = TopologyEpochDuckDB(store)
            assert recovered.claim_next_work("recovery-worker") is not None
            return

        snapshot = runtime.snapshot(claim)
        if transition == "work_committed":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.commit_work(snapshot, add_one(snapshot.inputs, None), claim)
            recovered = TopologyEpochDuckDB(store)
            seal = recovered.claim_next_component("recovery-sealer")
            assert seal is not None
            recovered.seal_component(seal)
            assert recovered.active_epoch_id() == epoch
            return

        runtime.commit_work(snapshot, add_one(snapshot.inputs, None), claim)
        seal = runtime.claim_next_component("sealer")
        assert seal is not None
        if transition == "component_sealed":
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.seal_component(seal)
            recovered = TopologyEpochDuckDB(store)
            assert recovered.active_epoch_id() == epoch
            return

        if transition == "data_frontier_planned":
            runtime.seal_component(seal)
            PublicationDuckDB(store).publish(PublicationRequest("fault-data", pa.Table.from_pylist([
                {"operation": "upsert", "ref_uri": "urn:raw", "ts": start + timedelta(minutes=1),
                 "numeric_value": 5.0, "text_value": None},
            ], schema=MUTATION_SCHEMA)))
            runtime._transition_hook = crash_after
            with pytest.raises(RuntimeError, match=transition):
                runtime.plan_data_changes()
            recovered = TopologyEpochDuckDB(store)
            assert TopologyEpochReconciler(recovered, Graph(("urn:raw",))).run_until_idle("recovery-worker") == 2
            return

    finally:
        store.close()


def test_compaction_removes_only_superseded_private_overlay(tmp_path):
    store, runtime, _, _ = _runtime(tmp_path)
    try:
        old = runtime.ensure_epoch(1, "compact-old")
        runtime.construct_epoch(old, Graph(("urn:raw",)))
        new = runtime.ensure_epoch(2, "compact-new")
        runtime.construct_epoch(new, Graph(("urn:raw",)))
        assert runtime.compact(2) == 1
        assert runtime.epoch_summary(old).status == "compacted"
        assert runtime.current_epoch_id() == new
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM topology_epoch_work WHERE epoch_id = ?", [old]).fetchone() == (0,)
            assert conn.execute("SELECT compaction_watermark FROM topology_epoch_control").fetchone() == (2,)
    finally:
        store.close()
