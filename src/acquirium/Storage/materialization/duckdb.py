"""DuckDB persistence for range manifests.

The class is intentionally narrow: canonical publication remains owned by the
existing storage layer, which calls this seam in the same transaction.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from datetime import datetime
import json
from typing import Sequence
import pyarrow as pa

from acquirium.Storage.materialization.types import StreamChangeRange
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.ids import materialization_id, partition_ranges
from acquirium.Storage.materialization.types import PlanPartition, WorkLease
from acquirium.Storage.materialization.types import InputSnapshot
from acquirium.Storage.continuous.types import MUTATION_SCHEMA, PublicationRequest
from acquirium.Storage.artifacts import ArtifactRecord
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentArtifact, ExperimentRun, ExperimentRunRequest, run_output_ref


STREAM_CHANGE_RANGES_TABLE = "stream_change_ranges"

class StaleAttemptError(RuntimeError):
    """A newer intersecting canonical input change invalidated this attempt."""


class MaterializationDuckDB:
    def __init__(self, store: DuckDBStore) -> None:
        self._store = store
        with store._lock, store._write_conn() as conn:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {STREAM_CHANGE_RANGES_TABLE} (
                    ref_uri VARCHAR NOT NULL,
                    stream_version BIGINT NOT NULL,
                    publication_id VARCHAR NOT NULL,
                    start_ts TIMESTAMP NOT NULL,
                    end_ts TIMESTAMP NOT NULL,
                    change_kind VARCHAR NOT NULL,
                    row_count BIGINT NOT NULL,
                    PRIMARY KEY (ref_uri, stream_version, start_ts, end_ts)
                )"""
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_change_ranges_ref_version ON {STREAM_CHANGE_RANGES_TABLE} (ref_uri, stream_version)")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_definitions (
                definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, kind VARCHAR NOT NULL,
                source_digest VARCHAR NOT NULL, entrypoint VARCHAR NOT NULL, spec_json VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_deployments (
                name VARCHAR PRIMARY KEY, definition_id VARCHAR NOT NULL, generation BIGINT NOT NULL,
                status VARCHAR NOT NULL, current_graph_revision BIGINT, updated_at TIMESTAMP NOT NULL)""")
            conn.execute("ALTER TABLE materialization_deployments ADD COLUMN IF NOT EXISTS staged_generation BIGINT")
            conn.execute("ALTER TABLE materialization_deployments ADD COLUMN IF NOT EXISTS staged_definition_id VARCHAR")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_bindings (
                binding_id VARCHAR NOT NULL, deployment_name VARCHAR NOT NULL, generation BIGINT NOT NULL,
                logical_key VARCHAR NOT NULL, content_digest VARCHAR NOT NULL, graph_revision BIGINT NOT NULL,
                resolved_metadata_json VARCHAR NOT NULL, status VARCHAR NOT NULL,
                PRIMARY KEY (binding_id, generation))""")
            conn.execute("ALTER TABLE materialization_bindings ADD COLUMN IF NOT EXISTS definition_id VARCHAR")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_binding_refs (
                binding_id VARCHAR NOT NULL, generation BIGINT NOT NULL, ref_uri VARCHAR NOT NULL,
                role VARCHAR NOT NULL, direction VARCHAR NOT NULL,
                PRIMARY KEY (binding_id, generation, ref_uri, role, direction))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_rebind_requests (
                deployment_name VARCHAR NOT NULL, graph_revision BIGINT NOT NULL, status VARCHAR NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, error_json VARCHAR,
                PRIMARY KEY (deployment_name, graph_revision))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_graph_revisions (
                graph_revision BIGINT PRIMARY KEY, source_version BIGINT NOT NULL,
                content_digest VARCHAR NOT NULL, published_at TIMESTAMP NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_plans (
                plan_id VARCHAR PRIMARY KEY, binding_id VARCHAR NOT NULL, generation BIGINT NOT NULL,
                graph_revision BIGINT NOT NULL, input_vector_json VARCHAR NOT NULL, reason_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, completed_at TIMESTAMP)""")
            conn.execute("ALTER TABLE materialization_plans ADD COLUMN IF NOT EXISTS state_revision VARCHAR")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_plan_partitions (
                partition_id VARCHAR PRIMARY KEY, plan_id VARCHAR NOT NULL, start_ts TIMESTAMP NOT NULL,
                end_ts TIMESTAMP NOT NULL, status VARCHAR NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                lease_owner VARCHAR, lease_expires_at TIMESTAMP, committed_output_id VARCHAR, error_json VARCHAR)""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_materialization_partitions_pending ON materialization_plan_partitions (status, start_ts)")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_execution_receipts (
                execution_id VARCHAR PRIMARY KEY, partition_id VARCHAR NOT NULL, attempt INTEGER NOT NULL,
                input_vector_json VARCHAR NOT NULL, output_publication_id VARCHAR, status VARCHAR NOT NULL,
                rows_read BIGINT NOT NULL, rows_written BIGINT NOT NULL, error_json VARCHAR, finished_at TIMESTAMP NOT NULL)""")
            conn.execute("ALTER TABLE materialization_execution_receipts ADD COLUMN IF NOT EXISTS state_revision VARCHAR")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_attempt_snapshots (
                partition_id VARCHAR NOT NULL, attempt INTEGER NOT NULL, input_vector_json VARCHAR NOT NULL,
                rows_read BIGINT NOT NULL, created_at TIMESTAMP NOT NULL, PRIMARY KEY (partition_id, attempt))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_binding_progress (
                binding_id VARCHAR NOT NULL, generation BIGINT NOT NULL, ref_uri VARCHAR NOT NULL,
                stream_version BIGINT NOT NULL, PRIMARY KEY (binding_id, generation, ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_staged_outputs (
                binding_id VARCHAR NOT NULL, generation BIGINT NOT NULL, partition_id VARCHAR NOT NULL,
                ref_uri VARCHAR NOT NULL, ts TIMESTAMP NOT NULL, numeric_value DOUBLE, text_value VARCHAR,
                PRIMARY KEY (binding_id, generation, ref_uri, ts))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_staged_partitions (
                binding_id VARCHAR NOT NULL, generation BIGINT NOT NULL, partition_id VARCHAR NOT NULL,
                PRIMARY KEY (binding_id, generation, partition_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_artifacts (
                digest VARCHAR PRIMARY KEY, uri VARCHAR NOT NULL, size_bytes BIGINT NOT NULL,
                media_type VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_artifact_requests (
                request_id VARCHAR PRIMARY KEY, semantic_digest VARCHAR UNIQUE NOT NULL, kind VARCHAR NOT NULL,
                deployment_name VARCHAR NOT NULL, binding_id VARCHAR NOT NULL, previous_revision VARCHAR,
                input_vector_json VARCHAR NOT NULL, range_start TIMESTAMP NOT NULL, range_end TIMESTAMP NOT NULL,
                metadata_json VARCHAR NOT NULL, status VARCHAR NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                lease_owner VARCHAR, lease_expires_at TIMESTAMP, result_revision VARCHAR, error_json VARCHAR,
                created_at TIMESTAMP NOT NULL, completed_at TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_state_revisions (
                revision_id VARCHAR PRIMARY KEY, deployment_name VARCHAR NOT NULL, binding_id VARCHAR NOT NULL,
                parent_revision VARCHAR, artifact_digest VARCHAR NOT NULL, request_id VARCHAR NOT NULL,
                policy VARCHAR, effective_from TIMESTAMP, status VARCHAR NOT NULL, metrics_json VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL, activated_at TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_state_invalidations (
                revision_id VARCHAR PRIMARY KEY, binding_id VARCHAR NOT NULL, policy VARCHAR NOT NULL,
                effective_from TIMESTAMP, status VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_runs (
                run_id VARCHAR PRIMARY KEY, definition_id VARCHAR NOT NULL, graph_revision BIGINT NOT NULL,
                start_ts TIMESTAMP NOT NULL, end_ts TIMESTAMP NOT NULL, status VARCHAR NOT NULL,
                params_json VARCHAR NOT NULL, params_schema_json VARCHAR NOT NULL, metadata_json VARCHAR NOT NULL,
                input_vector_json VARCHAR NOT NULL, binding_snapshot_json VARCHAR NOT NULL, state_revision VARCHAR,
                started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, error_json VARCHAR,
                keep_reason VARCHAR, collected_at TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_metrics (
                run_id VARCHAR NOT NULL, name VARCHAR NOT NULL, value_json VARCHAR NOT NULL,
                recorded_at TIMESTAMP NOT NULL, PRIMARY KEY (run_id, name))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_artifacts (
                run_id VARCHAR NOT NULL, name VARCHAR NOT NULL, artifact_digest VARCHAR NOT NULL,
                metadata_json VARCHAR NOT NULL, PRIMARY KEY (run_id, name))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_outputs (
                run_id VARCHAR NOT NULL, name VARCHAR NOT NULL, ref_uri VARCHAR NOT NULL,
                PRIMARY KEY (run_id, name), UNIQUE (ref_uri))""")

    def record_change_ranges(self, ranges: Sequence[StreamChangeRange]) -> None:
        if not ranges:
            return
        with self._store._lock, self._store._write_conn() as conn:
            conn.executemany(
                f"""INSERT INTO {STREAM_CHANGE_RANGES_TABLE}
                (ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""",
                [(r.ref_uri, r.stream_version, r.publication_id,
                  r.interval.start.replace(tzinfo=None), r.interval.end.replace(tzinfo=None),
                  r.change_kind, r.row_count) for r in ranges],
            )

    def change_ranges(self, ref_uri: str, *, after_version: int, through_version: int) -> tuple[StreamChangeRange, ...]:
        with self._store._own_conn() as conn:
            rows = conn.execute(
                f"""SELECT ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count
                FROM {STREAM_CHANGE_RANGES_TABLE}
                WHERE ref_uri = ? AND stream_version > ? AND stream_version <= ?
                ORDER BY stream_version, start_ts""", [ref_uri, after_version, through_version]
            ).fetchall()
        from acquirium.Materialization.impact import TimeRange
        return tuple(StreamChangeRange(ref, version, pub,
                   TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), kind, count)
                     for ref, version, pub, start, end, kind, count in rows)

    def register_definition(self, definition: MaterializationDefinition) -> str:
        """Persist an immutable definition; identical registration is idempotent."""
        spec = json.dumps(definition_spec(definition), sort_keys=True)
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO materialization_definitions
                (definition_id, name, kind, source_digest, entrypoint, spec_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (definition_id) DO NOTHING""",
                [definition.definition_id, definition.name, definition.kind, definition.source_digest,
                 definition.entrypoint, spec, datetime.now(timezone.utc).replace(tzinfo=None)])
        return definition.definition_id

    def experiment_definition(self, definition_id: str) -> dict[str, object]:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT source_digest, entrypoint, kind FROM materialization_definitions WHERE definition_id = ?", [definition_id]).fetchone()
        if row is None: raise KeyError(definition_id)
        if row[2] != "experiment": raise ValueError("definition is not an experiment")
        return {"source_digest": row[0], "entrypoint": row[1]}

    def deploy(self, name: str, definition_id: str, *, graph_revision: int | None = None) -> int:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT generation, definition_id, staged_generation FROM materialization_deployments WHERE name = ?", [name]).fetchone()
            if row is not None and row[1] != definition_id and conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = ? AND status = 'active'", [name]).fetchone():
                generation = max(row[0], row[2] or row[0]) + 1
                conn.execute("UPDATE materialization_deployments SET staged_generation = ?, staged_definition_id = ?, updated_at = ? WHERE name = ?", [generation, definition_id, datetime.now(timezone.utc).replace(tzinfo=None), name])
                return generation
            generation = 1 if row is None else row[0] if row[1] == definition_id else row[0] + 1
            conn.execute("""INSERT INTO materialization_deployments
                (name, definition_id, generation, status, current_graph_revision, updated_at)
                VALUES (?, ?, ?, 'registered', ?, ?)
                ON CONFLICT (name) DO UPDATE SET definition_id = excluded.definition_id,
                    generation = excluded.generation, status = excluded.status,
                    current_graph_revision = excluded.current_graph_revision, updated_at = excluded.updated_at""",
                [name, definition_id, generation, graph_revision, datetime.now(timezone.utc).replace(tzinfo=None)])
        return generation

    def persist_bindings(self, deployment_name: str, generation: int, graph_revision: int, definition_id: str, bindings: Sequence[BindingSpec]) -> None:
        """Store a validated, complete binding generation before activation."""
        from acquirium.Materialization.bindings import validate_binding_topology
        validate_binding_topology(bindings, definition_id=definition_id)
        with self._store._lock, self._store._write_conn() as conn:
            for binding in bindings:
                binding_id = binding.binding_id(definition_id)
                conn.execute("""INSERT INTO materialization_bindings
                    (binding_id, deployment_name, generation, logical_key, content_digest, graph_revision, resolved_metadata_json, definition_id, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'staging')
                    ON CONFLICT (binding_id, generation) DO UPDATE SET content_digest = excluded.content_digest,
                    graph_revision = excluded.graph_revision, resolved_metadata_json = excluded.resolved_metadata_json, definition_id = excluded.definition_id""",
                    [binding_id, deployment_name, generation, binding.logical_key, binding.content_digest,
                     graph_revision, json.dumps(binding.metadata, sort_keys=True), definition_id])
                for direction, roles in (("input", binding.inputs), ("output", binding.outputs)):
                    for role, refs in roles.items():
                        conn.executemany("INSERT INTO materialization_binding_refs VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                                         [(binding_id, generation, ref, role, direction) for ref in refs])

    def stage_bindings(self, deployment_name: str, graph_revision: int, definition_id: str,
                       bindings: Sequence[BindingSpec]) -> int:
        """Create the next invisible topology generation without moving the active pointer."""
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT generation, staged_generation FROM materialization_deployments WHERE name = ?", [deployment_name]).fetchone()
            if row is None:
                raise KeyError(deployment_name)
            active, staged = row
            generation = max(active, staged or active) + 1
            if not conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = ? AND generation = ? AND status = 'active'", [deployment_name, active]).fetchone():
                generation = active
            conn.execute("UPDATE materialization_deployments SET staged_generation = ?, updated_at = ? WHERE name = ?", [generation, datetime.now(timezone.utc).replace(tzinfo=None), deployment_name])
        self.persist_bindings(deployment_name, generation, graph_revision, definition_id, bindings)
        return generation

    def activate_bindings(self, deployment_name: str, generation: int) -> None:
        """Atomically publish a fully staged binding generation."""
        with self._store._lock, self._store._write_conn() as conn:
            staged = conn.execute("""SELECT count(*) FROM materialization_bindings
                WHERE deployment_name = ? AND generation = ? AND status = 'staging'""",
                [deployment_name, generation]).fetchone()[0]
            staged_pointer = conn.execute("SELECT staged_generation FROM materialization_deployments WHERE name = ?", [deployment_name]).fetchone()
            if not staged and (staged_pointer is None or staged_pointer[0] != generation):
                raise ValueError("no staged bindings to activate")
            conflicts = conn.execute("""SELECT refs.ref_uri FROM materialization_binding_refs refs
                JOIN materialization_bindings binding ON binding.binding_id = refs.binding_id AND binding.generation = refs.generation
                WHERE refs.direction = 'output' AND binding.status = 'active' AND binding.deployment_name != ?
                AND refs.ref_uri IN (SELECT new_refs.ref_uri FROM materialization_binding_refs new_refs
                    JOIN materialization_bindings new_binding ON new_binding.binding_id = new_refs.binding_id AND new_binding.generation = new_refs.generation
                    WHERE new_refs.direction = 'output' AND new_binding.deployment_name = ? AND new_refs.generation = ?)
                LIMIT 1""", [deployment_name, deployment_name, generation]).fetchone()
            if conflicts:
                raise ValueError(f"output {conflicts[0]!r} is owned by another active deployment")
            active_generation = conn.execute("SELECT generation FROM materialization_deployments WHERE name = ?", [deployment_name]).fetchone()[0]
            has_active = conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = ? AND status = 'active'", [deployment_name]).fetchone() is not None
            incomplete = conn.execute("""SELECT count(*) FROM materialization_plans
                WHERE binding_id IN (SELECT binding_id FROM materialization_bindings
                    WHERE deployment_name = ? AND generation = ?) AND generation = ? AND status != 'committed'""",
                [deployment_name, generation, generation]).fetchone()[0]
            if incomplete and (has_active or active_generation != generation):
                raise ValueError("staged binding plans have not completed")
            from acquirium.Storage.continuous.duckdb import ContinuousDuckDB
            from acquirium.Storage.duckdb_store import REF_IDS_TABLE, TIMESERIES_TABLE
            mutations: list[dict] = []
            retiring_refs = [row[0] for row in conn.execute("""SELECT DISTINCT refs.ref_uri
                FROM materialization_binding_refs refs JOIN materialization_bindings binding
                ON binding.binding_id = refs.binding_id AND binding.generation = refs.generation
                WHERE binding.deployment_name = ? AND binding.status = 'active' AND binding.generation != ? AND refs.direction = 'output'""", [deployment_name, generation]).fetchall()]
            if retiring_refs:
                conn.register("_acq_retiring_refs", pa.table({"ref_uri": retiring_refs}))
                try:
                    retiring_rows = conn.execute(f"""SELECT refs.ref_uri, value.ts FROM {TIMESERIES_TABLE} value
                        JOIN {REF_IDS_TABLE} refs ON refs.ref_id = value.ref_id
                        WHERE refs.ref_uri IN (SELECT ref_uri FROM _acq_retiring_refs) AND NOT value.deleted""").fetchall()
                finally:
                    conn.unregister("_acq_retiring_refs")
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": ts.replace(tzinfo=timezone.utc), "numeric_value": None, "text_value": None} for ref, ts in retiring_rows)
            partitions = conn.execute("""SELECT staged.binding_id, staged.partition_id, part.start_ts, part.end_ts
                FROM materialization_staged_partitions staged
                JOIN materialization_plan_partitions part ON part.partition_id = staged.partition_id
                JOIN materialization_bindings binding ON binding.binding_id = staged.binding_id AND binding.generation = staged.generation
                WHERE staged.generation = ? AND binding.deployment_name = ?""", [generation, deployment_name]).fetchall()
            for binding_id, partition_id, start, end in partitions:
                refs = [row[0] for row in conn.execute("""SELECT ref_uri FROM materialization_binding_refs
                    WHERE binding_id = ? AND generation = ? AND direction = 'output'""", [binding_id, generation]).fetchall()]
                if not refs:
                    continue
                conn.register("_acq_stage_refs", pa.table({"ref_uri": refs}))
                try:
                    existing = conn.execute(f"""SELECT refs.ref_uri, value.ts FROM {TIMESERIES_TABLE} value
                        JOIN {REF_IDS_TABLE} refs ON refs.ref_id = value.ref_id
                        WHERE refs.ref_uri IN (SELECT ref_uri FROM _acq_stage_refs)
                        AND value.ts >= ? AND value.ts < ? AND NOT value.deleted""", [start, end]).fetchall()
                finally:
                    conn.unregister("_acq_stage_refs")
                staged_rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM materialization_staged_outputs
                    WHERE binding_id = ? AND generation = ? AND partition_id = ?""", [binding_id, generation, partition_id]).fetchall()
                keys = {(ref, ts) for ref, ts, _, _ in staged_rows}
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": ts.replace(tzinfo=timezone.utc), "numeric_value": None, "text_value": None}
                    for ref, ts in existing if (ref, ts) not in keys)
                mutations.extend({"operation": "upsert", "ref_uri": ref, "ts": ts.replace(tzinfo=timezone.utc), "numeric_value": numeric, "text_value": text}
                    for ref, ts, numeric, text in staged_rows)
            if mutations:
                publication_id = f"materialization:activate:{deployment_name}:{generation}"
                ContinuousDuckDB(self._store)._apply_publication(conn, publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
            conn.execute("DELETE FROM materialization_staged_outputs WHERE generation = ? AND binding_id IN (SELECT binding_id FROM materialization_bindings WHERE deployment_name = ? AND generation = ?)", [generation, deployment_name, generation])
            conn.execute("DELETE FROM materialization_staged_partitions WHERE generation = ? AND binding_id IN (SELECT binding_id FROM materialization_bindings WHERE deployment_name = ? AND generation = ?)", [generation, deployment_name, generation])
            conn.execute("UPDATE materialization_bindings SET status = 'retiring' WHERE deployment_name = ? AND status = 'active' AND generation != ?", [deployment_name, generation])
            conn.execute("UPDATE materialization_bindings SET status = 'active' WHERE deployment_name = ? AND generation = ? AND status = 'staging'", [deployment_name, generation])
            conn.execute("UPDATE materialization_deployments SET definition_id = coalesce(staged_definition_id, definition_id), staged_definition_id = NULL, generation = ?, staged_generation = NULL, current_graph_revision = (SELECT max(graph_revision) FROM materialization_bindings WHERE deployment_name = ? AND generation = ?), updated_at = ? WHERE name = ?", [generation, deployment_name, generation, datetime.now(timezone.utc).replace(tzinfo=None), deployment_name])

    def activate_ready_bindings(self) -> tuple[str, ...]:
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT deployment.name, deployment.staged_generation
                FROM materialization_deployments deployment
                WHERE deployment.staged_generation IS NOT NULL AND deployment.status = 'active'
                AND NOT EXISTS (SELECT 1 FROM materialization_plans plan
                    JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                    WHERE binding.deployment_name = deployment.name AND plan.generation = deployment.staged_generation AND plan.status != 'committed')""").fetchall()
        activated = []
        for name, generation in rows:
            self.activate_bindings(name, generation)
            activated.append(name)
        return tuple(activated)

    def record_graph_revision(self, graph_revision: int, source_version: int, content_digest: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO materialization_graph_revisions VALUES (?, ?, ?, ?)
                ON CONFLICT (graph_revision) DO NOTHING""",
                [graph_revision, source_version, content_digest,
                 datetime.now(timezone.utc).replace(tzinfo=None)])

    def request_rebind(self, deployment_name: str, graph_revision: int, *, force: bool = False) -> None:
        """Queue a durable rebind; a duplicate request is intentionally cheap."""
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO materialization_rebind_requests
                (deployment_name, graph_revision, status) VALUES (?, ?, 'pending')
                ON CONFLICT (deployment_name, graph_revision) DO UPDATE SET
                    status = 'pending', error_json = NULL
                WHERE materialization_rebind_requests.status = 'failed' OR ?""",
                [deployment_name, graph_revision, force])

    def lease_rebind(self, owner: str) -> tuple[str, int] | None:
        """Lease newest work; obsolete queued revisions can never win a race."""
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""UPDATE materialization_rebind_requests SET status = 'superseded'
                WHERE status = 'pending' AND graph_revision < (
                    SELECT max(graph_revision) FROM materialization_rebind_requests newer
                    WHERE newer.deployment_name = materialization_rebind_requests.deployment_name)""")
            row = conn.execute("""SELECT deployment_name, graph_revision FROM materialization_rebind_requests
                WHERE status = 'pending' ORDER BY graph_revision, deployment_name LIMIT 1""").fetchone()
            if row is None:
                return None
            conn.execute("""UPDATE materialization_rebind_requests SET status = 'leased', attempts = attempts + 1
                WHERE deployment_name = ? AND graph_revision = ?""", row)
        return row

    def finish_rebind(self, deployment_name: str, graph_revision: int, *, error: dict | None = None) -> None:
        status = "failed" if error is not None else "completed"
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""UPDATE materialization_rebind_requests SET status = ?, error_json = ?
                WHERE deployment_name = ? AND graph_revision = ? AND status = 'leased'""",
                [status, json.dumps(error) if error is not None else None, deployment_name, graph_revision])

    def deployment_names(self) -> tuple[str, ...]:
        with self._store._own_conn() as conn:
            return tuple(row[0] for row in conn.execute(
                "SELECT name FROM materialization_deployments WHERE status != 'failed' ORDER BY name"
            ).fetchall())

    def deployment_definition(self, name: str) -> dict[str, object]:
        """Return the current immutable bundle and generation for rebinding."""
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT d.definition_id, coalesce(deployment.staged_generation, deployment.generation), d.spec_json
                FROM materialization_deployments deployment
                JOIN materialization_definitions d ON d.definition_id = coalesce(deployment.staged_definition_id, deployment.definition_id)
                WHERE deployment.name = ?""", [name]).fetchone()
        if row is None:
            raise KeyError(name)
        definition_id, generation, spec = row
        return {"definition_id": definition_id, "generation": generation, "spec": json.loads(spec)}

    def stale_bindings(self) -> tuple[dict[str, object], ...]:
        """Return active/staging bindings whose persisted input progress trails heads."""
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE
        with self._store._own_conn() as conn:
            rows = conn.execute(f"""SELECT b.binding_id, b.deployment_name, b.generation, b.graph_revision, r.ref_uri,
                h.current_version, coalesce(p.stream_version, 0) AS progress, d.spec_json
                FROM materialization_bindings b
                JOIN materialization_deployments deployment ON deployment.name = b.deployment_name
                JOIN materialization_definitions d ON d.definition_id = coalesce(deployment.staged_definition_id, deployment.definition_id)
                JOIN materialization_binding_refs r ON r.binding_id = b.binding_id AND r.generation = b.generation AND r.direction = 'input'
                JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = r.ref_uri
                JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = ids.ref_id
                LEFT JOIN materialization_binding_progress p ON p.binding_id = b.binding_id AND p.generation = b.generation AND p.ref_uri = r.ref_uri
                WHERE b.status IN ('active', 'staging') AND h.current_version > coalesce(p.stream_version, 0)
                ORDER BY b.binding_id, r.ref_uri""").fetchall()
        grouped: dict[tuple[str, int, int], dict[str, object]] = {}
        for binding, deployment_name, generation, graph, ref, head, progress, spec in rows:
            item = grouped.setdefault((binding, generation, graph), {"binding_id": binding, "deployment_name": deployment_name, "generation": generation, "graph_revision": graph, "impact": json.loads(spec)["impact"], "heads": {}, "progress": {}})
            item["heads"][ref] = head
            item["progress"][ref] = progress
        return tuple(grouped.values())

    def bootstrap_bindings(self, deployment_name: str, generation: int) -> tuple[dict[str, object], ...]:
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
        with self._store._own_conn() as conn:
            rows = conn.execute(f"""SELECT binding.binding_id, refs.ref_uri, coalesce(head.current_version, 0),
                min(value.ts), max(value.ts)
                FROM materialization_bindings binding
                JOIN materialization_binding_refs refs ON refs.binding_id = binding.binding_id AND refs.generation = binding.generation AND refs.direction = 'input'
                LEFT JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = refs.ref_uri
                LEFT JOIN {STREAM_HEADS_TABLE} head ON head.ref_id = ids.ref_id
                LEFT JOIN {TIMESERIES_TABLE} value ON value.ref_id = ids.ref_id AND NOT value.deleted
                WHERE binding.deployment_name = ? AND binding.generation = ? AND binding.status = 'staging'
                GROUP BY binding.binding_id, refs.ref_uri, head.current_version""", [deployment_name, generation]).fetchall()
        grouped: dict[str, dict[str, object]] = {}
        for binding_id, ref, head, start, end in rows:
            item = grouped.setdefault(binding_id, {"binding_id": binding_id, "generation": generation, "heads": {}, "ranges": []})
            item["heads"][ref] = head
            if start is not None:
                item["ranges"].append((start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)))
        return tuple(grouped.values())

    def set_deployment_status(self, name: str, status: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            if conn.execute("SELECT 1 FROM materialization_deployments WHERE name = ?", [name]).fetchone() is None:
                raise KeyError(name)
            conn.execute("UPDATE materialization_deployments SET status = ?, updated_at = ? WHERE name = ?",
                         [status, datetime.now(timezone.utc).replace(tzinfo=None), name])

    def deployment_status(self, name: str) -> dict[str, object] | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT name, definition_id, generation, status, current_graph_revision FROM materialization_deployments WHERE name = ?", [name]).fetchone()
        if row is None:
            return None
        return dict(zip(("name", "definition_id", "generation", "status", "graph_revision"), row))

    def deployments(self) -> tuple[dict[str, object], ...]:
        with self._store._own_conn() as conn:
            rows = conn.execute("SELECT name, definition_id, generation, status, current_graph_revision FROM materialization_deployments ORDER BY name").fetchall()
        keys = ("name", "definition_id", "generation", "status", "graph_revision")
        return tuple(dict(zip(keys, row)) for row in rows)

    def create_plan(self, *, binding_id: str, generation: int, graph_revision: int,
                    input_vector: dict[str, int], ranges: Sequence[TimeRange],
                    reason: dict, maximum_partition_duration: timedelta) -> tuple[str, tuple[PlanPartition, ...]]:
        """Durably create idempotent semantic work and its range partitions."""
        normalized = partition_ranges(ranges, maximum_duration=maximum_partition_duration)
        plan_id = materialization_id(binding_id, generation, graph_revision, json.dumps(input_vector, sort_keys=True),
                                     [(item.start.isoformat(), item.end.isoformat()) for item in normalized], json.dumps(reason, sort_keys=True))
        partitions = tuple(PlanPartition(materialization_id(plan_id, item.start.isoformat(), item.end.isoformat()), plan_id, item) for item in normalized)
        with self._store._lock, self._store._write_conn() as conn:
            state = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = ? AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
            conn.execute("""INSERT INTO materialization_plans
                (plan_id, binding_id, generation, graph_revision, input_vector_json, reason_json, status, created_at, completed_at, state_revision)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?) ON CONFLICT (plan_id) DO NOTHING""",
                [plan_id, binding_id, generation, graph_revision, json.dumps(input_vector, sort_keys=True),
                 json.dumps(reason, sort_keys=True), datetime.now(timezone.utc).replace(tzinfo=None), state[0] if state else None])
            conn.executemany("""INSERT INTO materialization_plan_partitions
                (partition_id, plan_id, start_ts, end_ts, status) VALUES (?, ?, ?, ?, 'pending') ON CONFLICT DO NOTHING""",
                [(item.partition_id, plan_id, item.interval.start.replace(tzinfo=None), item.interval.end.replace(tzinfo=None)) for item in partitions])
        return plan_id, partitions

    def lease_partition(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> WorkLease | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        expires = now + duration
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
                WHERE status = 'leased' AND lease_expires_at <= ?""", [now])
            row = conn.execute("""SELECT partition_id, plan_id, start_ts, end_ts, attempt FROM materialization_plan_partitions
                WHERE status = 'pending' ORDER BY start_ts, partition_id LIMIT 1""").fetchone()
            if row is None:
                return None
            partition_id, plan_id, start, end, prior_attempt = row
            attempt = prior_attempt + 1
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'leased', attempt = ?, lease_owner = ?, lease_expires_at = ?
                WHERE partition_id = ? AND status = 'pending'""", [attempt, owner, expires, partition_id])
        partition = PlanPartition(partition_id, plan_id, TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), "leased")
        return WorkLease(partition, owner, attempt, expires.replace(tzinfo=timezone.utc))

    def lease_registered_partition(self, owner: str, *, deployment_name: str | None = None,
                                 duration: timedelta = timedelta(minutes=5)) -> WorkLease | None:
        """Lease pending work only when its deployment has been started."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        expires = now + duration
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
                WHERE status = 'leased' AND lease_expires_at <= ?""", [now])
            row = conn.execute("""SELECT part.partition_id, part.plan_id, part.start_ts, part.end_ts, part.attempt
                FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                JOIN materialization_deployments deployment ON deployment.name = binding.deployment_name
                WHERE part.status = 'pending' AND deployment.status = 'active' AND (? IS NULL OR deployment.name = ?)
                ORDER BY part.start_ts, part.partition_id LIMIT 1""", [deployment_name, deployment_name]).fetchone()
            if row is None:
                return None
            partition_id, plan_id, start, end, prior_attempt = row
            attempt = prior_attempt + 1
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'leased', attempt = ?, lease_owner = ?, lease_expires_at = ?
                WHERE partition_id = ? AND status = 'pending'""", [attempt, owner, expires, partition_id])
        partition = PlanPartition(partition_id, plan_id, TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), "leased")
        return WorkLease(partition, owner, attempt, expires.replace(tzinfo=timezone.utc))

    def release_partition(self, lease: WorkLease) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL WHERE partition_id = ? AND status = 'leased' AND lease_owner = ? AND attempt = ?""",
                [lease.partition.partition_id, lease.owner, lease.attempt])

    def commit_partition(self, lease: WorkLease, *, output_publication_id: str) -> bool:
        """Commit one leased partition once; duplicate commits are harmless."""
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = ?", [lease.partition.partition_id]).fetchone()
            if row is None:
                raise KeyError(lease.partition.partition_id)
            if row[0] == 'committed':
                return False
            if row != ('leased', lease.owner, lease.attempt):
                raise ValueError("partition lease is stale")
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'committed', committed_output_id = ?, lease_owner = NULL, lease_expires_at = NULL
                WHERE partition_id = ?""", [output_publication_id, lease.partition.partition_id])
            pending = conn.execute("SELECT count(*) FROM materialization_plan_partitions WHERE plan_id = ? AND status != 'committed'", [lease.partition.plan_id]).fetchone()[0]
            if pending == 0:
                conn.execute("UPDATE materialization_plans SET status = 'committed', completed_at = ? WHERE plan_id = ?", [datetime.now(timezone.utc).replace(tzinfo=None), lease.partition.plan_id])
        return True

    def snapshot_partition(self, lease: WorkLease, input_refs: Sequence[str]) -> InputSnapshot:
        """Read one Arrow input snapshot after verifying the currently held lease."""
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = ?", [lease.partition.partition_id]).fetchone()
            if row != ("leased", lease.owner, lease.attempt):
                raise ValueError("partition lease is stale")
            if not input_refs:
                return InputSnapshot(lease, pa.table({name: [] for name in MUTATION_SCHEMA.names}, schema=MUTATION_SCHEMA), {})
            conn.register("_acq_snapshot_refs", pa.table({"ref_uri": list(input_refs)}))
            try:
                heads = dict(conn.execute(f"""SELECT r.ref_uri, h.current_version FROM {REF_IDS_TABLE} r
                    JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = r.ref_id
                    WHERE r.ref_uri IN (SELECT ref_uri FROM _acq_snapshot_refs)""").fetchall())
                table = conn.execute(f"""SELECT 'upsert' AS operation, r.ref_uri, t.ts,
                    t.numeric_value, t.text_value FROM {TIMESERIES_TABLE} t
                    JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
                    WHERE r.ref_uri IN (SELECT ref_uri FROM _acq_snapshot_refs)
                    AND t.ts >= ? AND t.ts < ? AND NOT t.deleted ORDER BY r.ref_uri, t.ts""",
                    [lease.partition.interval.start.replace(tzinfo=None), lease.partition.interval.end.replace(tzinfo=None)]).arrow().read_all()
            finally:
                conn.unregister("_acq_snapshot_refs")
        table = table.cast(MUTATION_SCHEMA)
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO materialization_attempt_snapshots VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (partition_id, attempt) DO NOTHING""", [lease.partition.partition_id, lease.attempt,
                json.dumps(heads, sort_keys=True), table.num_rows, datetime.now(timezone.utc).replace(tzinfo=None)])
        return InputSnapshot(lease, table, heads)

    def partition_refs(self, partition_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT refs.direction, refs.ref_uri FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_binding_refs refs ON refs.binding_id = plan.binding_id AND refs.generation = plan.generation
                WHERE part.partition_id = ? ORDER BY refs.direction, refs.ref_uri""", [partition_id]).fetchall()
        return (tuple(ref for direction, ref in rows if direction == "input"), tuple(ref for direction, ref in rows if direction == "output"))

    def partition_definition(self, partition_id: str) -> dict[str, object]:
        """Resolve the immutable definition bundle owned by a leased partition."""
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT d.source_digest, d.entrypoint, d.spec_json FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                JOIN materialization_definitions d ON d.definition_id = binding.definition_id
                WHERE part.partition_id = ?""", [partition_id]).fetchone()
        if row is None:
            raise KeyError(partition_id)
        digest, entrypoint, spec = row
        return {"source_digest": digest, "entrypoint": entrypoint, "spec": json.loads(spec)}

    def partition_binding_metadata(self, partition_id: str) -> dict[str, object]:
        """Return the immutable resolved metadata captured with this binding generation."""
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT binding.resolved_metadata_json FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id
                    AND binding.generation = plan.generation
                WHERE part.partition_id = ?""", [partition_id]).fetchone()
        if row is None:
            raise KeyError(partition_id)
        return json.loads(row[0])

    def leased_partition(self, partition_id: str, owner: str, attempt: int) -> WorkLease:
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT plan_id, start_ts, end_ts, lease_expires_at FROM materialization_plan_partitions
                WHERE partition_id = ? AND status = 'leased' AND lease_owner = ? AND attempt = ?""", [partition_id, owner, attempt]).fetchone()
        if row is None:
            raise ValueError("partition lease is stale")
        plan_id, start, end, expires = row
        return WorkLease(PlanPartition(partition_id, plan_id, TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), "leased"), owner, attempt, expires.replace(tzinfo=timezone.utc))

    def fail_partition(self, lease: WorkLease, error: dict) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            changed = conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL, error_json = ? WHERE partition_id = ? AND status = 'leased' AND lease_owner = ? AND attempt = ?""",
                [json.dumps(error), lease.partition.partition_id, lease.owner, lease.attempt])

    def commit_replacement(self, snapshot: InputSnapshot, *, input_refs: Sequence[str],
                           output_refs: Sequence[str], replacement: pa.Table) -> str | None:
        """Atomically replace owned output keys in a partition and record receipt.

        Pointwise stale detection is deliberately conservative here: any newer
        manifest intersecting the owned range rejects the attempt. Wider
        impact expansion is applied by the scheduler before this commit path.
        """
        from acquirium.Storage.continuous.duckdb import ContinuousDuckDB
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, TIMESERIES_TABLE
        required = {"ref_uri", "ts", "numeric_value", "text_value"}
        if not required.issubset(replacement.column_names):
            raise ValueError("replacement must contain ref_uri, ts, numeric_value, and text_value")
        rows = replacement.select(["ref_uri", "ts", "numeric_value", "text_value"]).to_pylist()
        if any(row["ref_uri"] not in output_refs for row in rows):
            raise ValueError("replacement includes an unowned output")
        interval = snapshot.lease.partition.interval
        if any(not (interval.start <= row["ts"] < interval.end) for row in rows):
            raise ValueError("replacement lies outside its owned partition")
        execution_id = materialization_id(snapshot.lease.partition.partition_id, snapshot.lease.attempt)
        with self._store._lock, self._store._write_conn() as conn:
            persisted = conn.execute("SELECT input_vector_json FROM materialization_attempt_snapshots WHERE partition_id = ? AND attempt = ?", [snapshot.lease.partition.partition_id, snapshot.lease.attempt]).fetchone()
            if persisted is None:
                raise ValueError("snapshot attempt was not recorded")
            snapshot = InputSnapshot(snapshot.lease, snapshot.inputs, json.loads(persisted[0]))
            state = conn.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = ?", [snapshot.lease.partition.partition_id]).fetchone()
            if state == ("committed", None, snapshot.lease.attempt):
                receipt = conn.execute("SELECT output_publication_id FROM materialization_execution_receipts WHERE execution_id = ?", [execution_id]).fetchone()
                return receipt[0] if receipt else None
            if state != ("leased", snapshot.lease.owner, snapshot.lease.attempt):
                raise ValueError("partition lease is stale")
            binding = conn.execute("""SELECT plan.binding_id, plan.generation, binding.status
                FROM materialization_plans plan JOIN materialization_bindings binding
                ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                WHERE plan.plan_id = ?""", [snapshot.lease.partition.plan_id]).fetchone()
            binding_id, generation, binding_status = binding or (None, None, "active")
            reason = conn.execute("SELECT reason_json FROM materialization_plans WHERE plan_id = ?", [snapshot.lease.partition.plan_id]).fetchone()[0]
            from acquirium.Materialization.impact import ImpactPolicy
            impact = ImpactPolicy.from_json(json.loads(reason).get("impact", {"kind": "pointwise"}))
            for ref_uri, version in snapshot.input_versions.items():
                newer = conn.execute(f"""SELECT start_ts, end_ts FROM {STREAM_CHANGE_RANGES_TABLE}
                    WHERE ref_uri = ? AND stream_version > ?""", [ref_uri, version]).fetchall()
                for start, end in newer:
                    dirty = impact.affected(TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)))
                    if dirty.intersects(interval):
                        raise StaleAttemptError("a newer intersecting input change exists")
            if binding_status == "staging":
                conn.execute("INSERT INTO materialization_staged_partitions VALUES (?, ?, ?) ON CONFLICT DO NOTHING", [binding_id, generation, snapshot.lease.partition.partition_id])
                conn.execute("""DELETE FROM materialization_staged_outputs WHERE binding_id = ? AND generation = ?
                    AND ref_uri IN (SELECT ref_uri FROM materialization_binding_refs WHERE binding_id = ? AND generation = ? AND direction = 'output')
                    AND ts >= ? AND ts < ?""", [binding_id, generation, binding_id, generation,
                    interval.start.replace(tzinfo=None), interval.end.replace(tzinfo=None)])
                if rows:
                    conn.executemany("""INSERT INTO materialization_staged_outputs VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (binding_id, generation, ref_uri, ts) DO UPDATE SET partition_id = excluded.partition_id,
                        numeric_value = excluded.numeric_value, text_value = excluded.text_value""",
                        [(binding_id, generation, snapshot.lease.partition.partition_id, row["ref_uri"], row["ts"].replace(tzinfo=None), row["numeric_value"], row["text_value"]) for row in rows])
                publication_id = f"staged:{execution_id}"
            else:
                publication_id = None
                existing: list[dict] = []
                if output_refs:
                    conn.register("_acq_replace_refs", pa.table({"ref_uri": list(output_refs)}))
                    try:
                        existing_rows = conn.execute(f"""SELECT r.ref_uri, t.ts FROM {TIMESERIES_TABLE} t
                            JOIN {REF_IDS_TABLE} r ON r.ref_id = t.ref_id
                            WHERE r.ref_uri IN (SELECT ref_uri FROM _acq_replace_refs)
                            AND t.ts >= ? AND t.ts < ? AND NOT t.deleted""",
                            [interval.start.replace(tzinfo=None), interval.end.replace(tzinfo=None)]).fetchall()
                    finally:
                        conn.unregister("_acq_replace_refs")
                    replacement_keys = {(row["ref_uri"], row["ts"].replace(tzinfo=None)) for row in rows}
                    existing = [{"operation": "delete", "ref_uri": ref, "ts": ts.replace(tzinfo=timezone.utc), "numeric_value": None, "text_value": None}
                                for ref, ts in existing_rows if (ref, ts) not in replacement_keys]
                mutations = existing + [{"operation": "upsert", **row} for row in rows]
                if mutations:
                    publication_id = f"materialization:{execution_id}"
                    ContinuousDuckDB(self._store)._apply_publication(conn, publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'committed', committed_output_id = ?,
                lease_owner = NULL, lease_expires_at = NULL WHERE partition_id = ?""", [publication_id, snapshot.lease.partition.partition_id])
            pinned = conn.execute("SELECT state_revision FROM materialization_plans WHERE plan_id = ?", [snapshot.lease.partition.plan_id]).fetchone()[0]
            conn.execute("""INSERT INTO materialization_execution_receipts
                (execution_id, partition_id, attempt, input_vector_json, output_publication_id, status, rows_read, rows_written, error_json, finished_at, state_revision)
                VALUES (?, ?, ?, ?, ?, 'committed', ?, ?, NULL, ?, ?) ON CONFLICT (execution_id) DO NOTHING""",
                [execution_id, snapshot.lease.partition.partition_id, snapshot.lease.attempt,
                 json.dumps(snapshot.input_versions, sort_keys=True), publication_id, snapshot.inputs.num_rows,
                 len(rows), datetime.now(timezone.utc).replace(tzinfo=None), pinned])
            pending = conn.execute("SELECT count(*) FROM materialization_plan_partitions WHERE plan_id = ? AND status != 'committed'", [snapshot.lease.partition.plan_id]).fetchone()[0]
            if pending == 0:
                conn.execute("UPDATE materialization_plans SET status = 'committed', completed_at = ? WHERE plan_id = ?", [datetime.now(timezone.utc).replace(tzinfo=None), snapshot.lease.partition.plan_id])
                binding_id, generation, captured = conn.execute("SELECT binding_id, generation, input_vector_json FROM materialization_plans WHERE plan_id = ?", [snapshot.lease.partition.plan_id]).fetchone()
                for ref_uri, version in json.loads(captured).items():
                    conn.execute("""INSERT INTO materialization_binding_progress VALUES (?, ?, ?, ?)
                        ON CONFLICT (binding_id, generation, ref_uri) DO UPDATE SET stream_version = greatest(materialization_binding_progress.stream_version, excluded.stream_version)""",
                        [binding_id, generation, ref_uri, version])
        return publication_id

    def create_artifact_request(self, request: ArtifactRequest) -> str:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT request_id FROM materialization_artifact_requests WHERE semantic_digest = ?", [request.semantic_digest]).fetchone()
            if row: return row[0]
            conn.execute("""INSERT INTO materialization_artifact_requests
                (request_id, semantic_digest, kind, deployment_name, binding_id, previous_revision,
                 input_vector_json, range_start, range_end, metadata_json, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                [request.request_id, request.semantic_digest, request.kind, request.deployment_name, request.binding_id,
                 request.previous_revision, json.dumps(dict(request.input_versions), sort_keys=True),
                 request.interval.start.replace(tzinfo=None), request.interval.end.replace(tzinfo=None),
                 json.dumps(dict(request.metadata), sort_keys=True), now])
        return request.request_id

    def lease_artifact_request(self, owner: str, *, duration: timedelta = timedelta(minutes=15)) -> ArtifactLease | None:
        now = datetime.now(timezone.utc).replace(tzinfo=None); expires = now + duration
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("UPDATE materialization_artifact_requests SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL WHERE status = 'leased' AND lease_expires_at <= ?", [now])
            row = conn.execute("""SELECT request_id, kind, deployment_name, binding_id, previous_revision,
                input_vector_json, range_start, range_end, metadata_json, attempt FROM materialization_artifact_requests
                WHERE status = 'pending' ORDER BY created_at, request_id LIMIT 1""").fetchone()
            if row is None: return None
            request_id, kind, deployment, binding, previous, vector, start, end, metadata, attempt = row
            attempt += 1
            conn.execute("UPDATE materialization_artifact_requests SET status = 'leased', attempt = ?, lease_owner = ?, lease_expires_at = ? WHERE request_id = ?", [attempt, owner, expires, request_id])
        request = ArtifactRequest(request_id, kind, deployment, binding, json.loads(vector), TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), previous, json.loads(metadata))
        return ArtifactLease(request, owner, attempt, expires.replace(tzinfo=timezone.utc))

    def leased_artifact_request(self, request_id: str, owner: str, attempt: int) -> ArtifactLease:
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT kind, deployment_name, binding_id, previous_revision,
                input_vector_json, range_start, range_end, metadata_json, lease_expires_at
                FROM materialization_artifact_requests WHERE request_id = ? AND status = 'leased'
                AND lease_owner = ? AND attempt = ?""", [request_id, owner, attempt]).fetchone()
        if row is None:
            raise ValueError("artifact lease is stale")
        kind, deployment, binding, previous, vector, start, end, metadata, expires = row
        request = ArtifactRequest(request_id, kind, deployment, binding, json.loads(vector),
            TimeRange(start.replace(tzinfo=timezone.utc), end.replace(tzinfo=timezone.utc)), previous,
            json.loads(metadata))
        return ArtifactLease(request, owner, attempt, expires.replace(tzinfo=timezone.utc))

    def complete_artifact_request(self, lease: ArtifactLease, artifact: ArtifactRecord, candidate: ArtifactCandidate) -> StateRevision:
        if artifact.digest != candidate.digest: raise ValueError("artifact digest does not match produced bytes")
        revision_id = materialization_id("artifact", lease.request.binding_id, lease.request.request_id, artifact.digest)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT status, lease_owner, attempt, result_revision FROM materialization_artifact_requests WHERE request_id = ?", [lease.request.request_id]).fetchone()
            if row is None: raise KeyError(lease.request.request_id)
            if row[0] == 'completed': return self.state_revision(row[3])
            if row[:3] != ('leased', lease.owner, lease.attempt): raise ValueError("artifact lease is stale")
            conn.execute("INSERT INTO materialization_artifacts VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING", [artifact.digest, artifact.uri, artifact.size_bytes, artifact.media_type, json.dumps(dict(artifact.metadata), sort_keys=True), now])
            conn.execute("""INSERT INTO materialization_state_revisions
                (revision_id, deployment_name, binding_id, parent_revision, artifact_digest, request_id, status, metrics_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'candidate', ?, ?) ON CONFLICT DO NOTHING""", [revision_id,
                lease.request.deployment_name, lease.request.binding_id, lease.request.previous_revision,
                artifact.digest, lease.request.request_id, json.dumps(dict(candidate.metrics), sort_keys=True), now])
            conn.execute("UPDATE materialization_artifact_requests SET status = 'completed', result_revision = ?, lease_owner = NULL, lease_expires_at = NULL, completed_at = ? WHERE request_id = ?", [revision_id, now, lease.request.request_id])
        return self.state_revision(revision_id)

    def fail_artifact_request(self, lease: ArtifactLease, error: dict) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("UPDATE materialization_artifact_requests SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL, error_json = ? WHERE request_id = ? AND status = 'leased' AND lease_owner = ? AND attempt = ?", [json.dumps(error, sort_keys=True), lease.request.request_id, lease.owner, lease.attempt])

    def state_revision(self, revision_id: str) -> StateRevision:
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT r.revision_id, r.deployment_name, r.binding_id, r.parent_revision,
                a.digest, a.uri, a.size_bytes, a.media_type, a.metadata_json, r.status, r.policy,
                r.effective_from, r.metrics_json FROM materialization_state_revisions r JOIN materialization_artifacts a ON a.digest = r.artifact_digest WHERE r.revision_id = ?""", [revision_id]).fetchone()
        if row is None: raise KeyError(revision_id)
        identifier, deployment, binding, parent, digest, uri, size, media, metadata, status, policy, effective, metrics = row
        return StateRevision(identifier, deployment, binding, ArtifactRecord(digest, uri, size, media, json.loads(metadata)), status, parent, policy, effective.replace(tzinfo=timezone.utc) if effective else None, json.loads(metrics))

    def promote_state_revision(self, revision_id: str, *, policy: str = 'prospective', effective_from: datetime | None = None) -> StateRevision:
        if policy not in {'prospective', 'recompute_all', 'recompute_from'}: raise ValueError('unknown promotion policy')
        if policy == 'recompute_from' and effective_from is None: raise ValueError('recompute_from requires effective_from')
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT binding_id, status FROM materialization_state_revisions WHERE revision_id = ?", [revision_id]).fetchone()
            if row is None: raise KeyError(revision_id)
            binding, status = row
            if status not in {'candidate', 'active'}: raise ValueError('only candidate revisions may be promoted')
            conn.execute("UPDATE materialization_state_revisions SET status = 'retired' WHERE binding_id = ? AND status = 'active' AND revision_id != ?", [binding, revision_id])
            conn.execute("UPDATE materialization_state_revisions SET status = 'active', policy = ?, effective_from = ?, activated_at = ? WHERE revision_id = ?", [policy, effective_from.replace(tzinfo=None) if effective_from else None, now, revision_id])
            if policy != "prospective":
                conn.execute("""INSERT INTO materialization_state_invalidations VALUES (?, ?, ?, ?, 'pending', ?)
                    ON CONFLICT (revision_id) DO NOTHING""", [revision_id, binding,
                    policy, effective_from.replace(tzinfo=None) if effective_from else None, now])
                affected = "" if policy == "recompute_all" else " AND part.end_ts > ?"
                parameters: list[object] = [binding, revision_id]
                if policy == "recompute_from":
                    parameters.append(effective_from.replace(tzinfo=None))
                conn.execute("""UPDATE materialization_plan_partitions AS part SET status = 'superseded',
                    lease_owner = NULL, lease_expires_at = NULL
                    FROM materialization_plans AS plan
                    WHERE part.plan_id = plan.plan_id AND plan.binding_id = ?
                    AND coalesce(plan.state_revision, '') != ?
                    AND part.status IN ('pending', 'leased')""" + affected, parameters)
        return self.state_revision(revision_id)

    def active_state_revision(self, binding_id: str) -> StateRevision | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = ? AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
        return self.state_revision(row[0]) if row else None

    def partition_state_revision(self, partition_id: str) -> StateRevision | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT state_revision FROM materialization_plans plan JOIN materialization_plan_partitions part ON part.plan_id = plan.plan_id WHERE part.partition_id = ?", [partition_id]).fetchone()
        return self.state_revision(row[0]) if row and row[0] else None

    def pending_state_invalidations(self) -> tuple[dict[str, object], ...]:
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
        with self._store._own_conn() as conn:
            rows = conn.execute(f"""SELECT i.revision_id, i.binding_id, i.policy, i.effective_from,
                b.generation, b.graph_revision, r.ref_uri, coalesce(h.current_version, 0), min(t.ts), max(t.ts)
                FROM materialization_state_invalidations i
                JOIN materialization_bindings b ON b.binding_id = i.binding_id AND b.status = 'active'
                JOIN materialization_binding_refs r ON r.binding_id = b.binding_id AND r.generation = b.generation AND r.direction = 'input'
                LEFT JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = r.ref_uri
                LEFT JOIN {STREAM_HEADS_TABLE} h ON h.ref_id = ids.ref_id
                LEFT JOIN {TIMESERIES_TABLE} t ON t.ref_id = ids.ref_id AND NOT t.deleted
                WHERE i.status = 'pending' GROUP BY i.revision_id, i.binding_id, i.policy, i.effective_from,
                b.generation, b.graph_revision, r.ref_uri, h.current_version""").fetchall()
        grouped: dict[str, dict[str, object]] = {}
        for revision, binding, policy, effective, generation, graph, ref, head, start, end in rows:
            item = grouped.setdefault(revision, {"revision_id": revision, "binding_id": binding, "policy": policy,
                "effective_from": effective.replace(tzinfo=timezone.utc) if effective else None,
                "generation": generation, "graph_revision": graph, "heads": {}, "ranges": []})
            item["heads"][ref] = head
            if start is not None:
                range_start = start.replace(tzinfo=timezone.utc)
                if policy == "recompute_from" and item["effective_from"]:
                    range_start = max(range_start, item["effective_from"])
                if range_start <= end.replace(tzinfo=timezone.utc):
                    item["ranges"].append(TimeRange(range_start, end.replace(tzinfo=timezone.utc) + timedelta(microseconds=1)))
        return tuple(grouped.values())

    def complete_state_invalidation(self, revision_id: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("UPDATE materialization_state_invalidations SET status = 'planned' WHERE revision_id = ? AND status = 'pending'", [revision_id])

    def artifact_digests(self) -> set[str]:
        """Return every artifact retained by a durable candidate or revision."""
        with self._store._own_conn() as conn:
            rows = conn.execute("SELECT DISTINCT artifact_digest FROM materialization_state_revisions").fetchall()
        return {row[0] for row in rows}

    def start_experiment(self, request: ExperimentRunRequest) -> ExperimentRun:
        """Persist an immutable experiment snapshot before any user code runs."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            conn.execute("""INSERT INTO experiment_runs
                (run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json,
                 params_schema_json, metadata_json, input_vector_json, binding_snapshot_json,
                 state_revision, started_at, finished_at, error_json, keep_reason, collected_at)
                VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
                ON CONFLICT (run_id) DO NOTHING""", [request.run_id, request.definition_id,
                request.graph_revision, request.interval.start.replace(tzinfo=None), request.interval.end.replace(tzinfo=None),
                json.dumps(dict(request.params), sort_keys=True), json.dumps(dict(request.params_schema), sort_keys=True),
                json.dumps(dict(request.metadata), sort_keys=True), json.dumps(dict(request.input_versions), sort_keys=True),
                json.dumps(list(request.binding_snapshot), sort_keys=True), request.state_revision, now])
        return self.experiment_run(request.run_id)

    def experiment_run(self, run_id: str) -> ExperimentRun:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json, "
                "params_schema_json, metadata_json, input_vector_json, binding_snapshot_json, state_revision, "
                "started_at, finished_at, error_json, keep_reason, collected_at FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone()
        if row is None:
            raise KeyError(run_id)
        return ExperimentRun(row[0], row[1], row[2], TimeRange(row[3].replace(tzinfo=timezone.utc), row[4].replace(tzinfo=timezone.utc)),
            row[5], json.loads(row[6]), json.loads(row[7]), json.loads(row[8]), json.loads(row[9]), json.loads(row[10]), row[11],
            row[12].replace(tzinfo=timezone.utc), row[13].replace(tzinfo=timezone.utc) if row[13] else None,
            json.loads(row[14]) if row[14] else None, row[15], row[16].replace(tzinfo=timezone.utc) if row[16] else None)

    def finish_experiment(self, run_id: str, *, status: str, error: dict | None = None) -> ExperimentRun:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("experiment completion status must be succeeded, failed, or cancelled")
        with self._store._lock, self._store._write_conn() as conn:
            changed = conn.execute("UPDATE experiment_runs SET status = ?, error_json = ?, finished_at = ? "
                "WHERE run_id = ? AND status = 'running'", [status, json.dumps(error, sort_keys=True) if error else None,
                datetime.now(timezone.utc).replace(tzinfo=None), run_id]).rowcount
            if changed == 0 and not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone():
                raise KeyError(run_id)
        return self.experiment_run(run_id)

    def record_experiment_metric(self, run_id: str, name: str, value: object) -> None:
        if not name: raise ValueError("experiment metric name is required")
        try: encoded = json.dumps(value, sort_keys=True)
        except (TypeError, ValueError) as error: raise ValueError("experiment metric must be JSON-serializable") from error
        with self._store._lock, self._store._write_conn() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone(): raise KeyError(run_id)
            conn.execute("INSERT INTO experiment_run_metrics VALUES (?, ?, ?, ?) ON CONFLICT (run_id, name) DO UPDATE SET value_json = excluded.value_json, recorded_at = excluded.recorded_at", [run_id, name, encoded, datetime.now(timezone.utc).replace(tzinfo=None)])

    def attach_experiment_artifact(self, run_id: str, artifact: ExperimentArtifact) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone(): raise KeyError(run_id)
            if not conn.execute("SELECT 1 FROM materialization_artifacts WHERE digest = ?", [artifact.digest]).fetchone(): raise KeyError(artifact.digest)
            conn.execute("INSERT INTO experiment_run_artifacts VALUES (?, ?, ?, ?) ON CONFLICT (run_id, name) DO UPDATE SET artifact_digest = excluded.artifact_digest, metadata_json = excluded.metadata_json", [run_id, artifact.name, artifact.digest, json.dumps(dict(artifact.metadata), sort_keys=True)])

    def declare_experiment_output(self, run_id: str, name: str) -> str:
        ref_uri = run_output_ref(run_id, name)
        with self._store._lock, self._store._write_conn() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone(): raise KeyError(run_id)
            conn.execute("INSERT INTO experiment_run_outputs VALUES (?, ?, ?) ON CONFLICT (run_id, name) DO NOTHING", [run_id, name, ref_uri])
        return ref_uri

    def keep_experiment(self, run_id: str, reason: str) -> ExperimentRun:
        if not reason: raise ValueError("a retention reason is required")
        with self._store._lock, self._store._write_conn() as conn:
            if conn.execute("UPDATE experiment_runs SET keep_reason = ? WHERE run_id = ?", [reason, run_id]).rowcount == 0: raise KeyError(run_id)
        return self.experiment_run(run_id)

    def collect_experiment(self, run_id: str) -> ExperimentRun:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT keep_reason FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone()
            if row is None: raise KeyError(run_id)
            if row[0] is not None: raise ValueError("a kept experiment cannot be collected")
            # Keep the small run/metric tombstone, but release its expensive
            # attachment and output registrations. Canonical output values are
            # owned by the normal publication retention policy.
            conn.execute("DELETE FROM experiment_run_artifacts WHERE run_id = ?", [run_id])
            conn.execute("DELETE FROM experiment_run_outputs WHERE run_id = ?", [run_id])
            conn.execute("UPDATE experiment_runs SET status = 'collected', collected_at = ? WHERE run_id = ?", [datetime.now(timezone.utc).replace(tzinfo=None), run_id])
        return self.experiment_run(run_id)

    def list_experiments(self, *, status: str | None = None, metadata: dict[str, object] | None = None) -> tuple[ExperimentRun, ...]:
        with self._store._own_conn() as conn:
            rows = conn.execute("SELECT run_id FROM experiment_runs WHERE (? IS NULL OR status = ?) ORDER BY started_at DESC", [status, status]).fetchall()
        runs = tuple(self.experiment_run(row[0]) for row in rows)
        return tuple(run for run in runs if metadata is None or all(run.metadata.get(key) == value for key, value in metadata.items()))

    def rerun_experiment(self, run_id: str, new_run_id: str) -> ExperimentRun:
        previous = self.experiment_run(run_id)
        if previous.status == "collected": raise ValueError("a collected experiment cannot be rerun")
        return self.start_experiment(ExperimentRunRequest(new_run_id, previous.definition_id, previous.graph_revision,
            previous.interval, previous.params, previous.params_schema, previous.metadata, previous.input_versions,
            previous.binding_snapshot, previous.state_revision))

    def experiment_metrics(self, run_id: str) -> dict[str, object]:
        with self._store._own_conn() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone(): raise KeyError(run_id)
            rows = conn.execute("SELECT name, value_json FROM experiment_run_metrics WHERE run_id = ? ORDER BY name", [run_id]).fetchall()
        return {name: json.loads(value) for name, value in rows}

    def experiment_artifacts(self, run_id: str) -> tuple[ExperimentArtifact, ...]:
        with self._store._own_conn() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone(): raise KeyError(run_id)
            rows = conn.execute("SELECT name, artifact_digest, metadata_json FROM experiment_run_artifacts WHERE run_id = ? ORDER BY name", [run_id]).fetchall()
        return tuple(ExperimentArtifact(name, digest, json.loads(metadata)) for name, digest, metadata in rows)
