"""PostgreSQL persistence for Phase 2 materialization metadata."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Sequence
import pyarrow as pa

from psycopg_pool import ConnectionPool

from acquirium.Materialization.bindings import BindingSpec, validate_binding_topology
from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.ids import materialization_id, partition_ranges
from acquirium.Storage.materialization.types import PlanPartition, StreamChangeRange, WorkLease
from acquirium.Storage.materialization.types import InputSnapshot
from acquirium.Storage.continuous.types import MUTATION_SCHEMA
from acquirium.Storage.artifacts import ArtifactRecord
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentArtifact, ExperimentRun, ExperimentRunRequest, run_output_ref
from acquirium.Materialization.effects import EffectIntent
from acquirium.Materialization.services import ChangeHint


class MaterializationPostgres:
    """Durable definition/binding registry sharing the canonical PostgreSQL DB."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 4) -> None:
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_definitions (
                definition_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                source_digest TEXT NOT NULL, entrypoint TEXT NOT NULL, spec_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_deployments (
                name TEXT PRIMARY KEY, definition_id TEXT NOT NULL, generation BIGINT NOT NULL,
                status TEXT NOT NULL, current_graph_revision BIGINT, updated_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("ALTER TABLE materialization_deployments ADD COLUMN IF NOT EXISTS staged_generation BIGINT")
            conn.execute("ALTER TABLE materialization_deployments ADD COLUMN IF NOT EXISTS staged_definition_id TEXT")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_bindings (
                binding_id TEXT NOT NULL, deployment_name TEXT NOT NULL, generation BIGINT NOT NULL,
                logical_key TEXT NOT NULL, content_digest TEXT NOT NULL, graph_revision BIGINT NOT NULL,
                resolved_metadata_json JSONB NOT NULL, status TEXT NOT NULL,
                PRIMARY KEY (binding_id, generation))""")
            conn.execute("ALTER TABLE materialization_bindings ADD COLUMN IF NOT EXISTS definition_id TEXT")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_binding_refs (
                binding_id TEXT NOT NULL, generation BIGINT NOT NULL, ref_uri TEXT NOT NULL,
                role TEXT NOT NULL, direction TEXT NOT NULL,
                PRIMARY KEY (binding_id, generation, ref_uri, role, direction))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_graph_revisions (
                graph_revision BIGINT PRIMARY KEY, source_version BIGINT NOT NULL,
                content_digest TEXT NOT NULL, published_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_rebind_requests (
                deployment_name TEXT NOT NULL, graph_revision BIGINT NOT NULL, status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, error_json JSONB,
                PRIMARY KEY (deployment_name, graph_revision))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_plans (
                plan_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, generation BIGINT NOT NULL,
                graph_revision BIGINT NOT NULL, input_vector_json JSONB NOT NULL, reason_json JSONB NOT NULL,
                status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ)""")
            conn.execute("ALTER TABLE materialization_plans ADD COLUMN IF NOT EXISTS state_revision TEXT")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_plan_partitions (
                partition_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_expires_at TIMESTAMPTZ, committed_output_id TEXT, error_json JSONB)""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_materialization_partitions_pending ON materialization_plan_partitions (status, start_ts)")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_execution_receipts (
                execution_id TEXT PRIMARY KEY, partition_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                input_vector_json JSONB NOT NULL, output_publication_id TEXT, status TEXT NOT NULL,
                rows_read BIGINT NOT NULL, rows_written BIGINT NOT NULL, error_json JSONB, finished_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("ALTER TABLE materialization_execution_receipts ADD COLUMN IF NOT EXISTS state_revision TEXT")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_binding_progress (
                binding_id TEXT NOT NULL, generation BIGINT NOT NULL, ref_uri TEXT NOT NULL,
                stream_version BIGINT NOT NULL, PRIMARY KEY (binding_id, generation, ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_staged_outputs (
                binding_id TEXT NOT NULL, generation BIGINT NOT NULL, partition_id TEXT NOT NULL,
                ref_uri TEXT NOT NULL, ts TIMESTAMPTZ NOT NULL, numeric_value DOUBLE PRECISION, text_value TEXT,
                PRIMARY KEY (binding_id, generation, ref_uri, ts))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_staged_partitions (
                binding_id TEXT NOT NULL, generation BIGINT NOT NULL, partition_id TEXT NOT NULL,
                PRIMARY KEY (binding_id, generation, partition_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_attempt_snapshots (
                partition_id TEXT NOT NULL, attempt INTEGER NOT NULL, input_vector_json JSONB NOT NULL,
                rows_read BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (partition_id, attempt))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_artifacts (
                digest TEXT PRIMARY KEY, uri TEXT NOT NULL, size_bytes BIGINT NOT NULL,
                media_type TEXT NOT NULL, metadata_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_artifact_requests (
                request_id TEXT PRIMARY KEY, semantic_digest TEXT UNIQUE NOT NULL, kind TEXT NOT NULL,
                deployment_name TEXT NOT NULL, binding_id TEXT NOT NULL, previous_revision TEXT,
                input_vector_json JSONB NOT NULL, range_start TIMESTAMPTZ NOT NULL, range_end TIMESTAMPTZ NOT NULL,
                metadata_json JSONB NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_expires_at TIMESTAMPTZ, result_revision TEXT, error_json JSONB,
                created_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_state_revisions (
                revision_id TEXT PRIMARY KEY, deployment_name TEXT NOT NULL, binding_id TEXT NOT NULL,
                parent_revision TEXT, artifact_digest TEXT NOT NULL, request_id TEXT NOT NULL, policy TEXT,
                effective_from TIMESTAMPTZ, status TEXT NOT NULL, metrics_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL, activated_at TIMESTAMPTZ)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_state_invalidations (
                revision_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, policy TEXT NOT NULL,
                effective_from TIMESTAMPTZ, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_runs (
                run_id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, graph_revision BIGINT NOT NULL,
                start_ts TIMESTAMPTZ NOT NULL, end_ts TIMESTAMPTZ NOT NULL, status TEXT NOT NULL,
                params_json JSONB NOT NULL, params_schema_json JSONB NOT NULL, metadata_json JSONB NOT NULL,
                input_vector_json JSONB NOT NULL, binding_snapshot_json JSONB NOT NULL, state_revision TEXT,
                started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ, error_json JSONB,
                keep_reason TEXT, collected_at TIMESTAMPTZ)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_metrics (
                run_id TEXT NOT NULL, name TEXT NOT NULL, value_json JSONB NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (run_id, name))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_artifacts (
                run_id TEXT NOT NULL, name TEXT NOT NULL, artifact_digest TEXT NOT NULL,
                metadata_json JSONB NOT NULL, PRIMARY KEY (run_id, name))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS experiment_run_outputs (
                run_id TEXT NOT NULL, name TEXT NOT NULL, ref_uri TEXT NOT NULL,
                PRIMARY KEY (run_id, name), UNIQUE (ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_effect_intents (
                effect_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, kind TEXT NOT NULL,
                destination TEXT NOT NULL, payload_json JSONB NOT NULL, idempotency_key TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL, attempts INTEGER NOT NULL, next_attempt_at TIMESTAMPTZ, error_json JSONB)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_services (
                name TEXT PRIMARY KEY, definition_id TEXT NOT NULL, status TEXT NOT NULL,
                health TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_service_hints (
                service_name TEXT PRIMARY KEY, token TEXT NOT NULL, data_versions_json JSONB NOT NULL,
                graph_revision BIGINT, created_at TIMESTAMPTZ NOT NULL)""")

    def close(self) -> None:
        self._pool.close()

    def record_change_ranges(self, ranges: Sequence[StreamChangeRange]) -> None:
        """Persist ranges for callers that use the materialization store directly.

        Canonical ``ContinuousPostgres.publish`` writes these in its own
        transaction; this method makes the backend-neutral range-manifest
        contract complete for import/recovery tooling as well.
        """
        if not ranges:
            return
        with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.executemany("""INSERT INTO stream_change_ranges
                (ref_uri, stream_version, publication_id, start_ts, end_ts, change_kind, row_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ref_uri, stream_version, start_ts, end_ts) DO NOTHING""",
                [(item.ref_uri, item.stream_version, item.publication_id, item.interval.start,
                  item.interval.end, item.change_kind, item.row_count) for item in ranges])

    def change_ranges(self, ref_uri: str, *, after_version: int,
                      through_version: int) -> tuple[StreamChangeRange, ...]:
        """Read the canonical durable invalidation ranges used by the scheduler."""
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT ref_uri, stream_version, publication_id, start_ts, end_ts,
                change_kind, row_count FROM stream_change_ranges
                WHERE ref_uri = %s AND stream_version > %s AND stream_version <= %s
                ORDER BY stream_version, start_ts""", [ref_uri, after_version, through_version]).fetchall()
        return tuple(StreamChangeRange(ref, version, publication,
                                       TimeRange(start, end), kind, row_count)
                     for ref, version, publication, start, end, kind, row_count in rows)

    def register_definition(self, definition: MaterializationDefinition) -> str:
        spec = definition_spec(definition)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO materialization_definitions
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (definition_id) DO NOTHING""",
                [definition.definition_id, definition.name, definition.kind, definition.source_digest,
                 definition.entrypoint, json.dumps(spec), datetime.now(timezone.utc)])
        return definition.definition_id

    def experiment_definition(self, definition_id: str) -> dict[str, object]:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT source_digest, entrypoint, kind FROM materialization_definitions WHERE definition_id = %s", [definition_id]).fetchone()
        if row is None: raise KeyError(definition_id)
        if row[2] != "experiment": raise ValueError("definition is not an experiment")
        return {"source_digest": row[0], "entrypoint": row[1]}

    def deploy(self, name: str, definition_id: str, *, graph_revision: int | None = None) -> int:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT generation, definition_id, staged_generation FROM materialization_deployments WHERE name = %s FOR UPDATE", [name]).fetchone()
            if row is not None and row[1] != definition_id and conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = %s AND status = 'active'", [name]).fetchone():
                generation = max(row[0], row[2] or row[0]) + 1
                conn.execute("UPDATE materialization_deployments SET staged_generation = %s, staged_definition_id = %s, updated_at = %s WHERE name = %s", [generation, definition_id, datetime.now(timezone.utc), name])
                return generation
            generation = 1 if row is None else row[0] if row[1] == definition_id else row[0] + 1
            conn.execute("""INSERT INTO materialization_deployments
                (name, definition_id, generation, status, current_graph_revision, updated_at)
                VALUES (%s, %s, %s, 'registered', %s, %s)
                ON CONFLICT (name) DO UPDATE SET definition_id = excluded.definition_id,
                generation = excluded.generation, status = excluded.status,
                current_graph_revision = excluded.current_graph_revision, updated_at = excluded.updated_at""",
                [name, definition_id, generation, graph_revision, datetime.now(timezone.utc)])
        return generation

    def persist_bindings(self, deployment_name: str, generation: int, graph_revision: int, definition_id: str, bindings: Sequence[BindingSpec]) -> None:
        validate_binding_topology(bindings, definition_id=definition_id)
        with self._pool.connection() as conn, conn.transaction():
            for binding in bindings:
                binding_id = binding.binding_id(definition_id)
                conn.execute("""INSERT INTO materialization_bindings
                    (binding_id, deployment_name, generation, logical_key, content_digest, graph_revision, resolved_metadata_json, definition_id, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'staging')
                    ON CONFLICT (binding_id, generation) DO UPDATE SET content_digest = excluded.content_digest,
                    graph_revision = excluded.graph_revision, resolved_metadata_json = excluded.resolved_metadata_json, definition_id = excluded.definition_id""",
                    [binding_id, deployment_name, generation, binding.logical_key, binding.content_digest,
                     graph_revision, json.dumps(binding.metadata), definition_id])
                for direction, roles in (("input", binding.inputs), ("output", binding.outputs)):
                    for role, refs in roles.items():
                        with conn.cursor() as cursor:
                            cursor.executemany(
                                "INSERT INTO materialization_binding_refs VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                [(binding_id, generation, ref, role, direction) for ref in refs],
                            )

    def stage_bindings(self, deployment_name: str, graph_revision: int, definition_id: str,
                       bindings: Sequence[BindingSpec]) -> int:
        """Create the next invisible topology generation without moving the active pointer."""
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT generation, staged_generation FROM materialization_deployments WHERE name = %s FOR UPDATE", [deployment_name]).fetchone()
            if row is None:
                raise KeyError(deployment_name)
            active, staged = row
            generation = max(active, staged or active) + 1
            if not conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = %s AND generation = %s AND status = 'active'", [deployment_name, active]).fetchone():
                generation = active
            conn.execute("UPDATE materialization_deployments SET staged_generation = %s, updated_at = %s WHERE name = %s", [generation, datetime.now(timezone.utc), deployment_name])
        self.persist_bindings(deployment_name, generation, graph_revision, definition_id, bindings)
        return generation

    def activate_bindings(self, deployment_name: str, generation: int) -> None:
        """Atomically publish a fully staged binding generation."""
        with self._pool.connection() as conn, conn.transaction():
            staged = conn.execute("SELECT count(*) FROM materialization_bindings WHERE deployment_name = %s AND generation = %s AND status = 'staging'", [deployment_name, generation]).fetchone()[0]
            staged_pointer = conn.execute("SELECT staged_generation FROM materialization_deployments WHERE name = %s", [deployment_name]).fetchone()
            if not staged and (staged_pointer is None or staged_pointer[0] != generation):
                raise ValueError("no staged bindings to activate")
            conflicts = conn.execute("""SELECT refs.ref_uri FROM materialization_binding_refs refs
                JOIN materialization_bindings binding ON binding.binding_id = refs.binding_id AND binding.generation = refs.generation
                WHERE refs.direction = 'output' AND binding.status = 'active' AND binding.deployment_name != %s
                AND refs.ref_uri IN (SELECT new_refs.ref_uri FROM materialization_binding_refs new_refs
                    JOIN materialization_bindings new_binding ON new_binding.binding_id = new_refs.binding_id AND new_binding.generation = new_refs.generation
                    WHERE new_refs.direction = 'output' AND new_binding.deployment_name = %s AND new_refs.generation = %s) LIMIT 1""", [deployment_name, deployment_name, generation]).fetchone()
            if conflicts:
                raise ValueError(f"output {conflicts[0]!r} is owned by another active deployment")
            active_generation = conn.execute("SELECT generation FROM materialization_deployments WHERE name = %s", [deployment_name]).fetchone()[0]
            has_active = conn.execute("SELECT 1 FROM materialization_bindings WHERE deployment_name = %s AND status = 'active'", [deployment_name]).fetchone() is not None
            incomplete = conn.execute("""SELECT count(*) FROM materialization_plans
                WHERE binding_id IN (SELECT binding_id FROM materialization_bindings
                    WHERE deployment_name = %s AND generation = %s) AND generation = %s AND status != 'committed'""",
                [deployment_name, generation, generation]).fetchone()[0]
            if incomplete and (has_active or active_generation != generation):
                raise ValueError("staged binding plans have not completed")
            from acquirium.Storage.continuous.postgres import ContinuousPostgres
            mutations: list[dict] = []
            retiring_refs = [row[0] for row in conn.execute("""SELECT DISTINCT refs.ref_uri
                FROM materialization_binding_refs refs JOIN materialization_bindings binding
                ON binding.binding_id = refs.binding_id AND binding.generation = refs.generation
                WHERE binding.deployment_name = %s AND binding.status = 'active' AND binding.generation != %s AND refs.direction = 'output'""", [deployment_name, generation]).fetchall()]
            if retiring_refs:
                retiring_rows = conn.execute("SELECT ref_uri, ts FROM timeseries WHERE ref_uri = ANY(%s::text[]) AND NOT deleted", [retiring_refs]).fetchall()
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": ts, "numeric_value": None, "text_value": None} for ref, ts in retiring_rows)
            partitions = conn.execute("""SELECT staged.binding_id, staged.partition_id, part.start_ts, part.end_ts
                FROM materialization_staged_partitions staged
                JOIN materialization_plan_partitions part ON part.partition_id = staged.partition_id
                JOIN materialization_bindings binding ON binding.binding_id = staged.binding_id AND binding.generation = staged.generation
                WHERE staged.generation = %s AND binding.deployment_name = %s""", [generation, deployment_name]).fetchall()
            for binding_id, partition_id, start, end in partitions:
                refs = [row[0] for row in conn.execute("SELECT ref_uri FROM materialization_binding_refs WHERE binding_id = %s AND generation = %s AND direction = 'output'", [binding_id, generation]).fetchall()]
                if not refs:
                    continue
                existing = conn.execute("SELECT ref_uri, ts FROM timeseries WHERE ref_uri = ANY(%s::text[]) AND ts >= %s AND ts < %s AND NOT deleted", [refs, start, end]).fetchall()
                staged_rows = conn.execute("SELECT ref_uri, ts, numeric_value, text_value FROM materialization_staged_outputs WHERE binding_id = %s AND generation = %s AND partition_id = %s", [binding_id, generation, partition_id]).fetchall()
                keys = {(ref, ts) for ref, ts, _, _ in staged_rows}
                mutations.extend({"operation": "delete", "ref_uri": ref, "ts": ts, "numeric_value": None, "text_value": None} for ref, ts in existing if (ref, ts) not in keys)
                mutations.extend({"operation": "upsert", "ref_uri": ref, "ts": ts, "numeric_value": numeric, "text_value": text} for ref, ts, numeric, text in staged_rows)
            if mutations:
                publication_id = f"materialization:activate:{deployment_name}:{generation}"
                ContinuousPostgres.__new__(ContinuousPostgres)._apply_publication(conn.cursor(), publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
            conn.execute("DELETE FROM materialization_staged_outputs WHERE generation = %s AND binding_id IN (SELECT binding_id FROM materialization_bindings WHERE deployment_name = %s AND generation = %s)", [generation, deployment_name, generation])
            conn.execute("DELETE FROM materialization_staged_partitions WHERE generation = %s AND binding_id IN (SELECT binding_id FROM materialization_bindings WHERE deployment_name = %s AND generation = %s)", [generation, deployment_name, generation])
            conn.execute("UPDATE materialization_bindings SET status = 'retiring' WHERE deployment_name = %s AND status = 'active' AND generation != %s", [deployment_name, generation])
            conn.execute("UPDATE materialization_bindings SET status = 'active' WHERE deployment_name = %s AND generation = %s AND status = 'staging'", [deployment_name, generation])
            conn.execute("UPDATE materialization_deployments SET definition_id = coalesce(staged_definition_id, definition_id), staged_definition_id = NULL, generation = %s, staged_generation = NULL, current_graph_revision = (SELECT max(graph_revision) FROM materialization_bindings WHERE deployment_name = %s AND generation = %s), updated_at = %s WHERE name = %s", [generation, deployment_name, generation, datetime.now(timezone.utc), deployment_name])

    def activate_ready_bindings(self) -> tuple[str, ...]:
        with self._pool.connection() as conn:
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
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("INSERT INTO materialization_graph_revisions VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                         [graph_revision, source_version, content_digest, datetime.now(timezone.utc)])

    def request_rebind(self, deployment_name: str, graph_revision: int, *, force: bool = False) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO materialization_rebind_requests (deployment_name, graph_revision, status)
                VALUES (%s, %s, 'pending') ON CONFLICT (deployment_name, graph_revision) DO UPDATE SET
                    status = 'pending', error_json = NULL
                WHERE materialization_rebind_requests.status = 'failed' OR %s""", [deployment_name, graph_revision, force])

    def lease_rebind(self, owner: str) -> tuple[str, int] | None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_rebind_requests AS request SET status = 'superseded'
                WHERE status = 'pending' AND graph_revision < (SELECT max(graph_revision)
                FROM materialization_rebind_requests newer WHERE newer.deployment_name = request.deployment_name)""")
            row = conn.execute("""SELECT deployment_name, graph_revision FROM materialization_rebind_requests
                WHERE status = 'pending' ORDER BY graph_revision, deployment_name FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if row is None:
                return None
            conn.execute("""UPDATE materialization_rebind_requests SET status = 'leased', attempts = attempts + 1
                WHERE deployment_name = %s AND graph_revision = %s""", row)
            return row

    def finish_rebind(self, deployment_name: str, graph_revision: int, *, error: dict | None = None) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_rebind_requests SET status = %s, error_json = %s
                WHERE deployment_name = %s AND graph_revision = %s AND status = 'leased'""",
                ["failed" if error is not None else "completed", json.dumps(error) if error else None,
                 deployment_name, graph_revision])

    def deployment_names(self) -> tuple[str, ...]:
        with self._pool.connection() as conn:
            rows = conn.execute("SELECT name FROM materialization_deployments WHERE status != 'failed' ORDER BY name").fetchall()
        return tuple(row[0] for row in rows)

    def deployment_definition(self, name: str) -> dict[str, object]:
        """Return the current immutable bundle and generation for rebinding."""
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT d.definition_id, coalesce(deployment.staged_generation, deployment.generation), d.spec_json
                FROM materialization_deployments deployment
                JOIN materialization_definitions d ON d.definition_id = coalesce(deployment.staged_definition_id, deployment.definition_id)
                WHERE deployment.name = %s""", [name]).fetchone()
        if row is None:
            raise KeyError(name)
        definition_id, generation, spec = row
        if isinstance(spec, str):
            spec = json.loads(spec)
        return {"definition_id": definition_id, "generation": generation, "spec": spec}

    def stale_bindings(self) -> tuple[dict[str, object], ...]:
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT b.binding_id, b.deployment_name, b.generation, b.graph_revision, r.ref_uri,
                h.current_version, coalesce(p.stream_version, 0) AS progress, d.spec_json
                FROM materialization_bindings b
                JOIN materialization_deployments deployment ON deployment.name = b.deployment_name
                JOIN materialization_definitions d ON d.definition_id = coalesce(deployment.staged_definition_id, deployment.definition_id)
                JOIN materialization_binding_refs r ON r.binding_id = b.binding_id AND r.generation = b.generation AND r.direction = 'input'
                JOIN stream_heads h ON h.ref_uri = r.ref_uri
                LEFT JOIN materialization_binding_progress p ON p.binding_id = b.binding_id AND p.generation = b.generation AND p.ref_uri = r.ref_uri
                WHERE b.status IN ('active', 'staging') AND h.current_version > coalesce(p.stream_version, 0)
                ORDER BY b.binding_id, r.ref_uri""").fetchall()
        grouped: dict[tuple[str, int, int], dict[str, object]] = {}
        for binding, deployment_name, generation, graph, ref, head, progress, spec in rows:
            item = grouped.setdefault((binding, generation, graph), {"binding_id": binding, "deployment_name": deployment_name, "generation": generation, "graph_revision": graph, "impact": (json.loads(spec) if isinstance(spec, str) else spec)["impact"], "heads": {}, "progress": {}})
            item["heads"][ref] = head
            item["progress"][ref] = progress
        return tuple(grouped.values())

    def bootstrap_bindings(self, deployment_name: str, generation: int) -> tuple[dict[str, object], ...]:
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT binding.binding_id, refs.ref_uri, coalesce(head.current_version, 0),
                min(value.ts), max(value.ts)
                FROM materialization_bindings binding
                JOIN materialization_binding_refs refs ON refs.binding_id = binding.binding_id AND refs.generation = binding.generation AND refs.direction = 'input'
                LEFT JOIN stream_heads head ON head.ref_uri = refs.ref_uri
                LEFT JOIN timeseries value ON value.ref_uri = refs.ref_uri AND NOT value.deleted
                WHERE binding.deployment_name = %s AND binding.generation = %s AND binding.status = 'staging'
                GROUP BY binding.binding_id, refs.ref_uri, head.current_version""", [deployment_name, generation]).fetchall()
        grouped: dict[str, dict[str, object]] = {}
        for binding_id, ref, head, start, end in rows:
            item = grouped.setdefault(binding_id, {"binding_id": binding_id, "generation": generation, "heads": {}, "ranges": []})
            item["heads"][ref] = head
            if start is not None:
                item["ranges"].append((start, end))
        return tuple(grouped.values())

    def set_deployment_status(self, name: str, status: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            changed = conn.execute("UPDATE materialization_deployments SET status = %s, updated_at = %s WHERE name = %s",
                                   [status, datetime.now(timezone.utc), name]).rowcount
            if changed == 0:
                raise KeyError(name)

    def deployment_status(self, name: str) -> dict[str, object] | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT name, definition_id, generation, status, current_graph_revision FROM materialization_deployments WHERE name = %s", [name]).fetchone()
        if row is None:
            return None
        return dict(zip(("name", "definition_id", "generation", "status", "graph_revision"), row))

    def deployments(self) -> tuple[dict[str, object], ...]:
        with self._pool.connection() as conn:
            rows = conn.execute("SELECT name, definition_id, generation, status, current_graph_revision FROM materialization_deployments ORDER BY name").fetchall()
        keys = ("name", "definition_id", "generation", "status", "graph_revision")
        return tuple(dict(zip(keys, row)) for row in rows)

    def create_plan(self, *, binding_id: str, generation: int, graph_revision: int,
                    input_vector: dict[str, int], ranges: Sequence[TimeRange], reason: dict,
                    maximum_partition_duration: timedelta) -> tuple[str, tuple[PlanPartition, ...]]:
        normalized = partition_ranges(ranges, maximum_duration=maximum_partition_duration)
        plan_id = materialization_id(binding_id, generation, graph_revision, json.dumps(input_vector, sort_keys=True),
                                     [(item.start.isoformat(), item.end.isoformat()) for item in normalized], json.dumps(reason, sort_keys=True))
        partitions = tuple(PlanPartition(materialization_id(plan_id, item.start.isoformat(), item.end.isoformat()), plan_id, item) for item in normalized)
        with self._pool.connection() as conn, conn.transaction():
            state = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = %s AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
            conn.execute("""INSERT INTO materialization_plans
                (plan_id, binding_id, generation, graph_revision, input_vector_json, reason_json, status, created_at, completed_at, state_revision)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, NULL, %s) ON CONFLICT (plan_id) DO NOTHING""",
                [plan_id, binding_id, generation, graph_revision, json.dumps(input_vector), json.dumps(reason), datetime.now(timezone.utc), state[0] if state else None])
            with conn.cursor() as cursor:
                cursor.executemany("""INSERT INTO materialization_plan_partitions
                    (partition_id, plan_id, start_ts, end_ts, status) VALUES (%s, %s, %s, %s, 'pending') ON CONFLICT DO NOTHING""",
                    [(item.partition_id, plan_id, item.interval.start, item.interval.end) for item in partitions])
        return plan_id, partitions

    def lease_partition(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> WorkLease | None:
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
                WHERE status = 'leased' AND lease_expires_at <= %s""", [now])
            row = conn.execute("""SELECT partition_id, plan_id, start_ts, end_ts, attempt FROM materialization_plan_partitions
                WHERE status = 'pending' ORDER BY start_ts, partition_id FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if row is None:
                return None
            partition_id, plan_id, start, end, prior_attempt = row
            attempt, expires = prior_attempt + 1, now + duration
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'leased', attempt = %s, lease_owner = %s, lease_expires_at = %s
                WHERE partition_id = %s""", [attempt, owner, expires, partition_id])
        return WorkLease(PlanPartition(partition_id, plan_id, TimeRange(start, end), "leased"), owner, attempt, expires)

    def lease_registered_partition(self, owner: str, *, deployment_name: str | None = None,
                                 duration: timedelta = timedelta(minutes=5)) -> WorkLease | None:
        """Lease pending work only when its deployment has been started."""
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
                WHERE status = 'leased' AND lease_expires_at <= %s""", [now])
            row = conn.execute("""SELECT part.partition_id, part.plan_id, part.start_ts, part.end_ts, part.attempt
                FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                JOIN materialization_deployments deployment ON deployment.name = binding.deployment_name
                WHERE part.status = 'pending' AND deployment.status = 'active' AND (%s::text IS NULL OR deployment.name = %s)
                ORDER BY part.start_ts, part.partition_id FOR UPDATE OF part SKIP LOCKED LIMIT 1""", [deployment_name, deployment_name]).fetchone()
            if row is None:
                return None
            partition_id, plan_id, start, end, prior_attempt = row
            attempt, expires = prior_attempt + 1, now + duration
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'leased', attempt = %s, lease_owner = %s, lease_expires_at = %s
                WHERE partition_id = %s""", [attempt, owner, expires, partition_id])
        return WorkLease(PlanPartition(partition_id, plan_id, TimeRange(start, end), "leased"), owner, attempt, expires)

    def release_partition(self, lease: WorkLease) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL WHERE partition_id = %s AND status = 'leased' AND lease_owner = %s AND attempt = %s""",
                [lease.partition.partition_id, lease.owner, lease.attempt])

    def commit_partition(self, lease: WorkLease, *, output_publication_id: str) -> bool:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = %s FOR UPDATE", [lease.partition.partition_id]).fetchone()
            if row is None:
                raise KeyError(lease.partition.partition_id)
            if row[0] == "committed":
                return False
            if row != ("leased", lease.owner, lease.attempt):
                raise ValueError("partition lease is stale")
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'committed', committed_output_id = %s,
                lease_owner = NULL, lease_expires_at = NULL WHERE partition_id = %s""", [output_publication_id, lease.partition.partition_id])
            pending = conn.execute("SELECT count(*) FROM materialization_plan_partitions WHERE plan_id = %s AND status != 'committed'", [lease.partition.plan_id]).fetchone()[0]
            if pending == 0:
                conn.execute("UPDATE materialization_plans SET status = 'committed', completed_at = %s WHERE plan_id = %s", [datetime.now(timezone.utc), lease.partition.plan_id])
        return True

    def snapshot_partition(self, lease: WorkLease, input_refs: Sequence[str]) -> InputSnapshot:
        with self._pool.connection() as conn, conn.transaction():
            state = conn.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = %s FOR UPDATE", [lease.partition.partition_id]).fetchone()
            if state != ("leased", lease.owner, lease.attempt):
                raise ValueError("partition lease is stale")
            if not input_refs:
                return InputSnapshot(lease, pa.Table.from_pylist([], schema=MUTATION_SCHEMA), {})
            heads = dict(conn.execute("SELECT ref_uri, current_version FROM stream_heads WHERE ref_uri = ANY(%s::text[])", [list(input_refs)]).fetchall())
            rows = conn.execute("""SELECT 'upsert', ref_uri, ts, numeric_value, text_value FROM timeseries
                WHERE ref_uri = ANY(%s::text[]) AND ts >= %s AND ts < %s AND NOT deleted ORDER BY ref_uri, ts""",
                [list(input_refs), lease.partition.interval.start, lease.partition.interval.end]).fetchall()
        table = pa.Table.from_pylist([dict(zip(MUTATION_SCHEMA.names, row)) for row in rows], schema=MUTATION_SCHEMA)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO materialization_attempt_snapshots VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING""", [lease.partition.partition_id, lease.attempt, json.dumps(heads), table.num_rows, datetime.now(timezone.utc)])
        return InputSnapshot(lease, table, heads)

    def partition_refs(self, partition_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT refs.direction, refs.ref_uri FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_binding_refs refs ON refs.binding_id = plan.binding_id AND refs.generation = plan.generation
                WHERE part.partition_id = %s ORDER BY refs.direction, refs.ref_uri""", [partition_id]).fetchall()
        return (tuple(ref for direction, ref in rows if direction == "input"), tuple(ref for direction, ref in rows if direction == "output"))

    def partition_definition(self, partition_id: str) -> dict[str, object]:
        """Resolve the immutable definition bundle owned by a leased partition."""
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT d.source_digest, d.entrypoint, d.spec_json FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                JOIN materialization_definitions d ON d.definition_id = binding.definition_id
                WHERE part.partition_id = %s""", [partition_id]).fetchone()
        if row is None:
            raise KeyError(partition_id)
        digest, entrypoint, spec = row
        if isinstance(spec, str):
            spec = json.loads(spec)
        return {"source_digest": digest, "entrypoint": entrypoint, "spec": spec}

    def partition_binding_metadata(self, partition_id: str) -> dict[str, object]:
        """Return the immutable resolved metadata captured with this binding generation."""
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT binding.resolved_metadata_json FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id
                    AND binding.generation = plan.generation
                WHERE part.partition_id = %s""", [partition_id]).fetchone()
        if row is None:
            raise KeyError(partition_id)
        metadata = row[0]
        return json.loads(metadata) if isinstance(metadata, str) else metadata

    def leased_partition(self, partition_id: str, owner: str, attempt: int) -> WorkLease:
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT plan_id, start_ts, end_ts, lease_expires_at FROM materialization_plan_partitions
                WHERE partition_id = %s AND status = 'leased' AND lease_owner = %s AND attempt = %s""", [partition_id, owner, attempt]).fetchone()
        if row is None:
            raise ValueError("partition lease is stale")
        plan_id, start, end, expires = row
        return WorkLease(PlanPartition(partition_id, plan_id, TimeRange(start, end), "leased"), owner, attempt, expires)

    def fail_partition(self, lease: WorkLease, error: dict) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL, error_json = %s WHERE partition_id = %s AND status = 'leased' AND lease_owner = %s AND attempt = %s""",
                [json.dumps(error), lease.partition.partition_id, lease.owner, lease.attempt])

    def commit_replacement(self, snapshot: InputSnapshot, *, input_refs: Sequence[str], output_refs: Sequence[str], replacement: pa.Table) -> str | None:
        from acquirium.Storage.continuous.postgres import ContinuousPostgres
        from acquirium.Storage.materialization.duckdb import StaleAttemptError
        required = {"ref_uri", "ts", "numeric_value", "text_value"}
        if not required.issubset(replacement.column_names):
            raise ValueError("replacement must contain ref_uri, ts, numeric_value, and text_value")
        rows = replacement.select(["ref_uri", "ts", "numeric_value", "text_value"]).to_pylist()
        interval = snapshot.lease.partition.interval
        if any(row["ref_uri"] not in output_refs or not (interval.start <= row["ts"] < interval.end) for row in rows):
            raise ValueError("replacement lies outside owned outputs or partition")
        execution_id = materialization_id(snapshot.lease.partition.partition_id, snapshot.lease.attempt)
        with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            persisted = cur.execute("SELECT input_vector_json FROM materialization_attempt_snapshots WHERE partition_id = %s AND attempt = %s", [snapshot.lease.partition.partition_id, snapshot.lease.attempt]).fetchone()
            if persisted is None:
                raise ValueError("snapshot attempt was not recorded")
            vector = json.loads(persisted[0]) if isinstance(persisted[0], str) else persisted[0]
            snapshot = InputSnapshot(snapshot.lease, snapshot.inputs, vector)
            state = cur.execute("SELECT status, lease_owner, attempt FROM materialization_plan_partitions WHERE partition_id = %s FOR UPDATE", [snapshot.lease.partition.partition_id]).fetchone()
            if state and state[0] == "committed":
                row = cur.execute("SELECT output_publication_id FROM materialization_execution_receipts WHERE execution_id = %s", [execution_id]).fetchone()
                return row[0] if row else None
            if state != ("leased", snapshot.lease.owner, snapshot.lease.attempt):
                raise ValueError("partition lease is stale")
            binding = cur.execute("""SELECT plan.binding_id, plan.generation, binding.status
                FROM materialization_plans plan JOIN materialization_bindings binding
                ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                WHERE plan.plan_id = %s""", [snapshot.lease.partition.plan_id]).fetchone()
            binding_id, generation, binding_status = binding or (None, None, "active")
            reason = cur.execute("SELECT reason_json FROM materialization_plans WHERE plan_id = %s", [snapshot.lease.partition.plan_id]).fetchone()[0]
            from acquirium.Materialization.impact import ImpactPolicy
            # Keep policy expansion in Python so DuckDB/PostgreSQL have identical semantics.
            impact = ImpactPolicy.from_json((json.loads(reason) if isinstance(reason, str) else reason).get("impact", {"kind": "pointwise"}))
            for ref, version in snapshot.input_versions.items():
                for start, end in cur.execute("SELECT start_ts, end_ts FROM stream_change_ranges WHERE ref_uri = %s AND stream_version > %s", [ref, version]).fetchall():
                    if impact.affected(TimeRange(start, end)).intersects(interval):
                        raise StaleAttemptError("a newer intersecting input change exists")
            if binding_status == "staging":
                cur.execute("INSERT INTO materialization_staged_partitions VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", [binding_id, generation, snapshot.lease.partition.partition_id])
                cur.execute("""DELETE FROM materialization_staged_outputs WHERE binding_id = %s AND generation = %s
                    AND ref_uri IN (SELECT ref_uri FROM materialization_binding_refs WHERE binding_id = %s AND generation = %s AND direction = 'output')
                    AND ts >= %s AND ts < %s""", [binding_id, generation, binding_id, generation, interval.start, interval.end])
                if rows:
                    cur.executemany("""INSERT INTO materialization_staged_outputs VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (binding_id, generation, ref_uri, ts) DO UPDATE SET partition_id = excluded.partition_id,
                        numeric_value = excluded.numeric_value, text_value = excluded.text_value""",
                        [(binding_id, generation, snapshot.lease.partition.partition_id, row["ref_uri"], row["ts"], row["numeric_value"], row["text_value"]) for row in rows])
                publication_id = f"staged:{execution_id}"
            else:
                existing_rows = cur.execute("""SELECT ref_uri, ts FROM timeseries WHERE ref_uri = ANY(%s::text[])
                    AND ts >= %s AND ts < %s AND NOT deleted""", [list(output_refs), interval.start, interval.end]).fetchall() if output_refs else []
                keys = {(row["ref_uri"], row["ts"]) for row in rows}
                mutations = ([{"operation": "delete", "ref_uri": ref, "ts": ts, "numeric_value": None, "text_value": None} for ref, ts in existing_rows if (ref, ts) not in keys]
                             + [{"operation": "upsert", **row} for row in rows])
                publication_id = None
                if mutations:
                    publication_id = f"materialization:{execution_id}"
                    ContinuousPostgres.__new__(ContinuousPostgres)._apply_publication(cur, publication_id, pa.Table.from_pylist(mutations, schema=MUTATION_SCHEMA))
            cur.execute("UPDATE materialization_plan_partitions SET status = 'committed', committed_output_id = %s, lease_owner = NULL, lease_expires_at = NULL WHERE partition_id = %s", [publication_id, snapshot.lease.partition.partition_id])
            pinned = cur.execute("SELECT state_revision FROM materialization_plans WHERE plan_id = %s", [snapshot.lease.partition.plan_id]).fetchone()[0]
            cur.execute("""INSERT INTO materialization_execution_receipts
                (execution_id, partition_id, attempt, input_vector_json, output_publication_id,
                 status, rows_read, rows_written, error_json, finished_at, state_revision)
                VALUES (%s, %s, %s, %s, %s, 'committed', %s, %s, NULL, %s, %s)
                ON CONFLICT DO NOTHING""", [execution_id, snapshot.lease.partition.partition_id,
                snapshot.lease.attempt, json.dumps(snapshot.input_versions), publication_id,
                snapshot.inputs.num_rows, len(rows), datetime.now(timezone.utc), pinned])
            if cur.execute("SELECT count(*) FROM materialization_plan_partitions WHERE plan_id = %s AND status != 'committed'", [snapshot.lease.partition.plan_id]).fetchone()[0] == 0:
                cur.execute("UPDATE materialization_plans SET status = 'committed', completed_at = %s WHERE plan_id = %s", [datetime.now(timezone.utc), snapshot.lease.partition.plan_id])
                binding, generation, captured = cur.execute("SELECT binding_id, generation, input_vector_json FROM materialization_plans WHERE plan_id = %s", [snapshot.lease.partition.plan_id]).fetchone()
                captured = json.loads(captured) if isinstance(captured, str) else captured
                for ref, version in captured.items():
                    cur.execute("""INSERT INTO materialization_binding_progress VALUES (%s, %s, %s, %s)
                        ON CONFLICT (binding_id, generation, ref_uri) DO UPDATE SET stream_version = greatest(materialization_binding_progress.stream_version, excluded.stream_version)""", [binding, generation, ref, version])
        return publication_id

    def create_artifact_request(self, request: ArtifactRequest) -> str:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT request_id FROM materialization_artifact_requests WHERE semantic_digest = %s", [request.semantic_digest]).fetchone()
            if row:
                return row[0]
            conn.execute("""INSERT INTO materialization_artifact_requests
                (request_id, semantic_digest, kind, deployment_name, binding_id, previous_revision,
                 input_vector_json, range_start, range_end, metadata_json, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)""",
                [request.request_id, request.semantic_digest, request.kind, request.deployment_name,
                 request.binding_id, request.previous_revision, json.dumps(dict(request.input_versions)),
                 request.interval.start, request.interval.end, json.dumps(dict(request.metadata)), datetime.now(timezone.utc)])
        return request.request_id

    def lease_artifact_request(self, owner: str, *, duration: timedelta = timedelta(minutes=15)) -> ArtifactLease | None:
        now = datetime.now(timezone.utc); expires = now + duration
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("UPDATE materialization_artifact_requests SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL WHERE status = 'leased' AND lease_expires_at <= %s", [now])
            row = conn.execute("""SELECT request_id, kind, deployment_name, binding_id, previous_revision,
                input_vector_json, range_start, range_end, metadata_json, attempt FROM materialization_artifact_requests
                WHERE status = 'pending' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if row is None:
                return None
            request_id, kind, deployment, binding, previous, vector, start, end, metadata, attempt = row
            attempt += 1
            conn.execute("UPDATE materialization_artifact_requests SET status = 'leased', attempt = %s, lease_owner = %s, lease_expires_at = %s WHERE request_id = %s", [attempt, owner, expires, request_id])
        return ArtifactLease(ArtifactRequest(request_id, kind, deployment, binding,
            json.loads(vector) if isinstance(vector, str) else vector, TimeRange(start, end), previous,
            json.loads(metadata) if isinstance(metadata, str) else metadata), owner, attempt, expires)

    def leased_artifact_request(self, request_id: str, owner: str, attempt: int) -> ArtifactLease:
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT kind, deployment_name, binding_id, previous_revision,
                input_vector_json, range_start, range_end, metadata_json, lease_expires_at
                FROM materialization_artifact_requests WHERE request_id = %s AND status = 'leased'
                AND lease_owner = %s AND attempt = %s""", [request_id, owner, attempt]).fetchone()
        if row is None:
            raise ValueError("artifact lease is stale")
        kind, deployment, binding, previous, vector, start, end, metadata, expires = row
        return ArtifactLease(ArtifactRequest(request_id, kind, deployment, binding,
            json.loads(vector) if isinstance(vector, str) else vector, TimeRange(start, end), previous,
            json.loads(metadata) if isinstance(metadata, str) else metadata), owner, attempt, expires)

    def complete_artifact_request(self, lease: ArtifactLease, artifact: ArtifactRecord,
                                  candidate: ArtifactCandidate) -> StateRevision:
        if artifact.digest != candidate.digest:
            raise ValueError("artifact digest does not match produced bytes")
        revision_id = materialization_id("artifact", lease.request.binding_id,
                                        lease.request.request_id, artifact.digest)
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("""SELECT status, lease_owner, attempt, result_revision
                FROM materialization_artifact_requests WHERE request_id = %s FOR UPDATE""",
                [lease.request.request_id]).fetchone()
            if row is None:
                raise KeyError(lease.request.request_id)
            if row[0] == "completed":
                return self.state_revision(row[3])
            if row[:3] != ("leased", lease.owner, lease.attempt):
                raise ValueError("artifact lease is stale")
            conn.execute("""INSERT INTO materialization_artifacts
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                [artifact.digest, artifact.uri, artifact.size_bytes, artifact.media_type,
                 json.dumps(dict(artifact.metadata)), now])
            conn.execute("""INSERT INTO materialization_state_revisions
                (revision_id, deployment_name, binding_id, parent_revision, artifact_digest,
                 request_id, status, metrics_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'candidate', %s, %s)
                ON CONFLICT DO NOTHING""",
                [revision_id, lease.request.deployment_name, lease.request.binding_id,
                 lease.request.previous_revision, artifact.digest, lease.request.request_id,
                 json.dumps(dict(candidate.metrics)), now])
            conn.execute("""UPDATE materialization_artifact_requests SET status = 'completed',
                result_revision = %s, lease_owner = NULL, lease_expires_at = NULL, completed_at = %s
                WHERE request_id = %s""", [revision_id, now, lease.request.request_id])
        return self.state_revision(revision_id)

    def fail_artifact_request(self, lease: ArtifactLease, error: dict) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("UPDATE materialization_artifact_requests SET status = 'pending', "
                "lease_owner = NULL, lease_expires_at = NULL, error_json = %s "
                "WHERE request_id = %s AND status = 'leased' AND lease_owner = %s AND attempt = %s",
                [json.dumps(error, sort_keys=True), lease.request.request_id, lease.owner, lease.attempt])

    def state_revision(self, revision_id: str) -> StateRevision:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT r.revision_id, r.deployment_name, r.binding_id, "
                "r.parent_revision, a.digest, a.uri, a.size_bytes, a.media_type, a.metadata_json, "
                "r.status, r.policy, r.effective_from, r.metrics_json "
                "FROM materialization_state_revisions r JOIN materialization_artifacts a "
                "ON a.digest = r.artifact_digest WHERE r.revision_id = %s", [revision_id]).fetchone()
        if row is None:
            raise KeyError(revision_id)
        identifier, deployment, binding, parent, digest, uri, size, media, metadata, status, policy, effective, metrics = row
        return StateRevision(identifier, deployment, binding,
            ArtifactRecord(digest, uri, size, media, json.loads(metadata) if isinstance(metadata, str) else metadata),
            status, parent, policy, effective, json.loads(metrics) if isinstance(metrics, str) else metrics)

    def promote_state_revision(self, revision_id: str, *, policy: str = "prospective",
                               effective_from: datetime | None = None) -> StateRevision:
        if policy not in {"prospective", "recompute_all", "recompute_from"}:
            raise ValueError("unknown promotion policy")
        if policy == "recompute_from" and effective_from is None:
            raise ValueError("recompute_from requires effective_from")
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT binding_id, status FROM materialization_state_revisions WHERE revision_id = %s FOR UPDATE", [revision_id]).fetchone()
            if row is None:
                raise KeyError(revision_id)
            if row[1] not in {"candidate", "active"}:
                raise ValueError("only candidate revisions may be promoted")
            conn.execute("UPDATE materialization_state_revisions SET status = 'retired' "
                         "WHERE binding_id = %s AND status = 'active' AND revision_id != %s",
                         [row[0], revision_id])
            conn.execute("UPDATE materialization_state_revisions SET status = 'active', policy = %s, "
                         "effective_from = %s, activated_at = %s WHERE revision_id = %s",
                [policy, effective_from, datetime.now(timezone.utc), revision_id])
            if policy != "prospective":
                conn.execute("""INSERT INTO materialization_state_invalidations
                    VALUES (%s, %s, %s, %s, 'pending', %s) ON CONFLICT (revision_id) DO NOTHING""",
                    [revision_id, row[0], policy, effective_from, datetime.now(timezone.utc)])
                affected = "" if policy == "recompute_all" else " AND part.end_ts > %s"
                parameters: list[object] = [row[0], revision_id]
                if policy == "recompute_from":
                    parameters.append(effective_from)
                conn.execute("""UPDATE materialization_plan_partitions AS part SET status = 'superseded',
                    lease_owner = NULL, lease_expires_at = NULL
                    FROM materialization_plans AS plan
                    WHERE part.plan_id = plan.plan_id AND plan.binding_id = %s
                    AND coalesce(plan.state_revision, '') <> %s
                    AND part.status IN ('pending', 'leased')""" + affected, parameters)
        return self.state_revision(revision_id)

    def active_state_revision(self, binding_id: str) -> StateRevision | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = %s AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
        return self.state_revision(row[0]) if row else None

    def partition_state_revision(self, partition_id: str) -> StateRevision | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT plan.state_revision FROM materialization_plans plan JOIN materialization_plan_partitions part ON part.plan_id = plan.plan_id WHERE part.partition_id = %s", [partition_id]).fetchone()
        return self.state_revision(row[0]) if row and row[0] else None

    def pending_state_invalidations(self) -> tuple[dict[str, object], ...]:
        """Return retained input ranges that a promoted state revision must rebuild."""
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT i.revision_id, i.binding_id, i.policy, i.effective_from,
                b.generation, b.graph_revision, r.ref_uri, coalesce(h.current_version, 0), min(t.ts), max(t.ts)
                FROM materialization_state_invalidations i
                JOIN materialization_bindings b ON b.binding_id = i.binding_id AND b.status = 'active'
                JOIN materialization_binding_refs r ON r.binding_id = b.binding_id
                    AND r.generation = b.generation AND r.direction = 'input'
                LEFT JOIN stream_heads h ON h.ref_uri = r.ref_uri
                LEFT JOIN timeseries t ON t.ref_uri = r.ref_uri AND NOT t.deleted
                WHERE i.status = 'pending'
                GROUP BY i.revision_id, i.binding_id, i.policy, i.effective_from,
                    b.generation, b.graph_revision, r.ref_uri, h.current_version""").fetchall()
        grouped: dict[str, dict[str, object]] = {}
        for revision, binding, policy, effective, generation, graph, ref, head, start, end in rows:
            item = grouped.setdefault(revision, {"revision_id": revision, "binding_id": binding,
                "policy": policy, "effective_from": effective, "generation": generation,
                "graph_revision": graph, "heads": {}, "ranges": []})
            item["heads"][ref] = head
            if start is not None:
                range_start = start
                if policy == "recompute_from" and effective is not None:
                    range_start = max(range_start, effective)
                if range_start <= end:
                    item["ranges"].append(TimeRange(range_start, end + timedelta(microseconds=1)))
        return tuple(grouped.values())

    def complete_state_invalidation(self, revision_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("UPDATE materialization_state_invalidations SET status = 'planned' "
                         "WHERE revision_id = %s AND status = 'pending'", [revision_id])

    def artifact_digests(self) -> set[str]:
        """Return every artifact retained by a durable candidate or revision."""
        with self._pool.connection() as conn:
            rows = conn.execute("SELECT DISTINCT artifact_digest FROM materialization_state_revisions").fetchall()
        return {row[0] for row in rows}

    def start_experiment(self, request: ExperimentRunRequest) -> ExperimentRun:
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO experiment_runs
                (run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json,
                 params_schema_json, metadata_json, input_vector_json, binding_snapshot_json,
                 state_revision, started_at, finished_at, error_json, keep_reason, collected_at)
                VALUES (%s, %s, %s, %s, %s, 'running', %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL)
                ON CONFLICT (run_id) DO NOTHING""", [request.run_id, request.definition_id, request.graph_revision,
                request.interval.start, request.interval.end, json.dumps(dict(request.params)),
                json.dumps(dict(request.params_schema)), json.dumps(dict(request.metadata)),
                json.dumps(dict(request.input_versions)), json.dumps(list(request.binding_snapshot)),
                request.state_revision, now])
        return self.experiment_run(request.run_id)

    def experiment_run(self, run_id: str) -> ExperimentRun:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json, params_schema_json, metadata_json, input_vector_json, binding_snapshot_json, state_revision, started_at, finished_at, error_json, keep_reason, collected_at FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        parse = lambda value: json.loads(value) if isinstance(value, str) else value
        return ExperimentRun(row[0], row[1], row[2], TimeRange(row[3], row[4]), row[5], parse(row[6]), parse(row[7]),
            parse(row[8]), parse(row[9]), parse(row[10]), row[11], row[12], row[13], parse(row[14]) if row[14] else None,
            row[15], row[16])

    def finish_experiment(self, run_id: str, *, status: str, error: dict | None = None) -> ExperimentRun:
        if status not in {"succeeded", "failed", "cancelled"}: raise ValueError("invalid experiment completion status")
        with self._pool.connection() as conn, conn.transaction():
            changed = conn.execute("UPDATE experiment_runs SET status = %s, error_json = %s, finished_at = %s WHERE run_id = %s AND status = 'running'", [status, json.dumps(error) if error else None, datetime.now(timezone.utc), run_id]).rowcount
            if changed == 0 and not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
        return self.experiment_run(run_id)

    def record_experiment_metric(self, run_id: str, name: str, value: object) -> None:
        if not name: raise ValueError("experiment metric name is required")
        try: encoded = json.dumps(value, sort_keys=True)
        except (TypeError, ValueError) as error: raise ValueError("experiment metric must be JSON-serializable") from error
        with self._pool.connection() as conn, conn.transaction():
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
            conn.execute("INSERT INTO experiment_run_metrics VALUES (%s, %s, %s, %s) ON CONFLICT (run_id, name) DO UPDATE SET value_json = excluded.value_json, recorded_at = excluded.recorded_at", [run_id, name, encoded, datetime.now(timezone.utc)])

    def attach_experiment_artifact(self, run_id: str, artifact: ExperimentArtifact) -> None:
        with self._pool.connection() as conn, conn.transaction():
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
            if not conn.execute("SELECT 1 FROM materialization_artifacts WHERE digest = %s", [artifact.digest]).fetchone(): raise KeyError(artifact.digest)
            conn.execute("INSERT INTO experiment_run_artifacts VALUES (%s, %s, %s, %s) ON CONFLICT (run_id, name) DO UPDATE SET artifact_digest = excluded.artifact_digest, metadata_json = excluded.metadata_json", [run_id, artifact.name, artifact.digest, json.dumps(dict(artifact.metadata))])

    def declare_experiment_output(self, run_id: str, name: str) -> str:
        ref_uri = run_output_ref(run_id, name)
        with self._pool.connection() as conn, conn.transaction():
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
            conn.execute("INSERT INTO experiment_run_outputs VALUES (%s, %s, %s) ON CONFLICT (run_id, name) DO NOTHING", [run_id, name, ref_uri])
        return ref_uri

    def keep_experiment(self, run_id: str, reason: str) -> ExperimentRun:
        if not reason: raise ValueError("a retention reason is required")
        with self._pool.connection() as conn, conn.transaction():
            if conn.execute("UPDATE experiment_runs SET keep_reason = %s WHERE run_id = %s", [reason, run_id]).rowcount == 0: raise KeyError(run_id)
        return self.experiment_run(run_id)

    def collect_experiment(self, run_id: str) -> ExperimentRun:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT keep_reason FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone()
            if row is None: raise KeyError(run_id)
            if row[0] is not None: raise ValueError("a kept experiment cannot be collected")
            conn.execute("DELETE FROM experiment_run_artifacts WHERE run_id = %s", [run_id])
            conn.execute("DELETE FROM experiment_run_outputs WHERE run_id = %s", [run_id])
            conn.execute("UPDATE experiment_runs SET status = 'collected', collected_at = %s WHERE run_id = %s", [datetime.now(timezone.utc), run_id])
        return self.experiment_run(run_id)

    def list_experiments(self, *, status: str | None = None, metadata: dict[str, object] | None = None) -> tuple[ExperimentRun, ...]:
        with self._pool.connection() as conn:
            rows = conn.execute("SELECT run_id FROM experiment_runs WHERE (%s::text IS NULL OR status = %s) ORDER BY started_at DESC", [status, status]).fetchall()
        runs = tuple(self.experiment_run(row[0]) for row in rows)
        return tuple(run for run in runs if metadata is None or all(run.metadata.get(key) == value for key, value in metadata.items()))

    def rerun_experiment(self, run_id: str, new_run_id: str) -> ExperimentRun:
        previous = self.experiment_run(run_id)
        if previous.status == "collected": raise ValueError("a collected experiment cannot be rerun")
        return self.start_experiment(ExperimentRunRequest(new_run_id, previous.definition_id, previous.graph_revision,
            previous.interval, previous.params, previous.params_schema, previous.metadata, previous.input_versions,
            previous.binding_snapshot, previous.state_revision))

    def experiment_metrics(self, run_id: str) -> dict[str, object]:
        with self._pool.connection() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
            rows = conn.execute("SELECT name, value_json FROM experiment_run_metrics WHERE run_id = %s ORDER BY name", [run_id]).fetchall()
        return {name: json.loads(value) if isinstance(value, str) else value for name, value in rows}

    def experiment_artifacts(self, run_id: str) -> tuple[ExperimentArtifact, ...]:
        with self._pool.connection() as conn:
            if not conn.execute("SELECT 1 FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone(): raise KeyError(run_id)
            rows = conn.execute("SELECT name, artifact_digest, metadata_json FROM experiment_run_artifacts WHERE run_id = %s ORDER BY name", [run_id]).fetchall()
        return tuple(ExperimentArtifact(name, digest, json.loads(metadata) if isinstance(metadata, str) else metadata) for name, digest, metadata in rows)

    def create_effect_intent(self, intent: EffectIntent) -> str:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT effect_id FROM materialization_effect_intents WHERE idempotency_key = %s", [intent.idempotency_key]).fetchone()
            if row: return row[0]
            conn.execute("INSERT INTO materialization_effect_intents VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, NULL, NULL)", [intent.effect_id, intent.execution_id, intent.kind, intent.destination, json.dumps(dict(intent.payload)), intent.idempotency_key])
        return intent.effect_id

    def lease_effect_intent(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EffectIntent | None:
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT effect_id, execution_id, kind, destination, payload_json, idempotency_key, attempts FROM materialization_effect_intents WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= %s) ORDER BY effect_id FOR UPDATE SKIP LOCKED LIMIT 1", [now]).fetchone()
            if row is None: return None
            conn.execute("UPDATE materialization_effect_intents SET status = 'leased', attempts = attempts + 1, next_attempt_at = %s WHERE effect_id = %s", [now + duration, row[0]])
        payload = json.loads(row[4]) if isinstance(row[4], str) else row[4]
        return EffectIntent(row[0], row[1], row[2], row[3], payload, row[5], "leased", row[6] + 1, now + duration)

    def complete_effect_intent(self, effect_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction(): conn.execute("UPDATE materialization_effect_intents SET status = 'delivered', next_attempt_at = NULL WHERE effect_id = %s AND status = 'leased'", [effect_id])

    def fail_effect_intent(self, effect_id: str, error: dict, *, retry_after: timedelta | None = None) -> None:
        status = 'pending' if retry_after is not None else 'dead_letter'; when = datetime.now(timezone.utc) + retry_after if retry_after else None
        with self._pool.connection() as conn, conn.transaction(): conn.execute("UPDATE materialization_effect_intents SET status = %s, next_attempt_at = %s, error_json = %s WHERE effect_id = %s AND status = 'leased'", [status, when, json.dumps(error), effect_id])

    def register_service(self, name: str, definition_id: str) -> None:
        with self._pool.connection() as conn, conn.transaction(): conn.execute("INSERT INTO materialization_services VALUES (%s, %s, 'registered', 'unknown', %s) ON CONFLICT (name) DO UPDATE SET definition_id = excluded.definition_id", [name, definition_id, datetime.now(timezone.utc)])

    def set_service_status(self, name: str, status: str, health: str = 'healthy') -> None:
        with self._pool.connection() as conn, conn.transaction():
            if conn.execute("UPDATE materialization_services SET status = %s, health = %s, updated_at = %s WHERE name = %s", [status, health, datetime.now(timezone.utc), name]).rowcount == 0: raise KeyError(name)

    def coalesce_service_hint(self, hint: ChangeHint) -> None:
        with self._pool.connection() as conn, conn.transaction(): conn.execute("INSERT INTO materialization_service_hints VALUES (%s, %s, %s, %s, %s) ON CONFLICT (service_name) DO UPDATE SET token = excluded.token, data_versions_json = excluded.data_versions_json, graph_revision = excluded.graph_revision, created_at = excluded.created_at", [hint.service_name, hint.token, json.dumps(dict(hint.data_versions)), hint.graph_revision, hint.created_at])

    def next_service_hint(self, name: str) -> ChangeHint | None:
        with self._pool.connection() as conn: row = conn.execute("SELECT token, data_versions_json, graph_revision, created_at FROM materialization_service_hints WHERE service_name = %s", [name]).fetchone()
        return ChangeHint(name, row[0], json.loads(row[1]) if isinstance(row[1], str) else row[1], row[2], row[3]) if row else None

    def acknowledge_service_hint(self, name: str, token: str) -> None:
        with self._pool.connection() as conn, conn.transaction(): conn.execute("DELETE FROM materialization_service_hints WHERE service_name = %s AND token = %s", [name, token])
