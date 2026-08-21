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
from acquirium.Storage.materialization.types import PlanPartition, WorkLease
from acquirium.Storage.materialization.types import InputSnapshot
from acquirium.Storage.continuous.types import MUTATION_SCHEMA


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
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_bindings (
                binding_id TEXT NOT NULL, deployment_name TEXT NOT NULL, generation BIGINT NOT NULL,
                logical_key TEXT NOT NULL, content_digest TEXT NOT NULL, graph_revision BIGINT NOT NULL,
                resolved_metadata_json JSONB NOT NULL, status TEXT NOT NULL,
                PRIMARY KEY (binding_id, generation))""")
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
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_plan_partitions (
                partition_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, start_ts TIMESTAMPTZ NOT NULL,
                end_ts TIMESTAMPTZ NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_expires_at TIMESTAMPTZ, committed_output_id TEXT, error_json JSONB)""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_materialization_partitions_pending ON materialization_plan_partitions (status, start_ts)")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_execution_receipts (
                execution_id TEXT PRIMARY KEY, partition_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                input_vector_json JSONB NOT NULL, output_publication_id TEXT, status TEXT NOT NULL,
                rows_read BIGINT NOT NULL, rows_written BIGINT NOT NULL, error_json JSONB, finished_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_binding_progress (
                binding_id TEXT NOT NULL, generation BIGINT NOT NULL, ref_uri TEXT NOT NULL,
                stream_version BIGINT NOT NULL, PRIMARY KEY (binding_id, generation, ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_attempt_snapshots (
                partition_id TEXT NOT NULL, attempt INTEGER NOT NULL, input_vector_json JSONB NOT NULL,
                rows_read BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (partition_id, attempt))""")

    def close(self) -> None:
        self._pool.close()

    def register_definition(self, definition: MaterializationDefinition) -> str:
        spec = definition_spec(definition)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO materialization_definitions
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (definition_id) DO NOTHING""",
                [definition.definition_id, definition.name, definition.kind, definition.source_digest,
                 definition.entrypoint, json.dumps(spec), datetime.now(timezone.utc)])
        return definition.definition_id

    def deploy(self, name: str, definition_id: str, *, graph_revision: int | None = None) -> int:
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT generation, definition_id FROM materialization_deployments WHERE name = %s FOR UPDATE", [name]).fetchone()
            generation = 1 if row is None else row[0] if row[1] == definition_id else row[0] + 1
            conn.execute("""INSERT INTO materialization_deployments VALUES (%s, %s, %s, 'registered', %s, %s)
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
                conn.execute("""INSERT INTO materialization_bindings VALUES (%s, %s, %s, %s, %s, %s, %s, 'staging')
                    ON CONFLICT (binding_id, generation) DO UPDATE SET content_digest = excluded.content_digest,
                    graph_revision = excluded.graph_revision, resolved_metadata_json = excluded.resolved_metadata_json""",
                    [binding_id, deployment_name, generation, binding.logical_key, binding.content_digest,
                     graph_revision, json.dumps(binding.metadata)])
                for direction, roles in (("input", binding.inputs), ("output", binding.outputs)):
                    for role, refs in roles.items():
                        with conn.cursor() as cursor:
                            cursor.executemany(
                                "INSERT INTO materialization_binding_refs VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                [(binding_id, generation, ref, role, direction) for ref in refs],
                            )

    def record_graph_revision(self, graph_revision: int, source_version: int, content_digest: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("INSERT INTO materialization_graph_revisions VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                         [graph_revision, source_version, content_digest, datetime.now(timezone.utc)])

    def request_rebind(self, deployment_name: str, graph_revision: int) -> None:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""INSERT INTO materialization_rebind_requests (deployment_name, graph_revision, status)
                VALUES (%s, %s, 'pending') ON CONFLICT (deployment_name, graph_revision) DO UPDATE SET
                    status = 'pending', error_json = NULL
                WHERE materialization_rebind_requests.status = 'failed'""", [deployment_name, graph_revision])

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
            row = conn.execute("""SELECT d.definition_id, deployment.generation, d.spec_json
                FROM materialization_deployments deployment
                JOIN materialization_definitions d ON d.definition_id = deployment.definition_id
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
                h.current_version, coalesce(p.stream_version, 0) AS progress
                FROM materialization_bindings b
                JOIN materialization_binding_refs r ON r.binding_id = b.binding_id AND r.generation = b.generation AND r.direction = 'input'
                JOIN stream_heads h ON h.ref_uri = r.ref_uri
                LEFT JOIN materialization_binding_progress p ON p.binding_id = b.binding_id AND p.generation = b.generation AND p.ref_uri = r.ref_uri
                WHERE b.status IN ('active', 'staging') AND h.current_version > coalesce(p.stream_version, 0)
                ORDER BY b.binding_id, r.ref_uri""").fetchall()
        grouped: dict[tuple[str, int, int], dict[str, object]] = {}
        for binding, deployment_name, generation, graph, ref, head, progress in rows:
            item = grouped.setdefault((binding, generation, graph), {"binding_id": binding, "deployment_name": deployment_name, "generation": generation, "graph_revision": graph, "heads": {}, "progress": {}})
            item["heads"][ref] = head
            item["progress"][ref] = progress
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
            conn.execute("""INSERT INTO materialization_plans VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, NULL)
                ON CONFLICT (plan_id) DO NOTHING""", [plan_id, binding_id, generation, graph_revision,
                json.dumps(input_vector), json.dumps(reason), datetime.now(timezone.utc)])
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

    def lease_registered_partition(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> WorkLease | None:
        """Lease pending work only when its deployment has been started."""
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
                WHERE status = 'leased' AND lease_expires_at <= %s""", [now])
            row = conn.execute("""SELECT part.partition_id, part.plan_id, part.start_ts, part.end_ts, part.attempt
                FROM materialization_plan_partitions part
                JOIN materialization_plans plan ON plan.plan_id = part.plan_id
                JOIN materialization_bindings binding ON binding.binding_id = plan.binding_id AND binding.generation = plan.generation
                JOIN materialization_deployments deployment ON deployment.name = binding.deployment_name AND deployment.generation = plan.generation
                WHERE part.status = 'pending' AND deployment.status = 'active'
                ORDER BY part.start_ts, part.partition_id FOR UPDATE OF part SKIP LOCKED LIMIT 1""").fetchone()
            if row is None:
                return None
            partition_id, plan_id, start, end, prior_attempt = row
            attempt, expires = prior_attempt + 1, now + duration
            conn.execute("""UPDATE materialization_plan_partitions SET status = 'leased', attempt = %s, lease_owner = %s, lease_expires_at = %s
                WHERE partition_id = %s""", [attempt, owner, expires, partition_id])
        return WorkLease(PlanPartition(partition_id, plan_id, TimeRange(start, end), "leased"), owner, attempt, expires)

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
                JOIN materialization_deployments deployment ON deployment.name = binding.deployment_name AND deployment.generation = plan.generation
                JOIN materialization_definitions d ON d.definition_id = deployment.definition_id
                WHERE part.partition_id = %s""", [partition_id]).fetchone()
        if row is None:
            raise KeyError(partition_id)
        digest, entrypoint, spec = row
        if isinstance(spec, str):
            spec = json.loads(spec)
        return {"source_digest": digest, "entrypoint": entrypoint, "spec": spec}

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
            reason = cur.execute("SELECT reason_json FROM materialization_plans WHERE plan_id = %s", [snapshot.lease.partition.plan_id]).fetchone()[0]
            from acquirium.Materialization.impact import ImpactPolicy
            # Keep policy expansion in Python so DuckDB/PostgreSQL have identical semantics.
            impact = ImpactPolicy.from_json((json.loads(reason) if isinstance(reason, str) else reason).get("impact", {"kind": "pointwise"}))
            for ref, version in snapshot.input_versions.items():
                for start, end in cur.execute("SELECT start_ts, end_ts FROM stream_change_ranges WHERE ref_uri = %s AND stream_version > %s", [ref, version]).fetchall():
                    if impact.affected(TimeRange(start, end)).intersects(interval):
                        raise StaleAttemptError("a newer intersecting input change exists")
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
            cur.execute("""INSERT INTO materialization_execution_receipts VALUES (%s, %s, %s, %s, %s, 'committed', %s, %s, NULL, %s)
                ON CONFLICT DO NOTHING""", [execution_id, snapshot.lease.partition.partition_id, snapshot.lease.attempt, json.dumps(snapshot.input_versions), publication_id, snapshot.inputs.num_rows, len(rows), datetime.now(timezone.utc)])
            if cur.execute("SELECT count(*) FROM materialization_plan_partitions WHERE plan_id = %s AND status != 'committed'", [snapshot.lease.partition.plan_id]).fetchone()[0] == 0:
                cur.execute("UPDATE materialization_plans SET status = 'committed', completed_at = %s WHERE plan_id = %s", [datetime.now(timezone.utc), snapshot.lease.partition.plan_id])
                binding, generation, captured = cur.execute("SELECT binding_id, generation, input_vector_json FROM materialization_plans WHERE plan_id = %s", [snapshot.lease.partition.plan_id]).fetchone()
                captured = json.loads(captured) if isinstance(captured, str) else captured
                for ref, version in captured.items():
                    cur.execute("""INSERT INTO materialization_binding_progress VALUES (%s, %s, %s, %s)
                        ON CONFLICT (binding_id, generation, ref_uri) DO UPDATE SET stream_version = greatest(materialization_binding_progress.stream_version, excluded.stream_version)""", [binding, generation, ref, version])
        return publication_id
