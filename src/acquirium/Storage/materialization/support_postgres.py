"""PostgreSQL support persistence outside the topology epoch control plane."""
from __future__ import annotations

from datetime import datetime, timezone

from psycopg_pool import ConnectionPool

from acquirium.Storage.materialization.postgres import MaterializationPostgres


class MaterializationSupportPostgres(MaterializationPostgres):
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 4) -> None:
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_definitions (
                definition_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                source_digest TEXT NOT NULL, entrypoint TEXT NOT NULL, spec_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL)""")
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
                parent_revision TEXT, artifact_digest TEXT NOT NULL, request_id TEXT NOT NULL,
                policy TEXT, effective_from TIMESTAMPTZ, status TEXT NOT NULL, metrics_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL, activated_at TIMESTAMPTZ)""")
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
                status TEXT NOT NULL, attempts INTEGER NOT NULL, next_attempt_at TIMESTAMPTZ, error_json JSONB,
                lease_owner TEXT, lease_expires_at TIMESTAMPTZ)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_services (
                name TEXT PRIMARY KEY, definition_id TEXT NOT NULL, status TEXT NOT NULL,
                health TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL, last_data_versions_json JSONB NOT NULL DEFAULT '{}',
                last_graph_revision BIGINT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_service_hints (
                service_name TEXT PRIMARY KEY, token TEXT NOT NULL, data_versions_json JSONB NOT NULL,
                graph_revision BIGINT, created_at TIMESTAMPTZ NOT NULL)""")

    def promote_state_revision(self, revision_id: str, *, policy: str = "prospective", effective_from=None):
        if policy not in {"prospective", "recompute_all", "recompute_from"}:
            raise ValueError("unknown promotion policy")
        if policy == "recompute_from" and effective_from is None:
            raise ValueError("recompute_from requires effective_from")
        with self._pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT binding_id, status FROM materialization_state_revisions WHERE revision_id = %s", [revision_id]).fetchone()
            if row is None:
                raise KeyError(revision_id)
            if row[1] not in {"candidate", "active"}:
                raise ValueError("only candidate revisions may be promoted")
            now = datetime.now(timezone.utc)
            conn.execute("UPDATE materialization_state_revisions SET status = 'retired' WHERE binding_id = %s AND status = 'active' AND revision_id != %s", [row[0], revision_id])
            conn.execute("UPDATE materialization_state_revisions SET status = 'active', policy = %s, effective_from = %s, activated_at = %s WHERE revision_id = %s", [policy, effective_from, now, revision_id])
        return self.state_revision(revision_id)
