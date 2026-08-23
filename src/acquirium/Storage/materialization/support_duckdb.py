"""Non-topology materialization support tables for DuckDB.

Transformations do not use this class.  It retains only immutable definition,
artifact, experiment, service, and effect persistence needed by those
independent APIs; topology work lives in :mod:`epoch_duckdb`.
"""
from __future__ import annotations

from datetime import datetime, timezone

from acquirium.Storage.materialization.duckdb import MaterializationDuckDB


class MaterializationSupportDuckDB(MaterializationDuckDB):
    def __init__(self, store) -> None:
        self._store = store
        with store._lock, store._write_conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_definitions (
                definition_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, kind VARCHAR NOT NULL,
                source_digest VARCHAR NOT NULL, entrypoint VARCHAR NOT NULL, spec_json VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL)""")
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
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_effect_intents (
                effect_id VARCHAR PRIMARY KEY, execution_id VARCHAR NOT NULL, kind VARCHAR NOT NULL,
                destination VARCHAR NOT NULL, payload_json VARCHAR NOT NULL, idempotency_key VARCHAR UNIQUE NOT NULL,
                status VARCHAR NOT NULL, attempts INTEGER NOT NULL, next_attempt_at TIMESTAMP, error_json VARCHAR,
                lease_owner VARCHAR, lease_expires_at TIMESTAMP)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_services (
                name VARCHAR PRIMARY KEY, definition_id VARCHAR NOT NULL, status VARCHAR NOT NULL,
                health VARCHAR NOT NULL, updated_at TIMESTAMP NOT NULL, last_data_versions_json VARCHAR NOT NULL DEFAULT '{}',
                last_graph_revision BIGINT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_service_hints (
                service_name VARCHAR PRIMARY KEY, token VARCHAR NOT NULL, data_versions_json VARCHAR NOT NULL,
                graph_revision BIGINT, created_at TIMESTAMP NOT NULL)""")

    def promote_state_revision(self, revision_id: str, *, policy: str = "prospective", effective_from=None):
        if policy not in {"prospective", "recompute_all", "recompute_from"}:
            raise ValueError("unknown promotion policy")
        if policy == "recompute_from" and effective_from is None:
            raise ValueError("recompute_from requires effective_from")
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT binding_id, status FROM materialization_state_revisions WHERE revision_id = ?", [revision_id]).fetchone()
            if row is None:
                raise KeyError(revision_id)
            if row[1] not in {"candidate", "active"}:
                raise ValueError("only candidate revisions may be promoted")
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            conn.execute("UPDATE materialization_state_revisions SET status = 'retired' WHERE binding_id = ? AND status = 'active' AND revision_id != ?", [row[0], revision_id])
            conn.execute("UPDATE materialization_state_revisions SET status = 'active', policy = ?, effective_from = ?, activated_at = ? WHERE revision_id = ?", [policy, effective_from.replace(tzinfo=None) if effective_from else None, now, revision_id])
        return self.state_revision(revision_id)
