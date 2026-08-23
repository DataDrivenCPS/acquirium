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
from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.ids import materialization_id
from acquirium.Storage.artifacts import ArtifactRecord
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentArtifact, ExperimentRun, ExperimentRunRequest, frozen_inputs_match, run_output_ref
from acquirium.Materialization.effects import EffectIntent
from acquirium.Materialization.services import ChangeHint


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
            conn.execute("ALTER TABLE materialization_effect_intents ADD COLUMN IF NOT EXISTS lease_owner VARCHAR")
            conn.execute("ALTER TABLE materialization_effect_intents ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_services (
                name VARCHAR PRIMARY KEY, definition_id VARCHAR NOT NULL, status VARCHAR NOT NULL,
                health VARCHAR NOT NULL, updated_at TIMESTAMP NOT NULL, last_data_versions_json VARCHAR NOT NULL DEFAULT '{}',
                last_graph_revision BIGINT)""")
            # DuckDB cannot add a constrained column to an existing table.
            # The creation path has the default; old databases are backfilled
            # below and treated identically by the read path.
            conn.execute("ALTER TABLE materialization_services ADD COLUMN IF NOT EXISTS last_data_versions_json VARCHAR")
            conn.execute("ALTER TABLE materialization_services ADD COLUMN IF NOT EXISTS last_graph_revision BIGINT")
            conn.execute("UPDATE materialization_services SET last_data_versions_json = '{}' WHERE last_data_versions_json IS NULL")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_service_hints (
                service_name VARCHAR PRIMARY KEY, token VARCHAR NOT NULL, data_versions_json VARCHAR NOT NULL,
                graph_revision BIGINT, created_at TIMESTAMP NOT NULL)""")

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

    def service_definition(self, definition_id: str) -> dict[str, object]:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT source_digest, entrypoint, kind FROM materialization_definitions WHERE definition_id = ?", [definition_id]).fetchone()
        if row is None: raise KeyError(definition_id)
        if row[2] != "service": raise ValueError("definition is not a service")
        return {"source_digest": row[0], "entrypoint": row[1]}

    def stream_versions(self, refs: Sequence[str]) -> dict[str, int]:
        if not refs: return {}
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE
        with self._store._own_conn() as conn:
            conn.register("_acq_service_refs", pa.table({"ref_uri": list(refs)}))
            try:
                rows = conn.execute(f"""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                    FROM _acq_service_refs requested LEFT JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = requested.ref_uri
                    LEFT JOIN {STREAM_HEADS_TABLE} head ON head.ref_id = ids.ref_id ORDER BY requested.ref_uri""").fetchall()
            finally:
                conn.unregister("_acq_service_refs")
        return dict(rows)

    def all_stream_versions(self) -> dict[str, int]:
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE
        with self._store._own_conn() as conn:
            return dict(conn.execute(f"""SELECT ids.ref_uri, head.current_version FROM {STREAM_HEADS_TABLE} head
                JOIN {REF_IDS_TABLE} ids ON ids.ref_id = head.ref_id ORDER BY ids.ref_uri""").fetchall())

    def service_input_snapshot(self, refs: Sequence[str], *, since: datetime | None = None) -> tuple[dict[str, int], pa.Table]:
        """Read canonical service inputs without exposing backend internals.

        With ``since`` omitted, returns only the newest live row of each
        requested stream (bounded at one row per stream). With ``since`` given,
        returns every live row at or after that event time, for services that
        need a rolling window or the retained history.
        """
        from acquirium.Storage.duckdb_store import REF_IDS_TABLE, STREAM_HEADS_TABLE, TIMESERIES_TABLE
        with self._store._own_conn() as conn:
            conn.register("_acq_service_snapshot_refs", pa.table({"ref_uri": list(refs)}))
            try:
                versions = dict(conn.execute(f"""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                    FROM _acq_service_snapshot_refs requested LEFT JOIN {REF_IDS_TABLE} ids ON ids.ref_uri = requested.ref_uri
                    LEFT JOIN {STREAM_HEADS_TABLE} head ON head.ref_id = ids.ref_id""").fetchall())
                if since is None:
                    rows = conn.execute(f"""SELECT ref_uri, ts, numeric_value, text_value FROM (
                            SELECT ids.ref_uri AS ref_uri, value.ts AS ts, value.numeric_value AS numeric_value,
                                   value.text_value AS text_value,
                                   row_number() OVER (PARTITION BY ids.ref_uri ORDER BY value.ts DESC) AS recency
                            FROM {TIMESERIES_TABLE} value JOIN {REF_IDS_TABLE} ids ON ids.ref_id = value.ref_id
                            WHERE ids.ref_uri IN (SELECT ref_uri FROM _acq_service_snapshot_refs) AND NOT value.deleted
                        ) latest WHERE recency = 1 ORDER BY ref_uri""").fetchall()
                else:
                    rows = conn.execute(f"""SELECT ids.ref_uri, value.ts, value.numeric_value, value.text_value
                        FROM {TIMESERIES_TABLE} value JOIN {REF_IDS_TABLE} ids ON ids.ref_id = value.ref_id
                        WHERE ids.ref_uri IN (SELECT ref_uri FROM _acq_service_snapshot_refs)
                          AND NOT value.deleted AND value.ts >= ?
                        ORDER BY ids.ref_uri, value.ts""", [self._store._to_utc_naive(since)]).fetchall()
            finally:
                conn.unregister("_acq_service_snapshot_refs")
        return versions, pa.table({
            "ref_uri": [row[0] for row in rows],
            "ts": pa.array([row[1].replace(tzinfo=timezone.utc) for row in rows], type=pa.timestamp("us", tz="UTC")),
            "numeric_value": pa.array([row[2] for row in rows], type=pa.float64()),
            "text_value": pa.array([row[3] for row in rows], type=pa.string()),
        })


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

    def active_state_revision(self, binding_id: str) -> StateRevision | None:
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = ? AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
        return self.state_revision(row[0]) if row else None

    def artifact_digests(self) -> set[str]:
        """Return every artifact retained by a durable candidate or revision."""
        with self._store._own_conn() as conn:
            rows = conn.execute("SELECT DISTINCT artifact_digest FROM materialization_state_revisions").fetchall()
        return {row[0] for row in rows}

    def start_experiment(self, request: ExperimentRunRequest) -> ExperimentRun:
        """Persist an immutable experiment snapshot before any user code runs.

        Reusing an existing run_id is an idempotent replay only when every
        frozen input matches; otherwise the request is rejected so a run id
        can never be silently repurposed for different work.
        """
        try:
            existing = self.experiment_run(request.run_id)
        except KeyError:
            existing = None
        if existing is not None:
            if frozen_inputs_match(existing, request):
                return existing
            raise ValueError(f"experiment run_id {request.run_id!r} already exists with different frozen inputs")
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

    @staticmethod
    def _row_to_experiment_run(row) -> ExperimentRun:
        """Decode one full experiment_runs row (shared by single and list reads)."""
        return ExperimentRun(row[0], row[1], row[2], TimeRange(row[3].replace(tzinfo=timezone.utc), row[4].replace(tzinfo=timezone.utc)),
            row[5], json.loads(row[6]), json.loads(row[7]), json.loads(row[8]), json.loads(row[9]), json.loads(row[10]), row[11],
            row[12].replace(tzinfo=timezone.utc), row[13].replace(tzinfo=timezone.utc) if row[13] else None,
            json.loads(row[14]) if row[14] else None, row[15], row[16].replace(tzinfo=timezone.utc) if row[16] else None)

    _EXPERIMENT_RUN_COLUMNS = ("run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json, "
        "params_schema_json, metadata_json, input_vector_json, binding_snapshot_json, state_revision, "
        "started_at, finished_at, error_json, keep_reason, collected_at")

    def experiment_run(self, run_id: str) -> ExperimentRun:
        with self._store._own_conn() as conn:
            row = conn.execute(f"SELECT {self._EXPERIMENT_RUN_COLUMNS} FROM experiment_runs WHERE run_id = ?", [run_id]).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self._row_to_experiment_run(row)

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
        # One query returns every column so listing does not issue a read per run.
        with self._store._own_conn() as conn:
            rows = conn.execute(f"SELECT {self._EXPERIMENT_RUN_COLUMNS} FROM experiment_runs "
                "WHERE (? IS NULL OR status = ?) ORDER BY started_at DESC", [status, status]).fetchall()
        runs = tuple(self._row_to_experiment_run(row) for row in rows)
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

    def create_effect_intent(self, intent: EffectIntent) -> str:
        with self._store._lock, self._store._write_conn() as conn:
            row = conn.execute("SELECT effect_id FROM materialization_effect_intents WHERE idempotency_key = ?", [intent.idempotency_key]).fetchone()
            if row: return row[0]
            conn.execute("""INSERT INTO materialization_effect_intents
                (effect_id, execution_id, kind, destination, payload_json, idempotency_key, status, attempts)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', 0)""", [intent.effect_id, intent.execution_id,
                intent.kind, intent.destination, json.dumps(dict(intent.payload), sort_keys=True), intent.idempotency_key])
        return intent.effect_id

    def lease_effect_intent(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EffectIntent | None:
        if not owner:
            raise ValueError("effect lease owner is required")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            # A process can die after acquiring a lease.  Lease expiry is the
            # recovery mechanism; notifications and worker memory are never
            # correctness requirements for an external effect.
            conn.execute("""UPDATE materialization_effect_intents SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL, next_attempt_at = ?
                WHERE status = 'leased' AND lease_expires_at <= ?""", [now, now])
            row = conn.execute("""SELECT effect_id, execution_id, kind, destination, payload_json,
                idempotency_key, attempts FROM materialization_effect_intents
                WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY effect_id LIMIT 1""", [now]).fetchone()
            if row is None: return None
            expires = now + duration
            conn.execute("""UPDATE materialization_effect_intents SET status = 'leased', attempts = attempts + 1,
                lease_owner = ?, lease_expires_at = ?, next_attempt_at = NULL WHERE effect_id = ?""", [owner, expires, row[0]])
        return EffectIntent(row[0], row[1], row[2], row[3], json.loads(row[4]), row[5], "leased", row[6] + 1,
                            None, None, owner, expires)

    def complete_effect_intent(self, effect_id: str, owner: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._store._lock, self._store._write_conn() as conn:
            held = conn.execute("""SELECT 1 FROM materialization_effect_intents WHERE effect_id = ?
                AND status = 'leased' AND lease_owner = ? AND lease_expires_at > ?""", [effect_id, owner, now]).fetchone()
            if held is None: raise ValueError("effect lease is not held by this owner")
            conn.execute("""UPDATE materialization_effect_intents SET status = 'delivered', next_attempt_at = NULL,
                lease_owner = NULL, lease_expires_at = NULL WHERE effect_id = ?""", [effect_id])

    def fail_effect_intent(self, effect_id: str, owner: str, error: dict, *, retry_after: timedelta | None = None) -> None:
        status = 'pending' if retry_after is not None else 'dead_letter'
        when = (datetime.now(timezone.utc).replace(tzinfo=None) + retry_after) if retry_after else None
        with self._store._lock, self._store._write_conn() as conn:
            held = conn.execute("""SELECT 1 FROM materialization_effect_intents WHERE effect_id = ?
                AND status = 'leased' AND lease_owner = ?""", [effect_id, owner]).fetchone()
            if held is None: raise ValueError("effect lease is not held by this owner")
            conn.execute("""UPDATE materialization_effect_intents SET status = ?, next_attempt_at = ?,
                error_json = ?, lease_owner = NULL, lease_expires_at = NULL WHERE effect_id = ?""",
                [status, when, json.dumps(error, sort_keys=True), effect_id])

    def effect_intent(self, effect_id: str) -> EffectIntent:
        with self._store._own_conn() as conn:
            row = conn.execute("""SELECT effect_id, execution_id, kind, destination, payload_json, idempotency_key,
                status, attempts, next_attempt_at, error_json, lease_owner, lease_expires_at
                FROM materialization_effect_intents WHERE effect_id = ?""", [effect_id]).fetchone()
        if row is None: raise KeyError(effect_id)
        return EffectIntent(row[0], row[1], row[2], row[3], json.loads(row[4]), row[5], row[6], row[7],
            row[8].replace(tzinfo=timezone.utc) if row[8] else None, json.loads(row[9]) if row[9] else None,
            row[10], row[11].replace(tzinfo=timezone.utc) if row[11] else None)

    def register_service(self, name: str, definition_id: str):
        if not name: raise ValueError("service name is required")
        with self._store._lock, self._store._write_conn() as conn:
            definition = conn.execute("SELECT kind FROM materialization_definitions WHERE definition_id = ?", [definition_id]).fetchone()
            if definition is None: raise KeyError(definition_id)
            if definition[0] != "service": raise ValueError("definition is not a service")
            conn.execute("""INSERT INTO materialization_services (name, definition_id, status, health, updated_at)
                VALUES (?, ?, 'registered', 'unknown', ?) ON CONFLICT (name) DO UPDATE SET definition_id = excluded.definition_id""",
                [name, definition_id, datetime.now(timezone.utc).replace(tzinfo=None)])
        return self.service(name)

    def set_service_status(self, name: str, status: str, health: str = 'healthy'):
        if status not in {"registered", "running", "stopped", "failed"}: raise ValueError("invalid service status")
        with self._store._lock, self._store._write_conn() as conn:
            if conn.execute("SELECT 1 FROM materialization_services WHERE name = ?", [name]).fetchone() is None: raise KeyError(name)
            conn.execute("UPDATE materialization_services SET status = ?, health = ?, updated_at = ? WHERE name = ?", [status, health, datetime.now(timezone.utc).replace(tzinfo=None), name])
        return self.service(name)

    def service(self, name: str):
        from acquirium.Materialization.services import ServiceRecord
        with self._store._own_conn() as conn:
            row = conn.execute("SELECT name, definition_id, status, health, updated_at FROM materialization_services WHERE name = ?", [name]).fetchone()
        if row is None: raise KeyError(name)
        return ServiceRecord(row[0], row[1], row[2], row[3], row[4].replace(tzinfo=timezone.utc))

    def services(self, *, status: str | None = None):
        """Return every service record in one query (no per-row round trips)."""
        from acquirium.Materialization.services import ServiceRecord
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT name, definition_id, status, health, updated_at
                FROM materialization_services WHERE (? IS NULL OR status = ?) ORDER BY name""", [status, status]).fetchall()
        return tuple(ServiceRecord(name, definition_id, service_status, health, updated_at.replace(tzinfo=timezone.utc))
                     for name, definition_id, service_status, health, updated_at in rows)

    def services_needing_hint(self, data_versions: dict[str, int], graph_revision: int | None):
        with self._store._own_conn() as conn:
            rows = conn.execute("SELECT name, last_data_versions_json, last_graph_revision FROM materialization_services WHERE status = 'running'").fetchall()
        return tuple(name for name, versions, graph in rows
                     if json.loads(versions) != data_versions or graph != graph_revision)

    def coalesce_service_hint(self, hint: ChangeHint) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            if conn.execute("SELECT 1 FROM materialization_services WHERE name = ?", [hint.service_name]).fetchone() is None: raise KeyError(hint.service_name)
            existing = conn.execute("SELECT data_versions_json, graph_revision FROM materialization_service_hints WHERE service_name = ?", [hint.service_name]).fetchone()
            versions = dict(hint.data_versions)
            graph_revision = hint.graph_revision
            if existing is not None:
                prior = json.loads(existing[0])
                versions = {key: max(int(prior.get(key, value)), int(value)) for key, value in ({**prior, **versions}).items()}
                graph_revision = max(item for item in (existing[1], graph_revision) if item is not None) if existing[1] is not None or graph_revision is not None else None
            conn.execute("INSERT INTO materialization_service_hints VALUES (?, ?, ?, ?, ?) ON CONFLICT (service_name) DO UPDATE SET token = excluded.token, data_versions_json = excluded.data_versions_json, graph_revision = excluded.graph_revision, created_at = excluded.created_at", [hint.service_name, hint.token, json.dumps(versions, sort_keys=True), graph_revision, hint.created_at.replace(tzinfo=None)])

    def next_service_hint(self, name: str) -> ChangeHint | None:
        with self._store._own_conn() as conn: row = conn.execute("SELECT token, data_versions_json, graph_revision, created_at FROM materialization_service_hints WHERE service_name = ?", [name]).fetchone()
        return ChangeHint(name, row[0], json.loads(row[1]), row[2], row[3].replace(tzinfo=timezone.utc)) if row else None

    def acknowledge_service_hint(self, name: str, token: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            hint = conn.execute("SELECT data_versions_json, graph_revision FROM materialization_service_hints WHERE service_name = ? AND token = ?", [name, token]).fetchone()
            if hint is None: return
            conn.execute("UPDATE materialization_services SET last_data_versions_json = ?, last_graph_revision = ?, updated_at = ? WHERE name = ?", [hint[0], hint[1], datetime.now(timezone.utc).replace(tzinfo=None), name])
            conn.execute("DELETE FROM materialization_service_hints WHERE service_name = ? AND token = ?", [name, token])
