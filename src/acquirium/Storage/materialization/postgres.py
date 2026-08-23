"""PostgreSQL persistence for Phase 2 materialization metadata."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Sequence
import pyarrow as pa

from psycopg_pool import ConnectionPool

from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.ids import materialization_id
from acquirium.Storage.materialization.types import StreamChangeRange
from acquirium.Storage.artifacts import ArtifactRecord
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentArtifact, ExperimentRun, ExperimentRunRequest, frozen_inputs_match, run_output_ref
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
            conn.execute("ALTER TABLE materialization_effect_intents ADD COLUMN IF NOT EXISTS lease_owner TEXT")
            conn.execute("ALTER TABLE materialization_effect_intents ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ")
            conn.execute("""CREATE TABLE IF NOT EXISTS materialization_services (
                name TEXT PRIMARY KEY, definition_id TEXT NOT NULL, status TEXT NOT NULL,
                health TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL, last_data_versions_json JSONB NOT NULL DEFAULT '{}',
                last_graph_revision BIGINT)""")
            conn.execute("ALTER TABLE materialization_services ADD COLUMN IF NOT EXISTS last_data_versions_json JSONB NOT NULL DEFAULT '{}'")
            conn.execute("ALTER TABLE materialization_services ADD COLUMN IF NOT EXISTS last_graph_revision BIGINT")
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

    def service_definition(self, definition_id: str) -> dict[str, object]:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT source_digest, entrypoint, kind FROM materialization_definitions WHERE definition_id = %s", [definition_id]).fetchone()
        if row is None: raise KeyError(definition_id)
        if row[2] != "service": raise ValueError("definition is not a service")
        return {"source_digest": row[0], "entrypoint": row[1]}

    def stream_versions(self, refs: Sequence[str]) -> dict[str, int]:
        if not refs: return {}
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                FROM unnest(%s::text[]) AS requested(ref_uri) LEFT JOIN stream_heads head
                ON head.ref_uri = requested.ref_uri ORDER BY requested.ref_uri""", [list(refs)]).fetchall()
        return dict(rows)

    def all_stream_versions(self) -> dict[str, int]:
        with self._pool.connection() as conn:
            return dict(conn.execute("SELECT ref_uri, current_version FROM stream_heads ORDER BY ref_uri").fetchall())

    def service_input_snapshot(self, refs: Sequence[str], *, since: datetime | None = None) -> tuple[dict[str, int], pa.Table]:
        """Read canonical service inputs.

        With ``since`` omitted, returns only the newest live row of each
        requested stream (bounded at one row per stream). With ``since`` given,
        returns every live row at or after that event time, for services that
        need a rolling window or the retained history.
        """
        with self._pool.connection() as conn:
            versions = dict(conn.execute("""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                FROM unnest(%s::text[]) AS requested(ref_uri) LEFT JOIN stream_heads head
                ON head.ref_uri = requested.ref_uri""", [list(refs)]).fetchall())
            if since is None:
                rows = conn.execute("""SELECT DISTINCT ON (ref_uri) ref_uri, ts, numeric_value, text_value
                    FROM timeseries WHERE ref_uri = ANY(%s::text[]) AND NOT deleted
                    ORDER BY ref_uri, ts DESC""", [list(refs)]).fetchall()
            else:
                rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM timeseries
                    WHERE ref_uri = ANY(%s::text[]) AND NOT deleted AND ts >= %s
                    ORDER BY ref_uri, ts""", [list(refs), since]).fetchall()
        return versions, pa.table({
            "ref_uri": [row[0] for row in rows],
            "ts": pa.array([row[1] for row in rows], type=pa.timestamp("us", tz="UTC")),
            "numeric_value": pa.array([row[2] for row in rows], type=pa.float64()),
            "text_value": pa.array([row[3] for row in rows], type=pa.string()),
        })


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

    def active_state_revision(self, binding_id: str) -> StateRevision | None:
        with self._pool.connection() as conn:
            row = conn.execute("SELECT revision_id FROM materialization_state_revisions WHERE binding_id = %s AND status = 'active' ORDER BY activated_at DESC LIMIT 1", [binding_id]).fetchone()
        return self.state_revision(row[0]) if row else None

    def artifact_digests(self) -> set[str]:
        """Return every artifact retained by a durable candidate or revision."""
        with self._pool.connection() as conn:
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

    @staticmethod
    def _row_to_experiment_run(row) -> ExperimentRun:
        """Decode one full experiment_runs row (shared by single and list reads)."""
        parse = lambda value: json.loads(value) if isinstance(value, str) else value
        return ExperimentRun(row[0], row[1], row[2], TimeRange(row[3], row[4]), row[5], parse(row[6]), parse(row[7]),
            parse(row[8]), parse(row[9]), parse(row[10]), row[11], row[12], row[13], parse(row[14]) if row[14] else None,
            row[15], row[16])

    _EXPERIMENT_RUN_COLUMNS = ("run_id, definition_id, graph_revision, start_ts, end_ts, status, params_json, "
        "params_schema_json, metadata_json, input_vector_json, binding_snapshot_json, state_revision, "
        "started_at, finished_at, error_json, keep_reason, collected_at")

    def experiment_run(self, run_id: str) -> ExperimentRun:
        with self._pool.connection() as conn:
            row = conn.execute(f"SELECT {self._EXPERIMENT_RUN_COLUMNS} FROM experiment_runs WHERE run_id = %s", [run_id]).fetchone()
        if row is None: raise KeyError(run_id)
        return self._row_to_experiment_run(row)

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
        # One query returns every column so listing does not issue a read per run.
        with self._pool.connection() as conn:
            rows = conn.execute(f"SELECT {self._EXPERIMENT_RUN_COLUMNS} FROM experiment_runs "
                "WHERE (%s::text IS NULL OR status = %s) ORDER BY started_at DESC", [status, status]).fetchall()
        runs = tuple(self._row_to_experiment_run(row) for row in rows)
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
            conn.execute("""INSERT INTO materialization_effect_intents
                (effect_id, execution_id, kind, destination, payload_json, idempotency_key, status, attempts)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0)""", [intent.effect_id, intent.execution_id,
                intent.kind, intent.destination, json.dumps(dict(intent.payload)), intent.idempotency_key])
        return intent.effect_id

    def lease_effect_intent(self, owner: str, *, duration: timedelta = timedelta(minutes=5)) -> EffectIntent | None:
        if not owner: raise ValueError("effect lease owner is required")
        now = datetime.now(timezone.utc)
        with self._pool.connection() as conn, conn.transaction():
            conn.execute("""UPDATE materialization_effect_intents SET status = 'pending', lease_owner = NULL,
                lease_expires_at = NULL, next_attempt_at = %s WHERE status = 'leased' AND lease_expires_at <= %s""", [now, now])
            row = conn.execute("""SELECT effect_id, execution_id, kind, destination, payload_json, idempotency_key,
                attempts FROM materialization_effect_intents WHERE status = 'pending' AND
                (next_attempt_at IS NULL OR next_attempt_at <= %s) ORDER BY effect_id FOR UPDATE SKIP LOCKED LIMIT 1""", [now]).fetchone()
            if row is None: return None
            expires = now + duration
            conn.execute("""UPDATE materialization_effect_intents SET status = 'leased', attempts = attempts + 1,
                lease_owner = %s, lease_expires_at = %s, next_attempt_at = NULL WHERE effect_id = %s""", [owner, expires, row[0]])
        payload = json.loads(row[4]) if isinstance(row[4], str) else row[4]
        return EffectIntent(row[0], row[1], row[2], row[3], payload, row[5], "leased", row[6] + 1, None, None, owner, expires)

    def complete_effect_intent(self, effect_id: str, owner: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            changed = conn.execute("""UPDATE materialization_effect_intents SET status = 'delivered', next_attempt_at = NULL,
                lease_owner = NULL, lease_expires_at = NULL WHERE effect_id = %s AND status = 'leased'
                AND lease_owner = %s AND lease_expires_at > %s""", [effect_id, owner, datetime.now(timezone.utc)]).rowcount
            if not changed: raise ValueError("effect lease is not held by this owner")

    def fail_effect_intent(self, effect_id: str, owner: str, error: dict, *, retry_after: timedelta | None = None) -> None:
        status = 'pending' if retry_after is not None else 'dead_letter'; when = datetime.now(timezone.utc) + retry_after if retry_after else None
        with self._pool.connection() as conn, conn.transaction():
            changed = conn.execute("""UPDATE materialization_effect_intents SET status = %s, next_attempt_at = %s,
                error_json = %s, lease_owner = NULL, lease_expires_at = NULL WHERE effect_id = %s
                AND status = 'leased' AND lease_owner = %s""", [status, when, json.dumps(error), effect_id, owner]).rowcount
            if not changed: raise ValueError("effect lease is not held by this owner")

    def effect_intent(self, effect_id: str) -> EffectIntent:
        with self._pool.connection() as conn:
            row = conn.execute("""SELECT effect_id, execution_id, kind, destination, payload_json, idempotency_key,
                status, attempts, next_attempt_at, error_json, lease_owner, lease_expires_at
                FROM materialization_effect_intents WHERE effect_id = %s""", [effect_id]).fetchone()
        if row is None: raise KeyError(effect_id)
        parse = lambda value: json.loads(value) if isinstance(value, str) else value
        return EffectIntent(row[0], row[1], row[2], row[3], parse(row[4]), row[5], row[6], row[7], row[8],
                            parse(row[9]) if row[9] else None, row[10], row[11])

    def register_service(self, name: str, definition_id: str):
        if not name: raise ValueError("service name is required")
        with self._pool.connection() as conn, conn.transaction():
            definition = conn.execute("SELECT kind FROM materialization_definitions WHERE definition_id = %s", [definition_id]).fetchone()
            if definition is None: raise KeyError(definition_id)
            if definition[0] != "service": raise ValueError("definition is not a service")
            conn.execute("""INSERT INTO materialization_services (name, definition_id, status, health, updated_at)
                VALUES (%s, %s, 'registered', 'unknown', %s) ON CONFLICT (name) DO UPDATE SET definition_id = excluded.definition_id""",
                [name, definition_id, datetime.now(timezone.utc)])
        return self.service(name)

    def set_service_status(self, name: str, status: str, health: str = 'healthy'):
        if status not in {"registered", "running", "stopped", "failed"}: raise ValueError("invalid service status")
        with self._pool.connection() as conn, conn.transaction():
            if conn.execute("UPDATE materialization_services SET status = %s, health = %s, updated_at = %s WHERE name = %s", [status, health, datetime.now(timezone.utc), name]).rowcount == 0: raise KeyError(name)
        return self.service(name)

    def service(self, name: str):
        from acquirium.Materialization.services import ServiceRecord
        with self._pool.connection() as conn:
            row = conn.execute("SELECT name, definition_id, status, health, updated_at FROM materialization_services WHERE name = %s", [name]).fetchone()
        if row is None: raise KeyError(name)
        return ServiceRecord(*row)

    def services(self, *, status: str | None = None):
        """Return every service record in one query (no per-row round trips)."""
        from acquirium.Materialization.services import ServiceRecord
        with self._pool.connection() as conn:
            rows = conn.execute("""SELECT name, definition_id, status, health, updated_at
                FROM materialization_services WHERE (%s::text IS NULL OR status = %s) ORDER BY name""", [status, status]).fetchall()
        return tuple(ServiceRecord(name, definition_id, service_status, health, updated_at)
                     for name, definition_id, service_status, health, updated_at in rows)

    def services_needing_hint(self, data_versions: dict[str, int], graph_revision: int | None):
        with self._pool.connection() as conn:
            rows = conn.execute("SELECT name, last_data_versions_json, last_graph_revision FROM materialization_services WHERE status = 'running'").fetchall()
        return tuple(name for name, versions, graph in rows
                     if (json.loads(versions) if isinstance(versions, str) else versions) != data_versions or graph != graph_revision)

    def coalesce_service_hint(self, hint: ChangeHint) -> None:
        with self._pool.connection() as conn, conn.transaction():
            if conn.execute("SELECT 1 FROM materialization_services WHERE name = %s", [hint.service_name]).fetchone() is None: raise KeyError(hint.service_name)
            existing = conn.execute("SELECT data_versions_json, graph_revision FROM materialization_service_hints WHERE service_name = %s FOR UPDATE", [hint.service_name]).fetchone()
            versions = dict(hint.data_versions)
            graph_revision = hint.graph_revision
            if existing is not None:
                prior = json.loads(existing[0]) if isinstance(existing[0], str) else existing[0]
                versions = {key: max(int(prior.get(key, value)), int(value)) for key, value in ({**prior, **versions}).items()}
                graph_revision = max(item for item in (existing[1], graph_revision) if item is not None) if existing[1] is not None or graph_revision is not None else None
            conn.execute("INSERT INTO materialization_service_hints VALUES (%s, %s, %s, %s, %s) ON CONFLICT (service_name) DO UPDATE SET token = excluded.token, data_versions_json = excluded.data_versions_json, graph_revision = excluded.graph_revision, created_at = excluded.created_at", [hint.service_name, hint.token, json.dumps(versions), graph_revision, hint.created_at])

    def next_service_hint(self, name: str) -> ChangeHint | None:
        with self._pool.connection() as conn: row = conn.execute("SELECT token, data_versions_json, graph_revision, created_at FROM materialization_service_hints WHERE service_name = %s", [name]).fetchone()
        return ChangeHint(name, row[0], json.loads(row[1]) if isinstance(row[1], str) else row[1], row[2], row[3]) if row else None

    def acknowledge_service_hint(self, name: str, token: str) -> None:
        with self._pool.connection() as conn, conn.transaction():
            hint = conn.execute("SELECT data_versions_json, graph_revision FROM materialization_service_hints WHERE service_name = %s AND token = %s", [name, token]).fetchone()
            if hint is None: return
            conn.execute("UPDATE materialization_services SET last_data_versions_json = %s, last_graph_revision = %s, updated_at = %s WHERE name = %s", [json.dumps(hint[0]) if not isinstance(hint[0], str) else hint[0], hint[1], datetime.now(timezone.utc), name])
            conn.execute("DELETE FROM materialization_service_hints WHERE service_name = %s AND token = %s", [name, token])
