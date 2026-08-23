"""PostgreSQL implementation of the topology-epoch control plane.

The state-machine methods are shared with DuckDB.  Only connection syntax and
the canonical stream's URI-keyed reads differ; both backends therefore execute
the same trace and expose the same durable transitions.
"""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from typing import Sequence

import pyarrow as pa

from acquirium.Materialization.epochs import EpochClaim, EpochSnapshot, table_from_rows
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.materialization.ids import materialization_id


class _PostgresConnection:
    """Tiny placeholder adapter for the shared SQL state-machine methods."""

    def __init__(self, connection) -> None:
        self._connection = connection

    @staticmethod
    def _sql(sql: str) -> str:
        # The epoch SQL intentionally uses only positional placeholders.  Do
        # not touch PostgreSQL's native %s form used by the publication code.
        return sql.replace("?", "%s")

    def execute(self, sql: str, params=None):
        return self._connection.execute(self._sql(sql), params or [])

    def executemany(self, sql: str, params):
        with self._connection.cursor() as cursor:
            return cursor.executemany(self._sql(sql), params)

    def transaction(self):
        return self._connection.transaction()

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _PostgresStoreAdapter:
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        from psycopg_pool import ConnectionPool
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)
        self._lock = nullcontext()

    @contextmanager
    def _own_conn(self):
        with self._pool.connection() as connection:
            yield _PostgresConnection(connection)

    @contextmanager
    def _write_conn(self):
        with self._pool.connection() as connection, connection.transaction():
            yield _PostgresConnection(connection)

    def close(self) -> None:
        self._pool.close()


class TopologyEpochPostgres(TopologyEpochDuckDB):
    """The PostgreSQL counterpart of :class:`TopologyEpochDuckDB`."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10, state_revision_resolver=None,
                 query_resolver=None, transition_hook=None) -> None:
        self._store = _PostgresStoreAdapter(dsn, min_size=min_size, max_size=max_size)
        self._state_revision_resolver = state_revision_resolver
        self._query_resolver = query_resolver
        self._transition_hook = transition_hook
        with self._store._write_conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS stream_change_ranges (
                ref_uri TEXT NOT NULL, stream_version BIGINT NOT NULL, publication_id TEXT NOT NULL,
                start_ts TIMESTAMPTZ NOT NULL, end_ts TIMESTAMPTZ NOT NULL, change_kind TEXT NOT NULL,
                row_count BIGINT NOT NULL, PRIMARY KEY (ref_uri, stream_version, start_ts, end_ts))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_definitions (
                definition_id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                source_digest TEXT NOT NULL, entrypoint TEXT NOT NULL, spec_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_deployments (
                name TEXT PRIMARY KEY, definition_id TEXT NOT NULL,
                generation BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epochs (
                epoch_id TEXT PRIMARY KEY, graph_revision BIGINT NOT NULL, graph_digest TEXT NOT NULL,
                catalog_digest TEXT NOT NULL, status TEXT NOT NULL, superseded_by TEXT,
                created_at TIMESTAMPTZ NOT NULL, activated_at TIMESTAMPTZ, compacted_at TIMESTAMPTZ)""")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS topology_epochs_revision_catalog ON topology_epochs (graph_revision, catalog_digest)")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_binding_pins (
                epoch_id TEXT NOT NULL, binding_id TEXT NOT NULL, state_revision TEXT,
                policy TEXT, effective_from TIMESTAMPTZ,
                PRIMARY KEY (epoch_id, binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_bindings (
                epoch_id TEXT NOT NULL, binding_id TEXT NOT NULL, definition_id TEXT NOT NULL,
                logical_key TEXT NOT NULL, content_digest TEXT NOT NULL, inputs_json JSONB NOT NULL,
                outputs_json JSONB NOT NULL, metadata_json JSONB NOT NULL, state_revision TEXT,
                PRIMARY KEY (epoch_id, binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_edges (
                epoch_id TEXT NOT NULL, source_binding_id TEXT NOT NULL, target_binding_id TEXT NOT NULL,
                PRIMARY KEY (epoch_id, source_binding_id, target_binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_components (
                epoch_id TEXT NOT NULL, component_id TEXT NOT NULL, binding_ids_json JSONB NOT NULL,
                status TEXT NOT NULL, frontier BIGINT NOT NULL, sealed_frontier BIGINT NOT NULL,
                seal_publication_id TEXT,
                PRIMARY KEY (epoch_id, component_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_binding_frontiers (
                epoch_id TEXT NOT NULL, binding_id TEXT NOT NULL, input_versions_json JSONB NOT NULL,
                PRIMARY KEY (epoch_id, binding_id))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_work (
                work_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL, component_id TEXT NOT NULL,
                binding_id TEXT NOT NULL, frontier BIGINT NOT NULL,
                write_start_ts TIMESTAMPTZ NOT NULL, write_end_ts TIMESTAMPTZ NOT NULL,
                read_start_ts TIMESTAMPTZ NOT NULL, read_end_ts TIMESTAMPTZ NOT NULL,
                input_versions_json JSONB NOT NULL, upstream_frontier_json JSONB NOT NULL,
                binding_digest TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TIMESTAMPTZ, error_json JSONB, output_digest TEXT, committed_at TIMESTAMPTZ)""")
            conn.execute("CREATE INDEX IF NOT EXISTS topology_epoch_work_pending ON topology_epoch_work (epoch_id, status, write_start_ts, work_id)")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_outputs (
                epoch_id TEXT NOT NULL, work_id TEXT NOT NULL, ref_uri TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL, numeric_value DOUBLE PRECISION, text_value TEXT,
                PRIMARY KEY (epoch_id, work_id, ref_uri, ts))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_retirements (
                epoch_id TEXT NOT NULL, ref_uri TEXT NOT NULL,
                PRIMARY KEY (epoch_id, ref_uri))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_claims (
                claim_id TEXT PRIMARY KEY, kind TEXT NOT NULL, target_id TEXT NOT NULL UNIQUE,
                owner TEXT, attempt INTEGER NOT NULL DEFAULT 0, expires_at TIMESTAMPTZ)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS topology_epoch_control (
                control_id INTEGER PRIMARY KEY, candidate_epoch_id TEXT,
                current_epoch_id TEXT, active_epoch_id TEXT,
                updated_at TIMESTAMPTZ NOT NULL)""")
            conn.execute("""INSERT INTO topology_epoch_control (control_id, updated_at)
                VALUES (1, now()) ON CONFLICT (control_id) DO NOTHING""")

    def close(self) -> None:
        self._store.close()

    @staticmethod
    def _lock_deployments(conn) -> None:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('acquirium-deployments', 0))")

    @staticmethod
    def _lock_component(conn, epoch: str, component: str) -> None:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", [f"{epoch}:{component}"])

    def claim(self, kind: str, target_id: str, owner: str, *,
              duration: timedelta = timedelta(minutes=5)) -> EpochClaim | None:
        """Acquire a target claim atomically across PostgreSQL managers."""
        if not owner or duration <= timedelta():
            raise ValueError("claim owner and positive duration are required")
        now = self._now()
        expires = now + duration
        claim_id = materialization_id("topology-claim", kind, target_id)
        with self._store._write_conn() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", [target_id])
            row = conn.execute("""SELECT owner, attempt, expires_at FROM topology_epoch_claims
                WHERE target_id = ?""", [target_id]).fetchone()
            if row is not None and row[0] is not None and row[2] > now:
                return None
            attempt = int(row[1]) + 1 if row else 1
            conn.execute("""INSERT INTO topology_epoch_claims
                (claim_id, kind, target_id, owner, attempt, expires_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (target_id) DO UPDATE SET
                kind = excluded.kind, owner = excluded.owner,
                attempt = excluded.attempt, expires_at = excluded.expires_at""",
                [claim_id, kind, target_id, owner, attempt, expires])
        self._after_transition("claim_acquired")
        return EpochClaim(claim_id, kind, target_id, owner, attempt, expires)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _stored_timestamp(value: datetime) -> datetime:
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    def _json_value(self, value):
        # JSONB parameters accept the canonical JSON strings used by the
        # shared implementation; psycopg decodes them on reads.
        return value

    def _catalog(self, conn):
        rows = conn.execute("""SELECT definition_id, name, source_digest, entrypoint, spec_json
            FROM topology_epoch_definitions WHERE kind = 'transformation' ORDER BY definition_id""").fetchall()
        return tuple((row[0], row[1], row[2], row[3], row[4] if isinstance(row[4], str) else self._json(row[4])) for row in rows)

    def _definition(self, conn, definition_id):
        definition = super()._definition(conn, definition_id)
        if not isinstance(definition.spec, dict):
            from acquirium.Materialization.epochs import EpochDefinition
            definition = EpochDefinition(definition.definition_id, definition.name, definition.source_digest,
                                         definition.entrypoint, definition.kind, dict(definition.spec))
        return definition

    def _retained_ranges(self, conn, refs: Sequence[str]):
        if not refs:
            return ()
        row = conn.execute("""SELECT min(ts), max(ts) FROM timeseries
            WHERE ref_uri = ANY(%s::text[]) AND NOT deleted""", [list(refs)]).fetchone()
        if row[0] is None:
            return ()
        from acquirium.Materialization.impact import TimeRange
        return (TimeRange(self._aware(row[0]), self._aware(row[1]) + timedelta(microseconds=1)),)

    def _stream_versions(self, conn, refs: Sequence[str]):
        if not refs:
            return {}
        values = dict(conn.execute("SELECT ref_uri, current_version FROM stream_heads WHERE ref_uri = ANY(%s::text[]) ORDER BY ref_uri", [list(refs)]).fetchall())
        return {ref: int(values.get(ref, 0)) for ref in refs}

    def snapshot(self, claim: EpochClaim) -> EpochSnapshot:
        if claim.kind != "reconcile":
            from acquirium.Materialization.epochs import EpochClaimError
            raise EpochClaimError("claim is not a reconcile claim")
        with self._store._own_conn() as conn:
            self._require_claim(conn, claim)
            work = self._work(conn, claim.target_id)
            if work.status != "claimed" or work.attempt != claim.attempt:
                from acquirium.Materialization.epochs import EpochClaimError
                raise EpochClaimError("work attempt is stale")
            row = conn.execute("""SELECT epoch_id, binding_id, definition_id, logical_key, content_digest,
                inputs_json, outputs_json, metadata_json, state_revision FROM topology_epoch_bindings
                WHERE epoch_id = %s AND binding_id = %s""", [work.epoch_id, work.binding_id]).fetchone()
            from acquirium.Materialization.epochs import EpochBinding
            binding = EpochBinding(row[0], row[1], row[2], row[3], row[4], self._obj(row[5]), self._obj(row[6]), self._obj(row[7]), row[8])
            definition = self._definition(conn, binding.definition_id)
            owners = {
                ref: binding_id
                for binding_id, outputs_json in conn.execute(
                    "SELECT binding_id, outputs_json FROM topology_epoch_bindings WHERE epoch_id = %s",
                    [work.epoch_id],
                ).fetchall()
                for refs in self._obj(outputs_json).values()
                for ref in refs
            }
            active_epoch = conn.execute(
                "SELECT active_epoch_id FROM topology_epoch_control WHERE control_id = 1"
            ).fetchone()[0]
            rows = []
            for ref in binding.input_refs:
                if ref in owners:
                    dependency_ids = tuple(work.upstream_frontier.get(owners[ref], ()))
                    staged_rows = []
                    replaced = []
                    if dependency_ids:
                        staged_rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM (
                            SELECT o.ref_uri, o.ts, o.numeric_value, o.text_value,
                                   row_number() OVER (PARTITION BY o.ref_uri, o.ts ORDER BY w.committed_at DESC, o.work_id DESC) AS recency
                            FROM topology_epoch_outputs o JOIN topology_epoch_work w ON w.work_id = o.work_id
                            WHERE o.epoch_id = %s AND o.ref_uri = %s AND o.work_id = ANY(%s::text[])
                              AND w.status = 'committed' AND o.ts >= %s AND o.ts < %s) latest
                            WHERE recency = 1 ORDER BY ts""", [
                                work.epoch_id, ref, list(dependency_ids),
                                work.read_interval.start, work.read_interval.end,
                            ]).fetchall()
                        interval_rows = conn.execute("""SELECT write_start_ts, write_end_ts
                            FROM topology_epoch_work WHERE work_id = ANY(%s::text[])""",
                            [list(dependency_ids)]).fetchall()
                        replaced = [TimeRange(self._aware(start), self._aware(end)) for start, end in interval_rows]
                    baseline_rows = []
                    if active_epoch == work.epoch_id:
                        baseline_rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value
                            FROM timeseries WHERE ref_uri = %s AND ts >= %s AND ts < %s AND NOT deleted
                            ORDER BY ts""", [ref, work.read_interval.start, work.read_interval.end]).fetchall()
                        baseline_rows = [row for row in baseline_rows if not any(
                            interval.start <= self._aware(row[1]) < interval.end for interval in replaced
                        )]
                    by_timestamp = {row[1]: row for row in baseline_rows}
                    by_timestamp.update({row[1]: row for row in staged_rows})
                    values = [by_timestamp[ts] for ts in sorted(by_timestamp)]
                else:
                    values = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM timeseries
                        WHERE ref_uri = %s AND ts >= %s AND ts < %s AND NOT deleted ORDER BY ts""",
                                          [ref, work.read_interval.start, work.read_interval.end]).fetchall()
                rows.extend({"operation": "upsert", "ref_uri": ref, "ts": self._aware(ts), "numeric_value": numeric, "text_value": text}
                            for _, ts, numeric, text in values)
            rows.sort(key=lambda item: (item["ref_uri"], item["ts"]))
            return EpochSnapshot(work, binding, definition, table_from_rows(rows), dict(work.input_versions))

    @staticmethod
    def _obj(value):
        import json
        return json.loads(value) if isinstance(value, str) else value

    def _canonical_rows(self, conn, refs: Sequence[str], start: datetime, end: datetime):
        if not refs:
            return []
        return conn.execute("""SELECT ref_uri, ts FROM timeseries
            WHERE ref_uri = ANY(%s::text[]) AND ts >= %s AND ts < %s AND NOT deleted""", [list(refs), start, end]).fetchall()

    def _all_canonical_rows(self, conn, refs: Sequence[str]):
        if not refs:
            return []
        return conn.execute(
            "SELECT ref_uri, ts FROM timeseries WHERE ref_uri = ANY(%s::text[]) AND NOT deleted",
            [list(refs)],
        ).fetchall()

    def _apply_canonical_publication(self, conn, publication_id: str, mutations: pa.Table):
        from acquirium.Storage.publication.postgres import PublicationPostgres
        return PublicationPostgres.__new__(PublicationPostgres)._apply_publication(conn, publication_id, mutations)
