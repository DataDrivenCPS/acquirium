"""PostgreSQL implementation of the topology-epoch control plane.

The state-machine methods are shared with DuckDB through the connection
adapter.  Only advisory locking, multi-manager claim acquisition, and the
reads against PostgreSQL's URI-keyed canonical tables differ; both backends
therefore execute the same trace and expose the same durable transitions.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Sequence

import pyarrow as pa

from acquirium.Materialization.epochs import EpochClaim
from acquirium.Materialization.impact import TimeRange
from acquirium.Storage.materialization.dialect import PostgresCodecs, PostgresStoreAdapter
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.materialization.ids import materialization_id
from acquirium.Storage.materialization.schema import change_range_statements, epoch_statements


class TopologyEpochPostgres(PostgresCodecs, TopologyEpochDuckDB):
    """The PostgreSQL counterpart of :class:`TopologyEpochDuckDB`."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10, state_revision_resolver=None,
                 query_resolver=None, transition_hook=None) -> None:
        super().__init__(
            PostgresStoreAdapter(dsn, min_size=min_size, max_size=max_size),
            state_revision_resolver=state_revision_resolver,
            query_resolver=query_resolver,
            transition_hook=transition_hook,
        )

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

    def _catalog(self, conn):
        rows = conn.execute("""SELECT definition_id, name, source_digest, entrypoint, spec_json
            FROM materialization_definitions WHERE kind = 'transformation' ORDER BY definition_id""").fetchall()
        return tuple((row[0], row[1], row[2], row[3], row[4] if isinstance(row[4], str) else self._json(row[4])) for row in rows)

    def _retained_ranges(self, conn, refs: Sequence[str], *, include_deleted: bool = False):
        if not refs:
            return ()
        live_filter = "" if include_deleted else " AND NOT deleted"
        row = conn.execute("""SELECT min(ts), max(ts) FROM timeseries
            WHERE ref_uri = ANY(%s::text[])""" + live_filter, [list(refs)]).fetchone()
        if row[0] is None:
            return ()
        return (TimeRange(self._aware(row[0]), self._aware(row[1]) + timedelta(microseconds=1)),)

    def _stream_versions(self, conn, refs: Sequence[str]):
        if not refs:
            return {}
        values = dict(conn.execute("SELECT ref_uri, current_version FROM stream_heads WHERE ref_uri = ANY(%s::text[]) ORDER BY ref_uri", [list(refs)]).fetchall())
        return {ref: int(values.get(ref, 0)) for ref in refs}

    def _live_rows(self, conn, ref: str, interval: TimeRange) -> list[tuple]:
        return conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM timeseries
            WHERE ref_uri = ? AND ts >= ? AND ts < ? AND NOT deleted ORDER BY ts""",
            [ref, interval.start, interval.end]).fetchall()

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
