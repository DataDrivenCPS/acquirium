"""The durable inbox and the step that moves it into the timeseries store.

Ingestion no longer writes readings into ``timeseries`` directly. Each request
is appended to ``materialization_inbox`` in its own short transaction and is
acknowledged when that append commits. A step then drains the inbox in
sequence order and applies every entry inside one write transaction with one
revision, recording the step in ``materialization_steps``. Everything a step
commits becomes visible together; nothing from an unfinished step is visible
at all, and a crash mid-step leaves the inbox intact for the next attempt.

An entry is a canonical mutations table (see ``Storage.publication.types``)
serialized as an Arrow IPC stream, plus its kind: ``readings`` upserts rows,
``replace`` physically replaces one stream and records its reset marker.

``Engine.deferred`` says who runs steps. A server sets it and steps from its
materialization loop; an embedder that leaves it unset gets a step on every
submission, so a read after a write sees the write.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import polars as pl
import pyarrow as pa

KINDS = ("readings", "replace")


@dataclass(frozen=True)
class StepReport:
    step_no: int
    revision: int
    seq_from: int
    seq_to: int
    entries: int
    rows: int
    seconds: float


def encode(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def decode(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()


class Engine:
    """Own the inbox and run steps against one timeseries backend."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self.deferred = False
        with store._lock, store._write_conn() as conn:
            if self._postgres:
                self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_inbox (
                    seq BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL, meta TEXT NOT NULL,
                    payload BYTEA NOT NULL, received_at TIMESTAMPTZ NOT NULL)""")
            else:
                self._execute(conn, "CREATE SEQUENCE IF NOT EXISTS materialization_inbox_seq")
                self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_inbox (
                    seq BIGINT PRIMARY KEY DEFAULT nextval('materialization_inbox_seq'),
                    kind VARCHAR NOT NULL, meta VARCHAR NOT NULL,
                    payload BLOB NOT NULL, received_at TIMESTAMP NOT NULL)""")
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_steps (
                step_no BIGINT PRIMARY KEY, revision BIGINT NOT NULL,
                seq_from BIGINT NOT NULL, seq_to BIGINT NOT NULL,
                entries BIGINT NOT NULL, rows_in BIGINT NOT NULL,
                committed_at VARCHAR NOT NULL)""")

    @property
    def _postgres(self) -> bool:
        return getattr(self.store, "materialization_backend", None) == "postgres"

    def _execute(self, conn: Any, query: str, params: Iterable[Any] = ()) -> Any:
        if self._postgres:
            query = query.replace("?", "%s")
        return conn.execute(query, list(params))

    # ---- inbox ----

    def submit(self, kind: str, mutations: pa.Table, **meta: Any) -> int:
        """Append one entry and return its sequence number.

        The append is its own transaction: once it returns, the entry will be
        applied by a later step even if this process dies first. Unless the
        engine is deferred, that step runs now.
        """
        if kind not in KINDS:
            raise ValueError(f"unknown inbox kind {kind!r}")
        now = datetime.now(timezone.utc)
        received = now if self._postgres else now.replace(tzinfo=None)
        with self.store._lock, self.store._write_conn() as conn:
            row = self._execute(
                conn, "INSERT INTO materialization_inbox (kind, meta, payload, received_at) "
                      "VALUES (?, ?, ?, ?) RETURNING seq",
                [kind, json.dumps(meta, sort_keys=True), encode(mutations), received]).fetchone()
        seq = int(row[0])
        if not self.deferred:
            self.step()
        return seq

    def pending(self) -> int:
        with self.store._own_conn() as conn:
            return int(self._execute(conn, "SELECT count(*) FROM materialization_inbox").fetchone()[0])

    def last_step(self) -> int:
        with self.store._own_conn() as conn:
            row = self._execute(conn, "SELECT max(step_no) FROM materialization_steps").fetchone()
        return int(row[0] or 0)

    # ---- step ----

    def step(self, limit: int | None = None) -> StepReport | None:
        """Apply the oldest ``limit`` inbox entries (all when None) in one transaction.

        Returns None when the inbox is empty. Entries are applied in sequence
        order, so a replacement after a reading supersedes it exactly as it
        would have had both been applied as they arrived.
        """
        from time import perf_counter
        started = perf_counter()
        with self.store._lock, self.store._write_conn() as conn:
            query = "SELECT seq, kind, meta, payload FROM materialization_inbox ORDER BY seq"
            params: list[Any] = []
            if limit is not None:
                query += " LIMIT ?"
                params.append(int(limit))
            entries = self._execute(conn, query, params).fetchall()
            if not entries:
                return None
            revision = self.store._next_revision(conn)
            rows = 0
            for seq, kind, meta, payload in entries:
                table = decode(bytes(payload))
                rows += self._apply(conn, kind, json.loads(meta), table, revision)
            seq_from, seq_to = int(entries[0][0]), int(entries[-1][0])
            self._execute(conn, "DELETE FROM materialization_inbox WHERE seq>=? AND seq<=?", [seq_from, seq_to])
            step_no = int(self._execute(conn, "SELECT coalesce(max(step_no), 0) + 1 FROM materialization_steps").fetchone()[0])
            self._execute(conn, "INSERT INTO materialization_steps VALUES (?, ?, ?, ?, ?, ?, ?)",
                          [step_no, revision, seq_from, seq_to, len(entries), rows,
                           datetime.now(timezone.utc).isoformat()])
        return StepReport(step_no, revision, seq_from, seq_to, len(entries), rows, perf_counter() - started)

    def _apply(self, conn: Any, kind: str, meta: dict[str, Any], table: pa.Table, revision: int) -> int:
        frame = self.store._prepare_frame(pl.from_arrow(table))
        if kind == "readings":
            if frame.height:
                self.store._insert_frame(conn, frame, revision)
            return frame.height
        ref_uri = meta["ref_uri"]
        self._execute(conn, "DELETE FROM timeseries WHERE ref_id = (SELECT ref_id FROM ref_ids WHERE ref_uri = ?)", [ref_uri])
        if frame.height:
            self.store._insert_frame(conn, frame, revision)
        self._execute(conn, "INSERT INTO stream_resets (ref_uri, last_revision) VALUES (?, ?) "
                            "ON CONFLICT (ref_uri) DO UPDATE SET last_revision=EXCLUDED.last_revision",
                      [ref_uri, revision])
        return frame.height
