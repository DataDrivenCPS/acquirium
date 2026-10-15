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

After the inbox, a step evaluates the materialization plan on the same
connection through its ``evaluator``: the plan is prepared outside the
transaction (that may compile queries and write lineage) and evaluated
inside it, so derived rows commit with the raw rows that caused them.

``Engine.deferred`` says who runs steps. A server sets it and steps from its
materialization loop; an embedder that leaves it unset gets a step on every
submission, so a read after a write sees the write.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping, Any, Iterable, Protocol

import polars as pl
import pyarrow as pa

KINDS = ("readings", "replace")


class Evaluator(Protocol):
    """What a step runs after applying the inbox."""
    def prepare(self) -> Any:
        """Build whatever evaluation needs, outside the step's transaction; None means nothing to run."""
    def evaluate(self, conn: Any, plan: Any) -> bool:
        """Run the plan on the step's connection; True when something published."""
    def committed(self, report: "StepReport") -> None:
        """The step's transaction committed; publish what it changed."""


@dataclass(frozen=True)
class StepReport:
    step_no: int
    revision: int
    seq_from: int
    seq_to: int
    entries: int
    rows: int
    published: bool
    seconds: float
    # Where the time went: preparing the plan (a refresh when the graph
    # changed), applying the inbox, evaluating the plan (its reads,
    # transforms and publication, from the evaluator), and the commit.
    timings: Mapping[str, float] = field(default_factory=dict)


def encode(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def decode(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.BufferReader(payload)).read_all()


class Engine:
    """Own the inbox and run steps against one timeseries backend."""

    def __init__(self, store: Any, evaluator: Evaluator | None = None) -> None:
        self.store = store
        self.evaluator = evaluator
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
                published BOOLEAN NOT NULL, committed_at VARCHAR NOT NULL)""")

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
        """Apply the oldest ``limit`` inbox entries (all when None), then evaluate, in one transaction.

        Returns None when the inbox was empty and nothing published. Entries
        are applied in sequence order, so a replacement after a reading
        supersedes it exactly as it would have had both been applied as they
        arrived. The evaluator then runs on the same connection, so derived
        rows for this step's readings commit with them.
        """
        from time import perf_counter
        started = perf_counter()
        plan = self.evaluator.prepare() if self.evaluator is not None else None
        timings: dict[str, float] = {"prepare": perf_counter() - started}
        applying = perf_counter()
        with self.store._lock, self.store._write_conn() as conn:
            query = "SELECT seq, kind, meta, payload FROM materialization_inbox ORDER BY seq"
            params: list[Any] = []
            if limit is not None:
                query += " LIMIT ?"
                params.append(int(limit))
            entries = self._execute(conn, query, params).fetchall()
            rows, seq_from, seq_to, revision = 0, 0, 0, None
            if entries:
                revision = self.store._next_revision(conn)
                for seq, kind, meta, payload in entries:
                    table = decode(bytes(payload))
                    rows += self._apply(conn, kind, json.loads(meta), table, revision)
                seq_from, seq_to = int(entries[0][0]), int(entries[-1][0])
                self._execute(conn, "DELETE FROM materialization_inbox WHERE seq>=? AND seq<=?", [seq_from, seq_to])
            timings["apply"] = perf_counter() - applying
            evaluated = perf_counter()
            published = bool(self.evaluator.evaluate(conn, plan)) if plan is not None else False
            timings["evaluate"] = perf_counter() - evaluated
            timings.update(getattr(self.evaluator, "timings", None) or {})
            if not entries and not published:
                return None
            if revision is None:
                revision = int(self._execute(conn, "SELECT current_revision FROM system_state").fetchone()[0])
            step_no = int(self._execute(conn, "SELECT coalesce(max(step_no), 0) + 1 FROM materialization_steps").fetchone()[0])
            self._execute(conn, "INSERT INTO materialization_steps VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                          [step_no, revision, seq_from, seq_to, len(entries), rows, published,
                           datetime.now(timezone.utc).isoformat()])
            committing = perf_counter()
        timings["commit"] = perf_counter() - committing
        report = StepReport(step_no, revision, seq_from, seq_to, len(entries), rows, published, perf_counter() - started, timings)
        if self.evaluator is not None and plan is not None:
            self.evaluator.committed(report)
        return report

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
