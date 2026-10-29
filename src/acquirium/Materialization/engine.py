"""The durable inbox and the step that moves it into the timeseries store.

Ingestion does not write readings into ``timeseries`` directly. Each request
is appended to ``materialization_inbox`` in its own short transaction and is
acknowledged when that append commits. A step then drains the inbox in
sequence order and applies every entry inside one write transaction with one
revision, recording the step in ``materialization_steps``. Everything a step
commits becomes visible together; nothing from an unfinished step is visible
at all, and a crash mid-step leaves the inbox intact for the next attempt.

An entry is a canonical mutations table (see ``Storage.publication.types``)
serialized as an Arrow IPC stream, plus its kind: ``readings`` upserts rows,
``replace`` physically replaces one stream and records its reset marker.

After the inbox, a step evaluates the views on the same connection through
its ``evaluator`` (``evaluation.ViewEvaluator``): the plan is prepared outside
the transaction and evaluated inside it, so derived rows commit with the raw
rows that caused them.

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
        insert = ("INSERT INTO materialization_inbox (kind, meta, payload, received_at) "
                  "VALUES (?, ?, ?, ?) RETURNING seq")
        params = [kind, json.dumps(meta, sort_keys=True), encode(mutations), received]
        if self._postgres:
            # The step holds the store's lock and write connection for its
            # whole transaction. An append on a connection of its own is
            # acknowledged at once instead of after every transform of the
            # step; the server allows concurrent writers, and the step's
            # DELETE of applied rows never touches rows appended after it
            # read the inbox.
            with self.store._own_conn() as conn, conn.transaction():
                row = self._execute(conn, insert, params).fetchone()
        else:
            with self.store._lock, self.store._write_conn() as conn:
                row = self._execute(conn, insert, params).fetchone()
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
                # Consecutive readings go in as one upsert, in sequence order,
                # so the last row for a (stream, time) still wins.
                readings: list[pa.Table] = []
                for seq, kind, meta, payload in entries:
                    table = decode(bytes(payload))
                    if kind == "readings":
                        readings.append(table)
                        continue
                    rows += self._apply_readings(conn, readings, revision)
                    readings = []
                    rows += self._apply(conn, kind, json.loads(meta), table, revision)
                rows += self._apply_readings(conn, readings, revision)
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

    def _apply_readings(self, conn: Any, tables: list[pa.Table], revision: int) -> int:
        if not tables:
            return 0
        table = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="permissive")
        return self._apply(conn, "readings", {}, table, revision)

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


class InboxPublisher:
    """Hand normalized upserts and stream replacements to the engine's inbox.

    The receipt's ``versions`` carries the inbox sequence number the request
    was recorded under. Rows reach ``timeseries`` when a step applies that
    entry; with a non-deferred engine that has happened by the time this
    returns. ``publication_id`` labels the receipt; it does not deduplicate
    retries.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def publish(self, request):
        from acquirium.Storage.publication import ids
        from acquirium.Storage.publication.types import PublicationReceipt
        mutations = ids.normalize_mutations(request.mutations)
        if not mutations.num_rows:
            return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), 0, {})
        frame = pl.from_arrow(mutations)
        if (frame["operation"] != "upsert").any():
            raise ValueError("only upserts go through the inbox; delete with delete_timeseries")
        seq = self._engine.submit("readings", mutations, publication_id=request.publication_id)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), mutations.num_rows, {"seq": seq})

    def replace(self, request, ref_uri: str):
        """Replace one stream, including with nothing, at one step's revision."""
        from acquirium.Storage.publication import ids
        from acquirium.Storage.publication.types import PublicationReceipt
        frame = pl.from_arrow(request.mutations)
        if (
            frame["operation"].is_null() | (frame["operation"] != "upsert")
            | frame["ref_uri"].is_null() | (frame["ref_uri"] != ref_uri)
        ).any():
            raise ValueError("replacement requires only upserts for the specified stream")
        if frame["ts"].null_count():
            raise ValueError("replacement timestamps must not be null")
        if (frame["numeric_value"].is_not_null() & frame["text_value"].is_not_null()).any():
            raise ValueError("replacement rows cannot contain both numeric and text values")
        mutations = ids.normalize_mutations(request.mutations)
        seq = self._engine.submit("replace", mutations, ref_uri=ref_uri, publication_id=request.publication_id)
        return PublicationReceipt(request.publication_id, ids.payload_hash(mutations), mutations.num_rows, {"seq": seq})
