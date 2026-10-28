"""Bitemporal history of the versioned model graphs.

The model is the plant graph and every source graph except the experiment
graphs (see ``graph_registry.is_versioned_graph``). Every write to it is
recorded here before it reaches the live Oxigraph graph. A write is a list of
steps; a step deletes and inserts triples, each over its own effective
interval, and a whole-graph write also deletes every other triple of the
graph over its interval (``Step.clear``).

Three tables, all in the time-series store's database:

- ``model_writes``: one row per write, numbered by its recording time ``r``.
- ``model_events``: the triples each write deleted or inserted, with the
  interval each holds over. The deletions a whole-graph write implies are
  not listed; ``kind = 'replace'`` on its write row stands for them.
- ``model_triples``: where each triple is present (``valid_from`` to
  ``valid_to``) as known from recording time ``recorded_from`` until
  ``recorded_to``. The model at a time, as known at a recording time, is
  one range query. Rows of one triple that hold at the same recording time
  never overlap or touch.

Times are integer microseconds since the epoch in UTC; ``NEG_INF`` and
``POS_INF`` stand for the unbounded ends. Triples are N-Triples lines.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from rdflib import Graph, Literal
from rdflib.namespace import XSD
from rdflib.plugins.serializers.nt import _nt_row

from acquirium.Storage.metadata_outbox import execute
from acquirium.Storage.migrations import Migration, migrate

NEG_INF = -(2**63)
POS_INF = 2**63 - 1

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_micros(value: datetime) -> int:
    """Microseconds since the epoch; a naive datetime is taken as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_micros(value: int) -> datetime | str:
    """The inverse of ``to_micros``; the unbounded ends become strings."""
    if value == NEG_INF:
        return "-infinity"
    if value == POS_INF:
        return "infinity"
    return _EPOCH + timedelta(microseconds=value)


def now_micros() -> int:
    return to_micros(datetime.now(timezone.utc))


def parse_bound(value: datetime | str | None, *, end: bool) -> int | None:
    """An ``effective_from`` (``end=False``) or ``effective_until`` value.

    ``None`` stays ``None`` so the caller can fill in the default. The start
    may be ``"-infinity"`` and the end ``"infinity"``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        if value == ("infinity" if end else "-infinity"):
            return POS_INF if end else NEG_INF
        name = "effective_until" if end else "effective_from"
        allowed = '"infinity"' if end else '"-infinity"'
        raise ValueError(f"{name} must be a datetime or {allowed}, got {value!r}")
    if isinstance(value, datetime):
        return to_micros(value)
    raise TypeError(f"expected a datetime, got {type(value).__name__}")


def nt(triple) -> str:
    """The N-Triples line of an rdflib triple, as stored in the history.

    An ``xsd:string`` literal is written as a plain literal: RDF 1.1 treats
    them as one term, and Oxigraph returns the typed form for both.
    """
    s, p, o = triple
    if isinstance(o, Literal) and o.datatype == XSD.string and o.language is None:
        o = Literal(str(o))
    return _nt_row((s, p, o)).rstrip("\n")


class _SameLabels(dict):
    """Blank-node context that keeps every label as written.

    rdflib's N-Triples parser maps each label to a fresh blank node by
    default; history lines must read back as the same terms.
    """

    def get(self, key, default=None):
        return key


def parse_lines(lines: Iterable[str]) -> Graph:
    graph = Graph()
    data = "\n".join(lines)
    if data:
        graph.parse(data=data, format="nt", bnode_context=_SameLabels())
    return graph


@dataclass(frozen=True)
class Step:
    """Triples deleted (``'-'``) or inserted (``'+'``), each over its own
    interval ``[start, end)``: ``items`` holds ``(line, op, start, end)``.
    ``clear`` also deletes every other triple of the graph over that
    interval. A triple appears at most once in a step."""

    items: tuple[tuple[str, str, int, int], ...]
    clear: tuple[int, int] | None = None


MODEL_HISTORY_MIGRATIONS = (
    Migration(1, "model_history", (
        """CREATE TABLE IF NOT EXISTS model_writes (
            r BIGINT PRIMARY KEY, recorded_at BIGINT NOT NULL, graph_uri VARCHAR NOT NULL,
            kind VARCHAR NOT NULL, dated BOOLEAN NOT NULL,
            effective_from BIGINT NOT NULL, effective_until BIGINT)""",
        """CREATE TABLE IF NOT EXISTS model_events (
            r BIGINT NOT NULL, graph_uri VARCHAR NOT NULL, triple VARCHAR NOT NULL,
            op VARCHAR NOT NULL, valid_from BIGINT NOT NULL, valid_to BIGINT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS model_triples (
            graph_uri VARCHAR NOT NULL, triple VARCHAR NOT NULL,
            valid_from BIGINT NOT NULL, valid_to BIGINT NOT NULL,
            recorded_from BIGINT NOT NULL, recorded_to BIGINT)""",
    )),
)

# DuckDB plans these lookups as scans with zone maps; its ART indexes would
# only add write cost. Postgres needs them for the per-triple joins.
POSTGRES_MODEL_HISTORY_MIGRATIONS = MODEL_HISTORY_MIGRATIONS + (
    Migration(2, "model_history_indexes", (
        "CREATE INDEX IF NOT EXISTS model_triples_triple_idx ON model_triples (graph_uri, triple) WHERE recorded_to IS NULL",
        "CREATE INDEX IF NOT EXISTS model_events_triple_idx ON model_events (graph_uri, triple)",
    )),
)
DUCKDB_MODEL_HISTORY_MIGRATIONS = MODEL_HISTORY_MIGRATIONS + (
    Migration(2, "model_history_indexes", ()),
)

MODEL_HISTORY_TABLES = ("model_writes", "model_events", "model_triples")


class ModelHistory:
    """Record writes to the versioned model graphs and read the model at any
    time, as known at any recording time. Callers serialize writes."""

    def __init__(self, store) -> None:
        self.store = store
        self._postgres = getattr(store, "materialization_backend", None) == "postgres"
        with store._lock, store._write_conn() as conn:
            migrate(
                conn,
                backend="postgres" if self._postgres else "duckdb",
                component="model_history",
                migrations=POSTGRES_MODEL_HISTORY_MIGRATIONS if self._postgres else DUCKDB_MODEL_HISTORY_MIGRATIONS,
            )

    # -------------------- reads --------------------
    def head(self) -> int:
        """The recording time of the last write, 0 before the first."""
        with self.store._own_conn() as conn:
            return int(execute(conn, "SELECT COALESCE(MAX(r), 0) FROM model_writes").fetchone()[0])

    def graphs(self) -> list[str]:
        with self.store._own_conn() as conn:
            return [row[0] for row in execute(conn, "SELECT DISTINCT graph_uri FROM model_writes ORDER BY graph_uri").fetchall()]

    def has_undated_write(self, graph_uri: str) -> bool:
        """Whether some write to *graph_uri* took the default start time."""
        with self.store._own_conn() as conn:
            row = execute(
                conn, "SELECT 1 FROM model_writes WHERE graph_uri = ? AND NOT dated LIMIT 1", [graph_uri]
            ).fetchone()
        return row is not None

    def graph_at(self, graph_uri: str, at: int, *, as_of: int | None = None) -> set[str]:
        """The triples of *graph_uri* at time *at*, as known at recording time
        *as_of* (the latest when omitted)."""
        query = "SELECT triple FROM model_triples WHERE graph_uri = ? AND valid_from <= ? AND valid_to > ? AND "
        params: list = [graph_uri, at, at]
        if as_of is None:
            query += "recorded_to IS NULL"
        else:
            query += "recorded_from <= ? AND (recorded_to IS NULL OR recorded_to > ?)"
            params += [as_of, as_of]
        with self.store._own_conn() as conn:
            return {row[0] for row in execute(conn, query, params).fetchall()}

    def present(self, graph_uri: str, lines: Iterable[str], at: int) -> set[str]:
        """Which of *lines* are in *graph_uri* at time *at*."""
        lines = list(lines)
        if not lines:
            return set()
        with self.store._own_conn() as conn:
            stage = self._stage(conn, [("triple", "VARCHAR")], [(line,) for line in lines])
            try:
                return {row[0] for row in execute(
                    conn,
                    f"SELECT m.triple FROM model_triples m JOIN {stage} s ON s.triple = m.triple "
                    "WHERE m.graph_uri = ? AND m.recorded_to IS NULL AND m.valid_from <= ? AND m.valid_to > ?",
                    [graph_uri, at, at],
                ).fetchall()}
            finally:
                execute(conn, f"DROP TABLE {stage}")

    def versions(self, graph_uri: str, start: int, end: int) -> list[tuple[int, int, frozenset[str]]]:
        """The versions of *graph_uri* inside ``[start, end)``: maximal
        intervals over which its content is constant, with that content."""
        with self.store._own_conn() as conn:
            rows = execute(
                conn,
                "SELECT triple, valid_from, valid_to FROM model_triples "
                "WHERE graph_uri = ? AND recorded_to IS NULL AND valid_from < ? AND valid_to > ?",
                [graph_uri, end, start],
            ).fetchall()
        current: set[str] = set()
        starts: dict[int, list[str]] = defaultdict(list)
        ends: dict[int, list[str]] = defaultdict(list)
        for triple, valid_from, valid_to in rows:
            if valid_from <= start:
                current.add(triple)
            else:
                starts[valid_from].append(triple)
            if valid_to < end:
                ends[valid_to].append(triple)
        pieces = []
        left = start
        for bound in sorted(set(starts) | set(ends)):
            pieces.append((left, bound, frozenset(current)))
            current.difference_update(ends.get(bound, ()))
            current.update(starts.get(bound, ()))
            left = bound
        pieces.append((left, end, frozenset(current)))
        return pieces

    def next_touch(self, graph_uri: str, triples: Iterable[str], after: int) -> dict[str, int]:
        """For each triple, the first start or end after *after* of a recorded
        change that deletes or inserts it, ``POS_INF`` if there is none.

        A whole-graph write touches every triple of its graph.
        """
        triples = list(triples)
        if not triples:
            return {}
        with self.store._own_conn() as conn:
            bound = execute(
                conn,
                "SELECT MIN(b) FROM ("
                "SELECT effective_from AS b FROM model_writes WHERE graph_uri = ? AND kind = 'replace' AND effective_from > ? "
                "UNION ALL "
                "SELECT effective_until AS b FROM model_writes WHERE graph_uri = ? AND kind = 'replace' AND effective_until > ?"
                ") bounds",
                [graph_uri, after, graph_uri, after],
            ).fetchone()[0]
            replace_bound = POS_INF if bound is None else int(bound)
            stage = self._stage(conn, [("triple", "VARCHAR")], [(t,) for t in triples])
            try:
                touched = dict(execute(
                    conn,
                    "SELECT e.triple, MIN(CASE WHEN e.valid_from > ? THEN e.valid_from ELSE e.valid_to END) "
                    f"FROM model_events e JOIN {stage} s ON s.triple = e.triple "
                    "WHERE e.graph_uri = ? AND e.valid_to > ? GROUP BY e.triple",
                    [after, graph_uri, after],
                ).fetchall())
            finally:
                execute(conn, f"DROP TABLE {stage}")
        return {t: min(replace_bound, int(touched.get(t, POS_INF))) for t in triples}

    def changed(self, r: int) -> dict[str, list[tuple[int, int]]]:
        """Where write *r* changed the model: per graph, the merged intervals
        over which some triple's presence differs before and after it."""
        with self.store._own_conn() as conn:
            rows = execute(
                conn,
                "SELECT graph_uri, triple, valid_from, valid_to, recorded_from FROM model_triples "
                "WHERE recorded_from = ? OR recorded_to = ?",
                [r, r],
            ).fetchall()
        before: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
        after: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
        for graph_uri, triple, valid_from, valid_to, recorded_from in rows:
            (after if recorded_from == r else before)[(graph_uri, triple)].append((valid_from, valid_to))
        regions: dict[str, list[tuple[int, int]]] = defaultdict(list)
        for key in before.keys() | after.keys():
            regions[key[0]].extend(symmetric_difference(before.get(key, []), after.get(key, [])))
        return {graph_uri: merge(intervals) for graph_uri, intervals in regions.items() if intervals}

    # -------------------- writes --------------------
    def record(
        self,
        graph_uri: str,
        *,
        kind: str,
        start: int,
        end: int | None,
        dated: bool,
        steps: Sequence[Step],
        recorded_at: int,
    ) -> int:
        """Record one write and return its recording time.

        ``end`` is ``None`` when each triple got its own end. ``dated`` says
        whether the caller gave the start time.
        """
        with self.store._lock, self.store._write_conn() as conn:
            r = int(execute(conn, "SELECT COALESCE(MAX(r), 0) FROM model_writes").fetchone()[0]) + 1
            execute(
                conn,
                "INSERT INTO model_writes (r, recorded_at, graph_uri, kind, dated, effective_from, effective_until) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [r, recorded_at, graph_uri, kind, dated, start, end],
            )
            for step in steps:
                self._apply_step(conn, graph_uri, r, step)
            # Rows a later step of this write replaced were never visible.
            execute(conn, "DELETE FROM model_triples WHERE recorded_from = ? AND recorded_to = ?", [r, r])
        return r

    def _apply_step(self, conn, graph_uri: str, r: int, step: Step) -> None:
        items = self._stage(
            conn,
            [("triple", "VARCHAR"), ("op", "VARCHAR"), ("vf", "BIGINT"), ("vt", "BIGINT")],
            step.items,
        )
        merged = None
        try:
            execute(
                conn,
                "INSERT INTO model_events (r, graph_uri, triple, op, valid_from, valid_to) "
                f"SELECT ?, ?, triple, op, vf, vt FROM {items}",
                [r, graph_uri],
            )
            if step.clear is not None:
                clear_from, clear_to = step.clear
                execute(
                    conn,
                    f"INSERT INTO {items} (triple, op, vf, vt) "
                    "SELECT DISTINCT m.triple, '-', ?, ? FROM model_triples m "
                    "WHERE m.graph_uri = ? AND m.recorded_to IS NULL AND m.valid_from < ? AND m.valid_to > ? "
                    f"AND NOT EXISTS (SELECT 1 FROM {items} s WHERE s.triple = m.triple)",
                    [clear_from, clear_to, graph_uri, clear_to, clear_from],
                )
            # Deletions: keep the parts of each overlapped row outside the
            # deleted interval, then close the row.
            overlap = (
                "s.op = '-' AND m.graph_uri = ? AND m.recorded_to IS NULL AND m.triple = s.triple "
                "AND m.valid_from < s.vt AND m.valid_to > s.vf"
            )
            execute(
                conn,
                "INSERT INTO model_triples (graph_uri, triple, valid_from, valid_to, recorded_from, recorded_to) "
                f"SELECT m.graph_uri, m.triple, m.valid_from, s.vf, ?, CAST(NULL AS BIGINT) FROM model_triples m JOIN {items} s "
                f"ON {overlap} AND m.valid_from < s.vf "
                "UNION ALL "
                f"SELECT m.graph_uri, m.triple, s.vt, m.valid_to, ?, CAST(NULL AS BIGINT) FROM model_triples m JOIN {items} s "
                f"ON {overlap} AND s.vt < m.valid_to",
                [r, graph_uri, r, graph_uri],
            )
            execute(
                conn,
                f"UPDATE model_triples SET recorded_to = ? FROM {items} s "
                "WHERE s.op = '-' AND model_triples.graph_uri = ? AND model_triples.recorded_to IS NULL "
                "AND model_triples.triple = s.triple AND model_triples.valid_from < s.vt AND model_triples.valid_to > s.vf",
                [r, graph_uri],
            )
            # Insertions: one row spans the inserted interval and every row of
            # the triple it overlaps or touches. An interval already inside a
            # row changes nothing.
            merged = self._table(conn, [
                ("triple", "VARCHAR"), ("vf", "BIGINT"), ("vt", "BIGINT"),
                ("mf", "BIGINT"), ("mt", "BIGINT"), ("inside", "INTEGER"),
            ])
            execute(
                conn,
                f"INSERT INTO {merged} (triple, vf, vt, mf, mt, inside) "
                "SELECT s.triple, s.vf, s.vt, MIN(m.valid_from), MAX(m.valid_to), "
                "MAX(CASE WHEN m.valid_from <= s.vf AND m.valid_to >= s.vt THEN 1 ELSE 0 END) "
                f"FROM {items} s LEFT JOIN model_triples m ON m.graph_uri = ? AND m.recorded_to IS NULL "
                "AND m.triple = s.triple AND m.valid_from <= s.vt AND m.valid_to >= s.vf "
                "WHERE s.op = '+' GROUP BY s.triple, s.vf, s.vt",
                [graph_uri],
            )
            execute(
                conn,
                f"UPDATE model_triples SET recorded_to = ? FROM {merged} p "
                "WHERE p.inside = 0 AND model_triples.graph_uri = ? AND model_triples.recorded_to IS NULL "
                "AND model_triples.triple = p.triple AND model_triples.valid_from <= p.vt AND model_triples.valid_to >= p.vf",
                [r, graph_uri],
            )
            execute(
                conn,
                "INSERT INTO model_triples (graph_uri, triple, valid_from, valid_to, recorded_from, recorded_to) "
                "SELECT ?, p.triple, "
                "CASE WHEN p.mf IS NOT NULL AND p.mf < p.vf THEN p.mf ELSE p.vf END, "
                "CASE WHEN p.mt IS NOT NULL AND p.mt > p.vt THEN p.mt ELSE p.vt END, "
                f"?, CAST(NULL AS BIGINT) FROM {merged} p WHERE p.inside = 0",
                [graph_uri, r],
            )
        finally:
            execute(conn, f"DROP TABLE {items}")
            if merged is not None:
                execute(conn, f"DROP TABLE {merged}")

    # -------------------- staging --------------------
    def _table(self, conn, columns: Sequence[tuple[str, str]]) -> str:
        name = f"_acq_model_{uuid4().hex[:12]}"
        spec = ", ".join(f"{column} {kind}" for column, kind in columns)
        execute(conn, f"CREATE TEMP TABLE {name} ({spec})")
        return name

    def _stage(self, conn, columns: Sequence[tuple[str, str]], rows: Iterable[tuple]) -> str:
        """A temp table holding *rows*; the caller drops it."""
        name = self._table(conn, columns)
        rows = list(rows)
        if not rows:
            return name
        names = ", ".join(column for column, _ in columns)
        if self._postgres:
            with conn.cursor() as cur, cur.copy(f"COPY {name} ({names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)
        else:
            import pyarrow as pa

            types = {"VARCHAR": pa.string(), "BIGINT": pa.int64(), "INTEGER": pa.int32()}
            table = pa.table({
                column: pa.array([row[i] for row in rows], type=types[kind])
                for i, (column, kind) in enumerate(columns)
            })
            view = f"{name}_src"
            conn.register(view, table)
            try:
                conn.execute(f"INSERT INTO {name} SELECT {names} FROM {view}")
            finally:
                conn.unregister(view)
        return name


# -------------------- interval sets --------------------
def merge(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sorted disjoint union; touching intervals join."""
    out: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if out and start <= out[-1][1]:
            if end > out[-1][1]:
                out[-1] = (out[-1][0], end)
        else:
            out.append((start, end))
    return out


def symmetric_difference(a: Iterable[tuple[int, int]], b: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """The points covered by exactly one of *a* and *b*."""
    edges: dict[int, int] = defaultdict(int)
    for start, end in merge(a):
        edges[start] += 1
        edges[end] -= 1
    for start, end in merge(b):
        edges[start] += 1
        edges[end] -= 1
    out: list[tuple[int, int]] = []
    depth = 0
    left = None
    for point in sorted(edges):
        before = depth
        depth += edges[point]
        if before != 1 and depth == 1:
            left = point
        elif before == 1 and depth != 1 and left is not None:
            out.append((left, point))
            left = None
    return merge(out)
