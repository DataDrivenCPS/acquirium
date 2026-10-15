"""The model's change log: every write to a data graph, with the time it took effect.

Queries keep reading the live graph. The log exists so a plan, or a query
over a time range, can see the model as it was at an earlier time.

A write is recorded before it is applied to the graph. The two stores do not
share a transaction, so a crash between them leaves a recorded change that the
graph does not have; ``replay`` applies those on startup. The reverse, an
applied change with no record, cannot happen.

A change is dated by its author (``effective_at``, default: the time of the
write). A change dated earlier than a recorded change that touches any of the
same triples is rejected: the live graph applies writes in the order they
arrive, and undoing changes in effective-time order must give the same graph.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any

import pyoxigraph as ox
from pyoxigraph import NamedNode, RdfFormat
from rdflib import Graph

_logger = logging.getLogger("acquirium.graph_history")


class GraphHistoryConflict(ValueError):
    """A dated write touches a triple that a later-dated change already changed."""


@dataclass(frozen=True)
class RecordedChange:
    change_id: int
    graph_uri: str
    effective_at: datetime
    added: frozenset
    removed: frozenset

    def delta(self):
        from acquirium.Storage.graph_store import GraphDelta, _triples_affect_closure
        return GraphDelta(self.graph_uri, self.added, self.removed,
                          affects_closure=_triples_affect_closure([*self.added, *self.removed]))


def _nt(triple) -> str:
    s, p, o = triple
    return f"{s.n3()} {p.n3()} {o.n3()} ."


def _parse_nt(line: str):
    return next(iter(Graph().parse(data=line, format="nt")))


def _when(text: str) -> datetime:
    return datetime.fromisoformat(text)


def _iso(when: datetime) -> str:
    # One fixed shape, so string order is time order in SQL.
    return when.astimezone(timezone.utc).isoformat(timespec="microseconds")


class GraphVersion:
    """The inferred model as of one time, queryable like the live graph store."""

    def __init__(self, store: ox.Store, data_uri: str, dependency_uri: str, *, revision: int, as_of: datetime | None) -> None:
        self._store, self._data_uri, self._dependency_uri = store, data_uri, dependency_uri
        self.revision, self.as_of = revision, as_of

    def sparql_query(self, query: str, include_dependencies: bool = True, *, wait_for_fresh: bool = False) -> dict:
        from acquirium.Storage.graph_store import query_result_to_dict
        graphs = [NamedNode(self._data_uri)] + ([NamedNode(self._dependency_uri)] if include_dependencies else [])
        return query_result_to_dict(self._store.query(query, use_default_graph_as_union=False, default_graph=graphs))

    def graph_status(self) -> dict[str, int | bool]:
        return {"source_version": self.revision, "published_version": self.revision, "is_current": True,
                "rebuild_in_progress": False}


class GraphHistory:
    """Change log of the data graphs, in the time-series database."""

    def __init__(self, store: Any, graph: Any, *, cache_size: int = 8) -> None:
        self._store, self._graph = store, graph
        self._versions: OrderedDict[tuple, GraphVersion] = OrderedDict()
        self._cache_size = cache_size
        self._cache_lock = Lock()
        with store._lock, store._write_conn() as conn:
            self._execute(conn, """CREATE TABLE IF NOT EXISTS graph_history (
                change_id BIGINT PRIMARY KEY, graph_uri VARCHAR NOT NULL, effective_at VARCHAR NOT NULL,
                recorded_at VARCHAR NOT NULL, applied BOOLEAN NOT NULL, source_version BIGINT,
                initial BOOLEAN NOT NULL)""")
            self._execute(conn, """CREATE TABLE IF NOT EXISTS graph_history_triples (
                change_id BIGINT NOT NULL, op VARCHAR NOT NULL, triple VARCHAR NOT NULL)""")
            self._execute(conn, "CREATE INDEX IF NOT EXISTS graph_history_triples_triple ON graph_history_triples (triple)")

    def _execute(self, conn: Any, query: str, params=()):
        if getattr(self._store, "materialization_backend", None) == "postgres":
            query = query.replace("?", "%s")
        return conn.execute(query, list(params))

    def _executemany(self, conn: Any, query: str, rows: list[tuple]) -> None:
        if not rows:
            return
        if getattr(self._store, "materialization_backend", None) == "postgres":
            with conn.cursor() as cur:
                cur.executemany(query.replace("?", "%s"), rows)
        else:
            conn.executemany(query, rows)

    # ---- recording ----

    def record(self, delta, effective_at: datetime) -> int:
        """Log a prepared delta as taking effect at ``effective_at``; committed on return.

        Raises ``GraphHistoryConflict`` before writing anything if a later-dated
        recorded change touches one of the same triples.
        """
        effective = effective_at.astimezone(timezone.utc)
        lines = [("+", _nt(t)) for t in sorted(delta.added, key=_nt)] + [("-", _nt(t)) for t in sorted(delta.removed, key=_nt)]
        with self._store._lock, self._store._write_conn() as conn:
            self._check_conflicts(conn, delta.graph_uri, effective, [line for _, line in lines])
            change_id = int(self._execute(conn, "SELECT COALESCE(MAX(change_id), 0) + 1 FROM graph_history").fetchone()[0])
            self._execute(conn, "INSERT INTO graph_history VALUES (?, ?, ?, ?, ?, NULL, ?)",
                          [change_id, delta.graph_uri, _iso(effective), _iso(datetime.now(timezone.utc)), False,
                           bool(getattr(delta, "initial", False))])
            self._executemany(conn, "INSERT INTO graph_history_triples VALUES (?, ?, ?)",
                              [(change_id, op, line) for op, line in lines])
        return change_id

    def _check_conflicts(self, conn, graph_uri: str, effective: datetime, lines: list[str]) -> None:
        for start in range(0, len(lines), 500):
            chunk = lines[start:start + 500]
            marks = ",".join("?" for _ in chunk)
            row = self._execute(conn, f"""SELECT h.effective_at, t.triple FROM graph_history_triples t
                JOIN graph_history h ON h.change_id = t.change_id
                WHERE h.graph_uri = ? AND h.effective_at > ? AND t.triple IN ({marks})
                ORDER BY h.effective_at DESC LIMIT 1""", [graph_uri, _iso(effective), *chunk]).fetchone()
            if row is not None:
                raise GraphHistoryConflict(
                    f"a change effective {row[0]} already touches {row[1]}; "
                    f"a write effective {_iso(effective)} cannot be dated before it")

    def mark_applied(self, conn, change_id: int, source_version: int) -> None:
        """Record that the graph holds this change. Runs inside the caller's transaction."""
        self._execute(conn, "UPDATE graph_history SET applied = ?, source_version = ? WHERE change_id = ?",
                      [True, int(source_version), change_id])
        self._invalidate()

    def unapplied(self) -> list[RecordedChange]:
        """Recorded changes the graph does not have yet, oldest first."""
        with self._store._own_conn() as conn:
            heads = self._execute(conn, "SELECT change_id, graph_uri, effective_at FROM graph_history WHERE NOT applied ORDER BY change_id").fetchall()
            return [self._change(conn, change_id, graph_uri, _when(effective)) for change_id, graph_uri, effective in heads]

    def _change(self, conn, change_id: int, graph_uri: str, effective: datetime) -> RecordedChange:
        rows = self._execute(conn, "SELECT op, triple FROM graph_history_triples WHERE change_id = ?", [change_id]).fetchall()
        return RecordedChange(change_id, graph_uri, effective,
                              frozenset(_parse_nt(line) for op, line in rows if op == "+"),
                              frozenset(_parse_nt(line) for op, line in rows if op == "-"))

    # ---- versions ----

    def change_points(self) -> list[datetime]:
        """Effective times at which the model changed, ascending.

        An empty change does not count, nor does a graph's initial load: the
        model before it is not "nothing", it is unknown, and the load stands
        for all time.
        """
        with self._store._own_conn() as conn:
            rows = self._execute(conn, """SELECT DISTINCT h.effective_at FROM graph_history h
                WHERE h.applied AND NOT h.initial
                AND EXISTS (SELECT 1 FROM graph_history_triples t WHERE t.change_id = h.change_id)
                ORDER BY h.effective_at""").fetchall()
        return [_when(row[0]) for row in rows]

    def changes_after(self, when: datetime) -> list[RecordedChange]:
        """Applied changes effective after ``when``, newest recorded first (undo order)."""
        with self._store._own_conn() as conn:
            heads = self._execute(conn, "SELECT change_id, graph_uri, effective_at FROM graph_history "
                                        "WHERE applied AND NOT initial AND effective_at > ? ORDER BY change_id DESC",
                                  [_iso(when)]).fetchall()
            return [self._change(conn, change_id, graph_uri, _when(effective)) for change_id, graph_uri, effective in heads]

    def as_of(self, when: datetime | None) -> GraphVersion:
        """The inferred model as of ``when`` (``None``: before the first change).

        Built from the live data graphs with every later-dated change undone,
        then inferred like the live query graph. Cached per change point.
        """
        status = self._graph.graph_status()
        points = self.change_points()
        # Any time between two change points sees the same model; key on the
        # last point at or before ``when``.
        version_at = max((p for p in points if when is not None and p <= when), default=None)
        key = (version_at, int(status.get("source_version", 0)), getattr(self._graph, "_closure_version", 0))
        with self._cache_lock:
            if key in self._versions:
                self._versions.move_to_end(key)
                return self._versions[key]
        version = self._build(version_at, int(status.get("published_version", 0)))
        with self._cache_lock:
            self._versions[key] = version
            while len(self._versions) > self._cache_size:
                self._versions.popitem(last=False)
        return version

    def _build(self, when: datetime | None, revision: int) -> GraphVersion:
        import shifty
        graph = self._graph
        with graph._lock:
            data = graph._source_data_graph()
            shapes = Graph() + graph._ensure_dependency_cache_current()
        undone = self.changes_after(when) if when is not None else self.changes_after(datetime.min.replace(tzinfo=timezone.utc))
        for change in undone:
            for triple in change.added:
                data.remove(triple)
            for triple in change.removed:
                data.add(triple)
        inferred = shifty.infer(data, shapes).graph()
        store = ox.Store()
        data_uri, dependency_uri = str(graph.inferred_data_graph_uri), str(graph.dependency_query_graph_uri)
        for uri, g in ((data_uri, inferred), (dependency_uri, shapes)):
            if len(g):
                store.load(input=g.serialize(format="nt", encoding="utf-8"), format=RdfFormat.N_TRIPLES, to_graph=NamedNode(uri))
        _logger.debug("graph version as of %s: %d changes undone, %d inferred triples", when, len(undone), len(inferred))
        return GraphVersion(store, data_uri, dependency_uri, revision=revision, as_of=when)

    def _invalidate(self) -> None:
        with self._cache_lock:
            self._versions.clear()
