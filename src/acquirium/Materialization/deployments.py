"""The view set over time.

The view set changes like the model graphs: a deployment change gives a view
name a definition over an effective interval ``[start, end)``, or no
definition (``None``) to remove it there, and Acquirium keeps every change.
The definition a name has at a time is the one of the latest-recorded change
whose interval contains that time. Deploying from ``d`` is a change over
``[d, +inf)``; removing at ``e`` is ``None`` over ``[e, +inf)``, which keeps
the outputs before ``e``; purging is ``None`` over all time.

Definitions are compared as text, so a redeployment of the same text is no
change. Times are integer microseconds; recording times ``r`` increase.

``DeploymentLog`` keeps the changes in the time-series database, table
``view_deployments``, one row per change. The log is small (a few rows per
deployment), so reads load it and fold it with the functions here.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from acquirium.internals.intervals import NEG_INF, POS_INF, Interval, merge, now_micros
from acquirium.Storage.metadata_outbox import execute
from acquirium.Storage.migrations import Migration, migrate


@dataclass(frozen=True)
class DeploymentChange:
    r: int
    name: str
    definition: str | None
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError("a deployment change needs start < end")


def _pieces(changes: Iterable[DeploymentChange]) -> list[tuple[int, int, str | None]]:
    """Elementary intervals between the endpoints of one name's changes, each
    with the definition of the latest-recorded change covering it."""
    changes = sorted(changes, key=lambda change: change.r)
    points = sorted({p for change in changes for p in (change.start, change.end)})
    out: list[tuple[int, int, str | None]] = []
    for left, right in zip(points, points[1:]):
        definition = None
        for change in changes:
            if change.start <= left < change.end:
                definition = change.definition
        if out and out[-1][1] == left and out[-1][2] == definition:
            out[-1] = (out[-1][0], right, definition)
        else:
            out.append((left, right, definition))
    return out


def deployment(changes: Iterable[DeploymentChange], name: str, *, as_of: int | None = None) -> list[tuple[int, int, str]]:
    """The maximal intervals over which ``name`` has one definition, with that
    definition, as known at recording time ``as_of`` (the latest if omitted)."""
    own = [c for c in changes if c.name == name and (as_of is None or c.r <= as_of)]
    return [(left, right, definition) for left, right, definition in _pieces(own) if definition is not None]


def definition_at(changes: Iterable[DeploymentChange], name: str, at: int, *, as_of: int | None = None) -> str | None:
    for left, right, definition in deployment(changes, name, as_of=as_of):
        if left <= at < right:
            return definition
    return None


def names(changes: Iterable[DeploymentChange]) -> list[str]:
    return sorted({change.name for change in changes})


def changed(changes: Iterable[DeploymentChange], r: int) -> dict[str, list[Interval]]:
    """Where the changes recorded at ``r`` changed the view set: per name, the
    intervals over which its definition differs before and after them."""
    changes = list(changes)
    regions: dict[str, list[Interval]] = {}
    for name in sorted({c.name for c in changes if c.r == r}):
        before = deployment(changes, name, as_of=r - 1)
        after = deployment(changes, name, as_of=r)
        points = sorted({p for left, right, _ in before + after for p in (left, right)})
        region = [
            (left, right)
            for left, right in zip(points, points[1:])
            if _at(before, left) != _at(after, left)
        ]
        if region:
            regions[name] = merge(region)
    return regions


def _at(pieces: list[tuple[int, int, str]], at: int) -> str | None:
    for left, right, definition in pieces:
        if left <= at < right:
            return definition
    return None


def _adopt_current_deployments(conn) -> None:
    """Turn the rows of the former current-state table into changes over all
    time, then drop it. Its deployments had no recorded start."""
    exists = execute(
        conn,
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'materialization_deployments' "
        "AND table_schema = current_schema()",
    ).fetchone()
    if exists is None:
        return
    rows = execute(conn, "SELECT name, deployment_json FROM materialization_deployments ORDER BY name").fetchall()
    recorded_at = now_micros()
    for r, (name, definition) in enumerate(rows, start=1):
        execute(conn, "INSERT INTO view_deployments VALUES (?, ?, ?, ?, ?, ?)",
                [r, recorded_at, name, definition, NEG_INF, POS_INF])
    execute(conn, "DROP TABLE materialization_deployments")


DEPLOYMENT_LOG_MIGRATIONS = (
    Migration(1, "view_deployments", (
        """CREATE TABLE IF NOT EXISTS view_deployments (
            r BIGINT NOT NULL, recorded_at BIGINT NOT NULL, name VARCHAR NOT NULL,
            definition VARCHAR, valid_from BIGINT NOT NULL, valid_to BIGINT NOT NULL,
            PRIMARY KEY (r, name))""",
    )),
    Migration(2, "adopt_current_deployments", _adopt_current_deployments),
)


class DeploymentLog:
    """The recorded deployment changes. Callers serialize writes."""

    def __init__(self, store) -> None:
        self.store = store
        postgres = getattr(store, "materialization_backend", None) == "postgres"
        with store._lock, store._write_conn() as conn:
            migrate(conn, backend="postgres" if postgres else "duckdb",
                    component="view_deployments", migrations=DEPLOYMENT_LOG_MIGRATIONS)

    def changes(self) -> list[DeploymentChange]:
        with self.store._own_conn() as conn:
            rows = execute(conn, "SELECT r, name, definition, valid_from, valid_to FROM view_deployments ORDER BY r, name").fetchall()
        return [DeploymentChange(int(r), name, definition, int(start), int(end)) for r, name, definition, start, end in rows]

    def head(self) -> int:
        with self.store._own_conn() as conn:
            return int(execute(conn, "SELECT COALESCE(MAX(r), 0) FROM view_deployments").fetchone()[0])

    def current(self, *, at: int | None = None) -> dict[str, str]:
        """Every name's definition at ``at`` (now if omitted)."""
        changes = self.changes()
        at = now_micros() if at is None else at
        found = {name: definition_at(changes, name, at) for name in names(changes)}
        return {name: definition for name, definition in found.items() if definition is not None}

    def record(self, changes: Iterable[tuple[str, str | None, int, int]], *, conn=None) -> int:
        """Record ``(name, definition, start, end)`` changes as one write.

        No endpoint other than ``POS_INF`` may lie after the time of the
        write. Returns the write's recording time. With ``conn`` the rows are
        written in the caller's transaction.
        """
        changes = list(changes)
        if not changes:
            raise ValueError("a deployment write needs at least one change")
        recorded_at = now_micros()
        for name, _, start, end in changes:
            if start >= end:
                raise ValueError(f"deployment of {name!r}: the start must precede the end")
            if start > recorded_at or (end != POS_INF and end > recorded_at):
                raise ValueError(f"deployment of {name!r}: a start or end may not lie in the future")
        if len({name for name, *_ in changes}) != len(changes):
            raise ValueError("a deployment write changes each name at most once")
        if conn is not None:
            return self._insert(conn, changes, recorded_at)
        with self.store._lock, self.store._write_conn() as own:
            return self._insert(own, changes, recorded_at)

    @staticmethod
    def _insert(conn, changes, recorded_at: int) -> int:
        r = int(execute(conn, "SELECT COALESCE(MAX(r), 0) FROM view_deployments").fetchone()[0]) + 1
        for name, definition, start, end in changes:
            execute(conn, "INSERT INTO view_deployments VALUES (?, ?, ?, ?, ?, ?)",
                    [r, recorded_at, name, definition, start, end])
        return r


DEPLOYMENT_LOG_TABLES = ("view_deployments",)
