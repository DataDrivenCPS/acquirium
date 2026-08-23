"""Backend dialect plumbing shared by every materialization store pair.

The DuckDB classes hold the canonical state-machine logic, written with ``?``
placeholders against the ``_read``/``_write`` connections below, which deal
exclusively in aware UTC datetimes.  A PostgreSQL twin subclasses its DuckDB
class, swaps in :class:`PostgresStoreAdapter`, and overrides only the codecs
and the few queries that touch the differently-shaped canonical tables.
"""
from __future__ import annotations

import json
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _naive_utc(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _aware_utc(value):
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


class _TZResult:
    """Fetch results with naive-UTC timestamps converted to aware UTC."""

    def __init__(self, relation) -> None:
        self._relation = relation

    def fetchone(self):
        row = self._relation.fetchone()
        return None if row is None else tuple(_aware_utc(value) for value in row)

    def fetchall(self):
        return [tuple(_aware_utc(value) for value in row) for row in self._relation.fetchall()]

    def __getattr__(self, name):
        return getattr(self._relation, name)


class TZBoundaryConnection:
    """DuckDB stores naive-UTC timestamps; convert at the connection boundary
    so the shared state-machine code deals only in aware UTC datetimes."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def execute(self, sql: str, params=None):
        if params is None:
            return _TZResult(self._conn.execute(sql))
        return _TZResult(self._conn.execute(sql, [_naive_utc(value) for value in params]))

    def executemany(self, sql: str, rows):
        return self._conn.executemany(sql, [[_naive_utc(value) for value in row] for row in rows])

    def __getattr__(self, name):
        return getattr(self._conn, name)


class DuckDBCodecs:
    """Connection and value codecs for the DuckDB backend."""

    _DIALECT = "duckdb"
    # DuckDB has a single writer serialized by the store lock, so row locks
    # are unnecessary and unsupported.
    _FOR_UPDATE = ""
    _SKIP_LOCKED = ""

    @contextmanager
    def _read(self):
        with self._store._own_conn() as conn:
            yield TZBoundaryConnection(conn)

    @contextmanager
    def _write(self):
        with self._store._lock, self._store._write_conn() as conn:
            yield TZBoundaryConnection(conn)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _json(value: object) -> str:
        return canonical_json(value)

    @staticmethod
    def _decode(value):
        return json.loads(value) if isinstance(value, str) else value

    @staticmethod
    def _changed(conn, sql: str, params) -> int:
        """Count the rows a DML statement affected, on either backend.

        DuckDB's ``rowcount`` is always -1, so affected rows must be counted
        through ``RETURNING``; PostgreSQL behaves identically.
        """
        return len(conn.execute(sql + " RETURNING 1", params).fetchall())


class PostgresCodecs(DuckDBCodecs):
    """PostgreSQL stores aware timestamps natively, so no boundary conversion."""

    _DIALECT = "postgres"
    _FOR_UPDATE = " FOR UPDATE"
    _SKIP_LOCKED = " FOR UPDATE SKIP LOCKED"

    @contextmanager
    def _read(self):
        with self._store._own_conn() as conn:
            yield conn

    @contextmanager
    def _write(self):
        with self._store._write_conn() as conn:
            yield conn


class PostgresConnection:
    """Adapter running the shared ``?``-placeholder SQL against psycopg."""

    def __init__(self, connection) -> None:
        self._connection = connection

    @staticmethod
    def _sql(sql: str) -> str:
        # The shared store SQL intentionally uses only positional
        # placeholders.  Publication code uses PostgreSQL's native %s form
        # directly and never goes through this adapter.
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


class PostgresStoreAdapter:
    """Expose a psycopg pool through the DuckDB store's connection surface."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        from psycopg_pool import ConnectionPool
        self._pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)
        self._lock = nullcontext()

    @contextmanager
    def _own_conn(self):
        with self._pool.connection() as connection:
            yield PostgresConnection(connection)

    @contextmanager
    def _write_conn(self):
        with self._pool.connection() as connection, connection.transaction():
            yield PostgresConnection(connection)

    def close(self) -> None:
        self._pool.close()
