"""Backend dialect plumbing shared by every materialization store pair.

The DuckDB classes hold the canonical state-machine logic, written with ``?``
placeholders and the codec hooks below.  A PostgreSQL twin subclasses its
DuckDB class, swaps in :class:`PostgresStoreAdapter`, and overrides only the
codecs and the few queries that touch the differently-shaped canonical tables.
"""
from __future__ import annotations

import json
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


class DuckDBCodecs:
    """Value conversions for DuckDB's naive-UTC timestamps and text JSON."""

    _DIALECT = "duckdb"
    # DuckDB has a single writer serialized by the store lock, so row locks
    # are unnecessary and unsupported.
    _FOR_UPDATE = ""
    _SKIP_LOCKED = ""

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _stored_timestamp(value: datetime) -> datetime:
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value

    @staticmethod
    def _aware(value: datetime) -> datetime:
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

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
    """Value conversions for PostgreSQL's aware timestamps and JSONB columns."""

    _DIALECT = "postgres"
    _FOR_UPDATE = " FOR UPDATE"
    _SKIP_LOCKED = " FOR UPDATE SKIP LOCKED"

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _stored_timestamp(value: datetime) -> datetime:
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


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
