"""SQL storage for experiment records, in DuckDB or PostgreSQL.

Experiments keep their templates, variables, runs, observations, artifact
bytes, and graph-metadata outbox here. Their time-series samples live in
Escapement like every other stream; ``samples`` is where a ledger sends them.

A ledger exposes the connection protocol the experiment code uses: ``_lock``
serializes writers, ``_write_conn()`` yields a connection inside a transaction
(call it with ``_lock`` held), and ``_own_conn()`` yields an autocommit
connection for reads and DDL.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# Dropped by ``recreate``, dependents first.
LEDGER_TABLES = (
    "experiment_metadata_outbox",
    "experiment_metadata_state",
    "experiment_observations",
    "experiment_artifacts",
    "experiment_artifact_chunks",
    "experiment_runs",
    "experiment_variables",
    "experiment_templates",
    "experiment_metadata_resolutions",
    "experiment_streams",
    "schema_migrations",
)


def is_postgres(location: str | Path) -> bool:
    return str(location).startswith(("postgres://", "postgresql://"))


def open_ledger(location: str | Path, *, samples: Any = None, recreate: bool = False):
    """A ledger at a DuckDB path or a ``postgres://`` URL."""
    if is_postgres(location):
        return PostgresLedger(str(location), samples=samples, recreate=recreate)
    return DuckDBLedger(location, samples=samples, recreate=recreate)


class DuckDBLedger:
    def __init__(self, path: str | Path, *, samples: Any = None, recreate: bool = False) -> None:
        import duckdb

        self._duckdb = duckdb
        self.db_path = Path(path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.samples = samples
        self._lock = threading.Lock()
        # Held open so per-operation connections attach to a cached database
        # instead of reopening the file.
        self._anchor_conn = duckdb.connect(str(self.db_path))
        if recreate:
            with self._lock, self._own_conn() as conn:
                for table in LEDGER_TABLES:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")

    def _connect(self):
        return self._duckdb.connect(str(self.db_path))

    @contextmanager
    def _own_conn(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_conn(self):
        conn = self._connect()
        try:
            conn.begin()
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

    def close(self) -> None:
        self._anchor_conn.close()


class PostgresLedger:
    def __init__(self, dsn: str, *, samples: Any = None, recreate: bool = False) -> None:
        import psycopg

        self._psycopg = psycopg
        self.dsn = dsn
        self.samples = samples
        self._lock = threading.Lock()
        # Writes share one connection so a transaction governs every statement.
        self.conn = psycopg.connect(dsn, autocommit=True)
        if recreate:
            with self._lock, self._own_conn() as conn:
                for table in LEDGER_TABLES:
                    conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    def _connect(self):
        return self._psycopg.connect(self.dsn, autocommit=True)

    @contextmanager
    def _own_conn(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_conn(self):
        with self.conn.transaction():
            yield self.conn

    def close(self) -> None:
        self.conn.close()
