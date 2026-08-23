"""PostgreSQL counterpart of :class:`MaterializationDuckDB`.

The state-machine methods are shared with DuckDB through the connection
adapter; only the schema dialect and the reads against the differently-shaped
canonical timeseries tables are overridden here.
"""
from __future__ import annotations

from datetime import datetime
from typing import Sequence

import pyarrow as pa

from acquirium.Storage.materialization.dialect import PostgresCodecs, PostgresStoreAdapter
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.schema import change_range_statements, support_statements


class MaterializationPostgres(PostgresCodecs, MaterializationDuckDB):
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 4) -> None:
        self._store = PostgresStoreAdapter(dsn, min_size=min_size, max_size=max_size)
        with self._store._write_conn() as conn:
            for statement in (*change_range_statements(self._DIALECT), *support_statements(self._DIALECT)):
                conn.execute(statement)

    def close(self) -> None:
        self._store.close()

    def stream_versions(self, refs: Sequence[str]) -> dict[str, int]:
        if not refs: return {}
        with self._store._own_conn() as conn:
            rows = conn.execute("""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                FROM unnest(%s::text[]) AS requested(ref_uri) LEFT JOIN stream_heads head
                ON head.ref_uri = requested.ref_uri ORDER BY requested.ref_uri""", [list(refs)]).fetchall()
        return dict(rows)

    def all_stream_versions(self) -> dict[str, int]:
        with self._store._own_conn() as conn:
            return dict(conn.execute("SELECT ref_uri, current_version FROM stream_heads ORDER BY ref_uri").fetchall())

    def service_input_snapshot(self, refs: Sequence[str], *, since: datetime | None = None) -> tuple[dict[str, int], pa.Table]:
        with self._store._own_conn() as conn:
            versions = dict(conn.execute("""SELECT requested.ref_uri, COALESCE(head.current_version, 0)
                FROM unnest(%s::text[]) AS requested(ref_uri) LEFT JOIN stream_heads head
                ON head.ref_uri = requested.ref_uri""", [list(refs)]).fetchall())
            if since is None:
                rows = conn.execute("""SELECT DISTINCT ON (ref_uri) ref_uri, ts, numeric_value, text_value
                    FROM timeseries WHERE ref_uri = ANY(%s::text[]) AND NOT deleted
                    ORDER BY ref_uri, ts DESC""", [list(refs)]).fetchall()
            else:
                rows = conn.execute("""SELECT ref_uri, ts, numeric_value, text_value FROM timeseries
                    WHERE ref_uri = ANY(%s::text[]) AND NOT deleted AND ts >= %s
                    ORDER BY ref_uri, ts""", [list(refs), since]).fetchall()
        return versions, self._snapshot_table(rows)
