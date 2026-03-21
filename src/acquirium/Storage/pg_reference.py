"""Registry for PGReference external references.

Stores connection metadata extracted from the RDF graph and provides a
``timeseries()`` method that queries the external Postgres database,
returning ``pa.RecordBatch`` iterators identical to ``TimescaleStore``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

import logging
import psycopg
from psycopg import sql
import pyarrow as pa

from acquirium.internals.models import TimeseriesInfo

logger = logging.getLogger("acquirium.pg_reference")


@dataclass(frozen=True)
class PGReferenceInfo:
    """Connection and query metadata for a single PGReference node."""

    dsn: str
    table: str | None = None
    custom_query: str | None = None
    time_col: str = "time"
    value_col: str = "value"
    point_filter: str | None = None


def resolve_dsn(
    *,
    dsn: str | None = None,
    host: str | None = None,
    port: str | None = None,
    db: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> str:
    """Build a DSN from either a full string or individual components."""
    if dsn:
        return dsn
    if not host:
        raise ValueError("PGReference requires either PG_DSN or PG_HOST")
    parts = [f"postgresql://{user or ''}"]
    if password:
        parts[0] += f":{password}"
    parts[0] += f"@{host}:{port or '5432'}/{db or ''}"
    return parts[0]


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class PGReferenceRegistry:
    """In-memory map from reference URIs to external Postgres connections."""

    def __init__(self) -> None:
        self._refs: dict[str, PGReferenceInfo] = {}

    def register(self, ref_uri: str, info: PGReferenceInfo) -> None:
        self._refs[ref_uri] = info
        logger.info("Registered PGReference %s → %s/%s", ref_uri, info.dsn.split("@")[-1], info.table or "query")

    def is_pg_reference(self, uri: str) -> bool:
        return uri in self._refs

    @property
    def count(self) -> int:
        return len(self._refs)

    def timeseries(
        self,
        point_uri: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        batch_size: int = 50_000,
    ) -> Iterator[pa.RecordBatch]:
        """Query the external Postgres and yield RecordBatches.

        The output schema matches ``TimescaleStore.timeseries()`` exactly:
        ``[ts: timestamp[us, tz=UTC], value: string, uri: string]``.
        """
        info = self._refs[point_uri]
        conn = psycopg.connect(info.dsn, autocommit=True)
        try:
            with conn.cursor() as cur:
                if info.custom_query:
                    cur.execute(info.custom_query)
                else:
                    if not info.table:
                        raise ValueError(f"PGReference {point_uri}: PG_Table or PG_Query required")

                    clauses: list[str] = []
                    params: list[Any] = []

                    if info.point_filter:
                        clauses.append("point_uri = %s")
                        params.append(info.point_filter)
                    if start:
                        clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(info.time_col)).as_string(conn))
                        params.append(_to_utc(start))
                    if end:
                        clauses.append(sql.SQL("{} <= %s").format(sql.Identifier(info.time_col)).as_string(conn))
                        params.append(_to_utc(end))

                    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
                    order_sql = "ASC" if order == "asc" else "DESC"
                    limit_sql = f" LIMIT {int(limit)}" if limit else ""

                    query = (
                        sql.SQL("SELECT {time}, {value} FROM {table}").format(
                            time=sql.Identifier(info.time_col),
                            value=sql.Identifier(info.value_col),
                            table=sql.Identifier(info.table),
                        ).as_string(conn)
                        + where
                        + f" ORDER BY {sql.Identifier(info.time_col).as_string(conn)} {order_sql}"
                        + limit_sql
                    )
                    cur.execute(query, params)

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break

                    ts_col = [r[0] for r in rows]
                    val_col = [str(r[1]) if r[1] is not None else None for r in rows]
                    uri_col = [point_uri] * len(rows)

                    yield pa.record_batch(
                        [
                            pa.array(ts_col, type=pa.timestamp("us", tz="UTC")),
                            pa.array(val_col, type=pa.string()),
                            pa.array(uri_col, type=pa.string()),
                        ],
                        names=["ts", "value", "uri"],
                    )
        finally:
            conn.close()

    def timeseries_info_batch(self, point_uris: list[str]) -> dict[str, TimeseriesInfo]:
        """Return stats for PGReference URIs. Only returns entries for registered URIs."""
        result: dict[str, TimeseriesInfo] = {}
        for uri in point_uris:
            if uri not in self._refs:
                continue
            info = self._refs[uri]
            conn = psycopg.connect(info.dsn, autocommit=True)
            try:
                with conn.cursor() as cur:
                    if info.custom_query:
                        cur.execute(f"SELECT COUNT(*), MIN(sub.t), MAX(sub.t) FROM ({info.custom_query}) sub(t, v)")
                    else:
                        if not info.table:
                            continue
                        q = sql.SQL("SELECT COUNT(*), MIN({time}), MAX({time}) FROM {table}").format(
                            time=sql.Identifier(info.time_col), table=sql.Identifier(info.table),
                        )
                        if info.point_filter:
                            cur.execute(q.as_string(conn) + " WHERE point_uri = %s", [info.point_filter])
                        else:
                            cur.execute(q.as_string(conn))
                    cnt, earliest, latest = cur.fetchone()
                    result[uri] = TimeseriesInfo(table=info.table or "external", row_count=cnt or 0, earliest=earliest, latest=latest)
            finally:
                conn.close()
        return result
