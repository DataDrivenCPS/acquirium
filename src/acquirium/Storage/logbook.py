"""The plant and property logbook, stored beside the experiment ledger."""

from datetime import datetime, timezone

from acquirium.Storage.metadata_outbox import execute
from acquirium.Storage.migrations import Migration, migrate
from acquirium.internals.models import LogEntry, TimeIntervalModel


def _create_logbook(conn):
    conn.execute("""
        CREATE TABLE acquirium_logs (
            property_uri VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            observed_start TIMESTAMP,
            observed_end TIMESTAMP,
            message VARCHAR,
            PRIMARY KEY (property_uri, timestamp)
        )
    """)


def _utc_naive(value: datetime) -> datetime:
    """Convert to the database's naive UTC representation."""
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _utc(value: datetime | None) -> datetime | None:
    return value.replace(tzinfo=timezone.utc) if value is not None else None


class Logbook:
    def __init__(self, ledger, *, backend: str):
        self.ledger = ledger
        with ledger._lock, ledger._write_conn() as conn:
            migrate(
                conn,
                backend=backend,
                component="logbook",
                migrations=(Migration(1, "logbook", _create_logbook),),
            )

    def insert(self, log: LogEntry) -> None:
        """Upsert an entry keyed by property URI and UTC log timestamp."""
        start, end = log.period.start, log.period.end
        with self.ledger._lock, self.ledger._write_conn() as conn:
            execute(
                conn,
                """
                INSERT INTO acquirium_logs (property_uri, timestamp, observed_start, observed_end, message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (property_uri, timestamp) DO UPDATE SET
                    message = excluded.message,
                    observed_start = excluded.observed_start,
                    observed_end = excluded.observed_end
            """,
                [
                    log.property_uri,
                    _utc_naive(log.timestamp),
                    _utc_naive(start) if start is not None else None,
                    _utc_naive(end) if end is not None else None,
                    log.message,
                ],
            )

    def query(
        self,
        property_uri: str,
        log_time_interval: TimeIntervalModel | None,
        obs_time_interval: TimeIntervalModel | None,
    ) -> list[LogEntry]:
        """Read a property's entries in log timestamp order.

        Log-time bounds include their endpoints. Observation-time bounds
        check interval overlap with strict comparisons at the endpoints.
        """
        clauses = ["property_uri = ?"]
        params = [property_uri]
        if log_time_interval is not None:
            if log_time_interval.start is not None:
                clauses.append("timestamp >= ?")
                params.append(_utc_naive(log_time_interval.start))
            if log_time_interval.end is not None:
                clauses.append("timestamp <= ?")
                params.append(_utc_naive(log_time_interval.end))
        if obs_time_interval is not None:
            start, end = obs_time_interval.start, obs_time_interval.end
            if start is not None and end is not None:
                # A bounded overlap query includes continuing observations
                # whose end has yet to be recorded.
                clauses.append(
                    "observed_start < ? AND (observed_end IS NULL OR observed_end > ?)"
                )
                params.extend((_utc_naive(end), _utc_naive(start)))
            elif start is not None:
                clauses.append("observed_end > ?")
                params.append(_utc_naive(start))
            elif end is not None:
                clauses.append("observed_start < ?")
                params.append(_utc_naive(end))
        where = " AND ".join(clauses)
        with self.ledger._own_conn() as conn:
            rows = execute(
                conn,
                f"""
                SELECT timestamp, observed_start, observed_end, message
                FROM acquirium_logs WHERE {where} ORDER BY timestamp
            """,
                params,
            ).fetchall()
        return [
            LogEntry(
                property_uri=property_uri,
                timestamp=_utc(ts),
                message=message,
                period=TimeIntervalModel(start=_utc(start), end=_utc(end)),
            )
            for ts, start, end, message in rows
        ]

    def delete(self, property_uri: str) -> None:
        """Remove every log entry for the property."""
        with self.ledger._lock, self.ledger._write_conn() as conn:
            execute(
                conn,
                "DELETE FROM acquirium_logs WHERE property_uri = ?",
                [property_uri],
            )
