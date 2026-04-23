"""Storage backends for timeseries data and metadata."""

from pathlib import Path
from typing import TYPE_CHECKING

from .base import TimeseriesStore
from .graph_store import OxigraphGraphStore
from .timescale_store import TimescaleStore
from .pg_reference import PGReferenceRegistry, PGReferenceInfo, resolve_dsn

if TYPE_CHECKING:
    from .duckdb_store import DuckDBStore


def create_timeseries_store(
    backend: str = "timescale",
    *,
    pg_dsn: str | None = None,
    duckdb_path: str | Path | None = None,
    recreate: bool = False,
) -> TimeseriesStore:
    """Instantiate the timeseries backend selected by *backend*.

    ``backend="timescale"`` — returns :class:`TimescaleStore` (requires *pg_dsn*)
    ``backend="duckdb"``    — returns :class:`DuckDBStore` (requires *duckdb_path*)

    The ``duckdb`` backend lazily imports ``duckdb`` so the package is not
    required when using the TimescaleDB backend.
    """
    if backend == "duckdb":
        from .duckdb_store import DuckDBStore as _DuckDBStore  # noqa: PLC0415
        if duckdb_path is None:
            raise ValueError("duckdb_path is required for the duckdb backend")
        return _DuckDBStore(db_path=duckdb_path, recreate=recreate)
    elif backend in ("timescale", "postgres"):
        if not pg_dsn:
            raise ValueError("pg_dsn is required for the timescale backend")
        return TimescaleStore(dsn=pg_dsn, recreate=recreate)
    else:
        raise ValueError(
            f"Unknown timeseries backend: {backend!r}. Choose 'timescale' or 'duckdb'."
        )


__all__ = [
    "TimeseriesStore",
    "TimescaleStore",
    "OxigraphGraphStore",
    "PGReferenceRegistry",
    "PGReferenceInfo",
    "resolve_dsn",
    "create_timeseries_store",
]
