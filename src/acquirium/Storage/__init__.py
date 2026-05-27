"""Storage backends for timeseries data and metadata."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from .base import TimeseriesStore
from .graph_store import OxigraphGraphStore
from .timescale_store import TimescaleStore

if TYPE_CHECKING:
    from .duckdb_store import DuckDBStore

logger = logging.getLogger("acquirium.storage")


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
    logger.debug("create_timeseries_store backend=%s recreate=%s", backend, recreate)
    if backend == "duckdb":
        from .duckdb_store import DuckDBStore as _DuckDBStore  # noqa: PLC0415
        if duckdb_path is None:
            raise ValueError("duckdb_path is required for the duckdb backend")
        logger.debug("create_timeseries_store: duckdb path=%s", duckdb_path)
        return _DuckDBStore(db_path=duckdb_path, recreate=recreate)
    elif backend == "timescale":
        if not pg_dsn:
            raise ValueError("pg_dsn is required for the timescale backend")
        logger.debug("create_timeseries_store: timescale dsn-host=%s", pg_dsn.split("@")[-1] if "@" in pg_dsn else "<redacted>")
        return TimescaleStore(dsn=pg_dsn, recreate=recreate)
    else:
        raise ValueError(
            f"Unknown timeseries backend: {backend!r}. Choose 'timescale' or 'duckdb'."
        )


__all__ = [
    "TimeseriesStore",
    "TimescaleStore",
    "OxigraphGraphStore",
    "create_timeseries_store",
]
