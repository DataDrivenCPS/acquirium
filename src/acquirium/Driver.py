from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdflib import URIRef

from acquirium.DriverState import DriverState
from acquirium.internals.models import compute_ref_uri
from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.driver")

if TYPE_CHECKING:
    import polars as pl

    from acquirium.Client.acquirium import Acquirium


def _cast_value_to_utf8(col: "pl.Series") -> "pl.Expr":
    import math
    import polars as pl

    name = col.name
    if col.dtype == pl.Object:
        return pl.col(name).map_elements(
            lambda v: None if (v is None or (isinstance(v, float) and math.isnan(v))) else str(v),
            return_dtype=pl.Utf8,
        )
    if col.dtype in (pl.Float32, pl.Float64):
        return pl.col(name).fill_nan(None).cast(pl.Utf8)
    return pl.col(name).cast(pl.Utf8)


class Driver(ABC):
    """Base class for data-collection / processing drivers.

    The ``acquirium run`` CLI calls :meth:`setup` once at startup, then calls
    :meth:`tick` repeatedly, sleeping for ``interval`` seconds between calls.
    Override :meth:`stop` for cleanup on Ctrl-C or SIGTERM.

    Example::

        import polars as pl
        from acquirium.Driver import PollingIngestDriver

        class MyDriver(PollingIngestDriver):
            def setup(self):
                self.source_id = "my-source"
                self.aq.register_datasource(self.source_id)
                self.aq.register_streams([{"source_id": self.source_id, "ref_name": "temp"}])

            def collect(self):
                return pl.DataFrame({"ts": [datetime.now(timezone.utc)],
                                     "ref_name": ["temp"],
                                     "value": [21.5]})

    Invoke with::

        acquirium run my_module:MyDriver --config acquirium.toml
    """

    # Default datasource for single-source ingest drivers. Multi-source drivers
    # may omit this if every observation row carries a source_id column.
    source_id: str

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        self.aq = aq
        # Full parsed TOML dict so drivers can read their own config sections.
        self.config = config
        # Persistent state storage
        self.state = self._init_state(config)

    def reference_uri(self, ref_name: str) -> URIRef:
        """Return the canonical reference URI for ``self.source_id``/``ref_name``."""
        return compute_ref_uri(self.source_id, ref_name)

    def config_dir(self) -> Path:
        """Return the directory containing the loaded config file, if known."""
        return Path(self.config.get("__config_dir", Path.cwd()))

    def _init_state(self, config: dict) -> DriverState:
        """Initialize persistent state for this driver.

        Derives a unique identifier from the driver config:
        - Uses `driver_id` if provided in config
        - Otherwise derives deterministically from the driver spec

        Args:
            config: The full configuration dictionary.

        Returns:
            A DriverState instance backed by a JSON file.
        """
        # Get driver spec from config (used for deriving identifier)
        driver_section = config.get("driver", {})
        driver_id = driver_section.get("driver_id")

        if driver_id:
            # Use explicit driver_id if provided (sanitize for filesystem)
            identifier = _sanitize_filename(str(driver_id))
        else:
            # Derive from spec hash for stability
            spec = driver_section.get("spec", "")
            if spec:
                # Hash the spec to create a stable identifier
                spec_hash = hashlib.sha256(spec.encode()).hexdigest()[:16]
                # Also include class name for readability
                class_name = self.__class__.__name__
                identifier = f"{_sanitize_filename(class_name)}_{spec_hash}"
            else:
                # Fallback to class name only
                identifier = _sanitize_filename(self.__class__.__name__)

        state_file = self.config_dir() / ".acquirium" / "drivers" / f"{identifier}.json"
        return DriverState(state_file)

    @abstractmethod
    def setup(self) -> None:
        """One-time initialisation: register_datasource, insert RDF, etc.

        Called once before ticking starts.
        """

    @abstractmethod
    def tick(self) -> None:
        """Single driver iteration.

        Polling drivers usually inherit ``PollingIngestDriver`` and implement
        ``collect()`` instead. Push/event drivers can inherit
        ``EventIngestDriver`` and call ``insert_observations()`` when data
        arrives.
        """

    def on_graph_change(self) -> None:
        """Called by the CLI when the server's graph version advances.

        Override to react to graph mutations (e.g. re-query for new streams).
        Default is a no-op. Never called during setup() — use setup() for
        initial graph queries. Only fired on subsequent changes.
        """

    def stop(self) -> None:
        """Optional cleanup called on shutdown (Ctrl-C or SIGTERM).

        Default is a no-op.  Override to close file ref URIs, flush buffers, etc.
        """


def _sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename.

    Replaces unsafe characters with underscores.
    """
    unsafe = "<>:" "|?*\\"
    result = name
    for char in unsafe:
        result = result.replace(char, "_")
    # Also replace spaces and other problematic chars
    result = result.strip().replace(" ", "_")
    return result


class IngestDriver(Driver):
    """Driver base for sources that emit canonical timeseries observations.

    Observations are represented as a Polars DataFrame with required columns
    ``ts``, ``ref_name``, and ``value``. Each stream must have a ``source_id``:
    either include a ``source_id`` column for multi-source frames or set
    ``self.source_id`` as the default for single-source drivers. Drivers must
    register each stream, including its value kind, before inserting rows.
    """

    def insert_observations(self, observations: "pl.DataFrame | None") -> dict[str, Any]:
        import polars as pl

        with timed_debug(logger, "%s.normalize_observations", self.__class__.__name__):
            df = self.normalize_observations(observations)
        if df.is_empty():
            logger.debug("%s.insert_observations: empty after normalize, skipping", self.__class__.__name__)
            return {"ok": True, "rows_inserted": 0}

        if "source_id" not in df.columns:
            logger.debug(
                "%s.insert_observations source=%s rows=%d",
                self.__class__.__name__, self.source_id, len(df),
            )
            with timed_debug(logger, "%s.aq.insert_timeseries_arrow source=%s rows=%d",
                             self.__class__.__name__, self.source_id, len(df)):
                result = self.aq.insert_timeseries_arrow(self.source_id, df.to_arrow())
            return self._coerce_insert_result(result, len(df))

        total = 0
        for source_id, source_df in df.partition_by("source_id", as_dict=True).items():
            source = source_id[0] if isinstance(source_id, tuple) else source_id
            payload = source_df.drop("source_id")
            logger.debug(
                "%s.insert_observations source=%s rows=%d (multi-source)",
                self.__class__.__name__, source, len(payload),
            )
            with timed_debug(logger, "%s.aq.insert_timeseries_arrow source=%s rows=%d",
                             self.__class__.__name__, source, len(payload)):
                result = self.aq.insert_timeseries_arrow(str(source), payload.to_arrow())
            total += self._coerce_insert_result(result, len(payload))["rows_inserted"]
        return {"ok": True, "rows_inserted": total}

    def normalize_observations(self, observations: "pl.DataFrame | None") -> "pl.DataFrame":
        import polars as pl

        schema = {
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
        }
        if observations is None:
            return pl.DataFrame(schema=schema)
        if observations.is_empty():
            if "source_id" in observations.columns:
                schema = {"source_id": pl.Utf8, **schema}
            return pl.DataFrame(schema=schema)

        missing = {"ts", "ref_name", "value"} - set(observations.columns)
        if missing:
            raise ValueError(
                "Observation frames must include columns ts, ref_name, value; "
                f"missing {sorted(missing)}"
            )

        columns = ["ts", "ref_name", "value"]
        if "source_id" in observations.columns:
            columns.insert(0, "source_id")
        df = observations.select(columns).drop_nulls(subset=["ts", "ref_name"])

        exprs = [
            self.normalize_timestamps(df["ts"]).alias("ts"),
            pl.col("ref_name").cast(pl.Utf8),
            _cast_value_to_utf8(df["value"]),
        ]
        if "source_id" in df.columns:
            exprs.insert(0, pl.col("source_id").cast(pl.Utf8))
        return df.with_columns(exprs).drop_nulls(subset=["ts"])

    def normalize_timestamps(self, col: "pl.Series", date_format: str | None = None) -> "pl.Series":
        import polars as pl

        dtype = col.dtype
        if dtype == pl.Date:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        if dtype in (pl.String, pl.Utf8):
            return self._parse_string_timestamps(col, date_format=date_format)

        tz = getattr(dtype, "time_zone", None)
        if tz is None:
            return col.dt.replace_time_zone("UTC")
        if tz != "UTC":
            return col.dt.convert_time_zone("UTC")
        return col

    def _parse_string_timestamps(
        self, col: "pl.Series", date_format: str | None = None
    ) -> "pl.Series":
        import polars as pl

        non_null = col.drop_nulls().len()
        if non_null == 0:
            return col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")

        candidates: list[str | None] = (
            ([date_format] if date_format else []) + [None]
        )
        best: pl.Series | None = None
        best_nulls = non_null + 1
        for fmt in candidates:
            try:
                parsed = col.str.to_datetime(format=fmt, strict=False)
                nulls = parsed.null_count()
                if nulls < best_nulls:
                    best, best_nulls = parsed, nulls
                if best_nulls == 0:
                    break
            except Exception:
                continue
        if best is None:
            best = col.str.to_datetime(format=None, strict=False)

        tz = getattr(best.dtype, "time_zone", None)
        return best.dt.replace_time_zone("UTC") if tz is None else best.dt.convert_time_zone("UTC")

    def _coerce_insert_result(self, result: Any, fallback_rows: int) -> dict[str, Any]:
        if isinstance(result, dict):
            return {
                "ok": bool(result.get("ok", True)),
                "rows_inserted": int(result.get("rows_inserted", fallback_rows)),
            }
        return {"ok": True, "rows_inserted": int(result)}


class PollingIngestDriver(IngestDriver):
    """Ingest driver whose data is pulled by the runner on each tick."""

    def tick(self) -> None:
        self.insert_observations(self.collect())

    @abstractmethod
    def collect(self) -> "pl.DataFrame":
        """Return canonical observations for the current tick."""


class EventIngestDriver(IngestDriver):
    """Ingest driver whose data arrives asynchronously and is pushed by callbacks."""

    def tick(self) -> None:
        return None
