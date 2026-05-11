from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdflib import URIRef

from acquirium.internals.models import compute_ref_uri

if TYPE_CHECKING:
    import polars as pl

    from acquirium.Client.acquirium import Acquirium
    from acquirium.DriverState import DriverState, WriteAheadLog, ExponentialBackoff

log = logging.getLogger(__name__)

# Connection errors from the requests library that indicate the server is
# unreachable (as opposed to a bad-data or auth error).
_CONNECTION_EXC_NAMES = frozenset({"ConnectionError", "Timeout", "ConnectTimeout", "ReadTimeout"})


def _is_connection_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a transient network/connection failure."""
    type_name = type(exc).__name__
    if type_name in _CONNECTION_EXC_NAMES:
        return True
    # requests exceptions live in requests.exceptions; check the MRO names
    return any(c.__name__ in _CONNECTION_EXC_NAMES for c in type(exc).__mro__)


def _cast_value_to_utf8(col: "pl.Series") -> "pl.Expr":
    """Return an expression that casts *col* to Utf8, handling pl.Object and NaN."""
    import math
    import polars as pl

    if col.dtype == pl.Object:
        return pl.col("value").map_elements(
            lambda v: None if (v is None or (isinstance(v, float) and math.isnan(v))) else str(v),
            return_dtype=pl.Utf8,
        )
    if col.dtype in (pl.Float32, pl.Float64):
        return pl.col("value").fill_nan(None).cast(pl.Utf8)
    return pl.col("value").cast(pl.Utf8)


def _sanitise_driver_id(spec: str) -> str:
    """Turn a driver spec or class name into a filesystem-safe identifier."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", spec).strip("_") or "driver"


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

    Each driver instance gets a local **state directory** at
    ``<state_dir>/<driver_id>/``.  The directory and a :class:`DriverState`
    key-value store are available immediately via ``self.state``.

    Example::

        import polars as pl
        from acquirium.Driver import PollingIngestDriver

        class MyDriver(PollingIngestDriver):
            def setup(self):
                self.source_id = "my-source"
                self.aq.register_datasource(self.source_id)
                self.aq.register_stream(source_id=self.source_id, ref_name="temp")

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

        # ------------------------------------------------------------------
        # Resolve state directory
        # ------------------------------------------------------------------
        driver_cfg = config.get("driver", {})

        # driver_id: user-explicit > injected by CLI/server > class name
        driver_id = (
            driver_cfg.get("driver_id")
            or _sanitise_driver_id(config.get("__driver_id", ""))
            or self.__class__.__name__
        )

        state_base = Path(driver_cfg.get("state_dir", "driver_state"))
        if not state_base.is_absolute():
            state_base = Path(config.get("__config_dir", Path.cwd())) / state_base
        self._driver_state_dir = state_base / driver_id
        self._driver_state_dir.mkdir(parents=True, exist_ok=True)

        from acquirium.DriverState import DriverState as _DriverState
        self.state: "DriverState" = _DriverState(self._driver_state_dir)

    def reference_uri(self, ref_name: str) -> URIRef:
        """Return the canonical reference URI for ``self.source_id``/``ref_name``."""
        return compute_ref_uri(self.source_id, ref_name)

    def config_dir(self) -> Path:
        """Return the directory containing the loaded config file, if known."""
        return Path(self.config.get("__config_dir", Path.cwd()))

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


class IngestDriver(Driver):
    """Driver base for sources that emit canonical timeseries observations.

    Observations are represented as a Polars DataFrame with required columns
    ``ts``, ``ref_name``, and ``value``. Each stream must have a ``source_id``:
    either include a ``source_id`` column for multi-source frames or set
    ``self.source_id`` as the default for single-source drivers. Drivers must
    register each stream, including its value kind, before inserting rows.

    **Reliable delivery**: when the server is unreachable, observations are
    buffered to a local write-ahead log (``<state_dir>/wal/``) and retransmitted
    automatically once the connection is restored.  Retransmission uses
    exponential backoff so that a prolonged outage does not result in a storm
    of reconnect attempts.
    """

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        super().__init__(aq, config)
        driver_cfg = config.get("driver", {})

        from acquirium.DriverState import WriteAheadLog as _WAL, ExponentialBackoff as _Backoff
        self._wal: "WriteAheadLog" = _WAL(self._driver_state_dir / "wal")
        self._backoff: "ExponentialBackoff" = _Backoff(
            base=float(driver_cfg.get("backoff_base", 2.0)),
            max_delay=float(driver_cfg.get("backoff_max_delay", 300.0)),
        )
        self._log = logging.getLogger(f"acquirium.driver.{self.__class__.__name__}")

    # ------------------------------------------------------------------
    # Public insertion API
    # ------------------------------------------------------------------

    def insert_observations(self, observations: "pl.DataFrame | None") -> dict[str, Any]:
        """Normalise and insert *observations*, buffering to WAL on failure.

        Returns a dict with at least ``{"ok": bool, "rows_inserted": int}``.
        When data is buffered instead of inserted, the response also contains
        ``"buffered": <row_count>``.
        """
        import polars as pl

        df = self.normalize_observations(observations)

        # --- drain WAL before attempting a live insert ---
        if not self._wal.is_empty():
            if self._backoff.ready():
                drained = self._drain_wal()
            else:
                drained = False

            if not drained:
                # Server still unreachable; buffer new data too.
                if not df.is_empty():
                    self._wal.append(self._to_wal_df(df))
                    self._log.debug("WAL: buffered %d rows (backoff %.1fs)", len(df), self._backoff.next_delay())
                return {"ok": True, "rows_inserted": 0, "buffered": len(df)}

        if df.is_empty():
            return {"ok": True, "rows_inserted": 0}

        # --- attempt live insert ---
        try:
            result = self._insert_df(df)
            self._backoff.record_success()
            return result
        except Exception as exc:
            if _is_connection_error(exc):
                self._backoff.record_failure()
                self._wal.append(self._to_wal_df(df))
                self._log.warning(
                    "Server unreachable (%s); buffered %d rows to WAL. Next retry in %.1fs.",
                    exc,
                    len(df),
                    self._backoff.next_delay(),
                )
                return {"ok": True, "rows_inserted": 0, "buffered": len(df)}
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _insert_df(self, df: "pl.DataFrame") -> dict[str, Any]:
        """Insert a normalised observation DataFrame, partitioning by source_id."""
        if "source_id" not in df.columns:
            result = self.aq.insert_timeseries_arrow(self.source_id, df.to_arrow())
            return self._coerce_insert_result(result, len(df))

        total = 0
        for source_id, source_df in df.partition_by("source_id", as_dict=True).items():
            source = source_id[0] if isinstance(source_id, tuple) else source_id
            payload = source_df.drop("source_id")
            result = self.aq.insert_timeseries_arrow(str(source), payload.to_arrow())
            total += self._coerce_insert_result(result, len(payload))["rows_inserted"]
        return {"ok": True, "rows_inserted": total}

    def _to_wal_df(self, df: "pl.DataFrame") -> "pl.DataFrame":
        """Ensure the DataFrame has a materialised ``source_id`` column for WAL storage."""
        import polars as pl

        if "source_id" not in df.columns:
            sid = getattr(self, "source_id", "unknown")
            return df.with_columns(pl.lit(sid).alias("source_id")).select(
                ["source_id", "ts", "ref_name", "value"]
            )
        return df.select(["source_id", "ts", "ref_name", "value"])

    def _drain_wal(self) -> bool:
        """Flush all pending WAL entries.

        Returns True if the WAL is now empty, False if a connection error
        prevented full delivery (backoff will have been updated).
        Non-connection errors on individual entries are logged and discarded to
        avoid an infinite retry loop.
        """
        for seq, wal_df in self._wal.pending():
            try:
                for key, source_df in wal_df.partition_by("source_id", as_dict=True).items():
                    source = key[0] if isinstance(key, tuple) else key
                    payload = source_df.select(["ts", "ref_name", "value"])
                    self.aq.insert_timeseries_polars(str(source), payload)
                self._wal.ack(seq)
                self._backoff.record_success()
                self._log.info("WAL: flushed entry %d", seq)
            except Exception as exc:
                if _is_connection_error(exc):
                    self._backoff.record_failure()
                    self._log.warning(
                        "WAL: flush failed (%s); will retry in %.1fs.", exc, self._backoff.next_delay()
                    )
                    return False
                # Non-connection error: discard entry to avoid infinite retry.
                self._log.warning(
                    "WAL: entry %d failed with non-connection error (%s); discarding.", seq, exc
                )
                self._wal.ack(seq)
        return True

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
