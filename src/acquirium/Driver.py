from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdflib import URIRef

from acquirium.internals.models import compute_ref_uri

if TYPE_CHECKING:
    import polars as pl

    from acquirium.Client.acquirium import Acquirium


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
                self.aq.register_stream(source_id=self.source_id, ref_name="temp")

            def collect(self):
                return pl.DataFrame({"ts": [datetime.now(timezone.utc)],
                                     "ref_name": ["temp"],
                                     "value": [21.5]})

    Invoke with::

        acquirium run my_module:MyDriver --config acquirium.toml
    """

    source_id: str

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        self.aq = aq
        # Full parsed TOML dict so drivers can read their own config sections.
        self.config = config

    def reference_uri(self, ref_name: str) -> URIRef:
        """Return the canonical Acquirium reference URI for ``ref_name``."""
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
    ``ts``, ``ref_name``, and ``value``. A ``source_id`` column is optional; if
    absent, ``self.source_id`` is used for the whole frame. Drivers must
    register each stream, including its value kind, before inserting rows.
    """

    def insert_observations(self, observations: "pl.DataFrame | None") -> dict[str, Any]:
        import polars as pl

        df = self.normalize_observations(observations)
        if df.is_empty():
            return {"ok": True, "rows_inserted": 0}

        if "source_id" not in df.columns:
            result = self.aq.insert_timeseries_polars(self.source_id, df)
            return self._coerce_insert_result(result, len(df))

        total = 0
        for source_id, source_df in df.partition_by("source_id", as_dict=True).items():
            source = source_id[0] if isinstance(source_id, tuple) else source_id
            payload = source_df.drop("source_id")
            result = self.aq.insert_timeseries_polars(str(source), payload)
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
