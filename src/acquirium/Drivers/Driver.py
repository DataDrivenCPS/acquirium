from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rdflib import URIRef

from acquirium.DriverState import DriverState
from acquirium.internals.models import compute_ref_uri
from acquirium.Storage.values import assign_stream_value_kind
from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.driver")


class UndeclaredStreamError(ValueError):
    """Raised when observations refer to streams the driver did not declare."""


class DriverBufferFull(RuntimeError):
    """Raised when an ingest driver's in-memory observation buffer is full."""


@dataclass(frozen=True)
class FileBatch:
    """Observations read from one file and the checkpoint after those rows."""

    observations: "pl.DataFrame | None"
    next_cursor: Any

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

    The server's driver runner calls :meth:`setup` once at startup, then calls
    :meth:`tick` repeatedly, sleeping for ``interval`` seconds between calls.
    Override :meth:`stop` for cleanup when the driver is stopped.

    Example::

        from acquirium.Drivers.Driver import PollingIngestDriver

        class MyDriver(PollingIngestDriver):
            def setup(self):
                self.source_id = "my-source"
                self.declare("temp", value_kind="numeric")

            def read(self):
                self.add("temp", 21.5)

    Run it by listing it under ``[[drivers]]`` in acquirium.toml::

        [[drivers]]
        spec = "my_module:MyDriver"

    then ``acquirium server --config acquirium.toml``, or push it to a running
    server with ``acquirium driver start acquirium.toml``.
    """

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        self.aq = aq
        # Full parsed TOML dict so drivers can read their own config sections.
        self.config = config
        # Persistent state storage
        self.state = self._init_state(config)
        self._source_id: str | None = None

    @property
    def source_id(self) -> str:
        """Default datasource for single-source ingest drivers.

        Multi-source drivers may leave this unset if every observation row
        carries a ``source_id`` column.
        """
        if self._source_id is None:
            raise AttributeError(
                f"{type(self).__name__}.source_id is not set yet. Set "
                "`source_id` under the driver's config, or assign "
                "`self.source_id` in setup() when it has to be derived at "
                "runtime — it is needed by insert_graph, declare, "
                "stream declarations and other source-scoped helpers."
            )
        return self._source_id

    @source_id.setter
    def source_id(self, value: str) -> None:
        self._source_id = value

    def reference_uri(self, ref_name: str) -> URIRef:
        """Return the canonical reference URI for ``self.source_id``/``ref_name``."""
        return compute_ref_uri(self.source_id, ref_name)

    def insert_graph(self, rdf_graph: str, *, format: str = "turtle", replace: bool = False) -> None:
        """Write RDF to this driver's source-owned graph.

        Driver code must not pass a source id manually: this helper always
        uses ``self.source_id``. Multi-source drivers should use the general
        client API only at the narrow point where they select an owner.
        """
        self.aq.insert_graph(
            rdf_graph,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def insert_graph_file(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        replace: bool = False,
    ) -> None:
        """Read an RDF file into this driver's source-owned graph."""
        self.aq.insert_graph_file(
            path,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def sparql_update(self, update: str) -> dict[str, Any]:
        """Apply an update only to this driver's source-owned graph."""
        return self.aq.sparql_update(update, source_id=self.source_id)

    def config_dir(self) -> Path:
        """Return the directory containing the loaded config file, if known."""
        return Path(self.config.get("__config_dir", Path.cwd()))

    def data_dir(self) -> Path:
        """Return the resolved Acquirium data directory.

        Resolved the same way the server resolves its data dir so driver state
        lands *inside* the data dir rather than in a stray ``.acquirium`` next
        to the config file: the ``ACQUIRIUM_DATA_DIR`` env var wins, then
        ``[server] data_dir`` from config (relative paths resolved against the
        config dir), falling back to ``<config_dir>/.acquirium``.
        """
        env_dir = os.getenv("ACQUIRIUM_DATA_DIR")
        if env_dir:
            return Path(env_dir)
        server_dir = self.config.get("server", {}).get("data_dir")
        if server_dir:
            path = Path(server_dir)
            return path if path.is_absolute() else (self.config_dir() / path).resolve()
        return self.config_dir() / ".acquirium"

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

        state_file = self.data_dir() / "drivers" / f"{identifier}.json"
        return DriverState(state_file)

    @abstractmethod
    def setup(self) -> None:
        """One-time initialisation: assign source identity, declare streams, etc.

        Called once before ticking starts.
        """

    @abstractmethod
    def tick(self) -> None:
        """Single driver iteration.

        Polling drivers inherit ``PollingIngestDriver`` and implement ``read``
        or ``collect``. Event drivers inherit ``EventIngestDriver`` and report
        callback values with ``add``.
        """

    def on_graph_change(self) -> None:
        """Called by the runner when the server's source generation advances.

        Override to react to graph mutations (e.g. re-query for new streams).
        Default is a no-op. Never called during setup() — use setup() for
        initial graph queries. Only fired on subsequent changes.
        """

    def stop(self) -> None:
        """Optional cleanup called on shutdown (Ctrl-C or SIGTERM).

        Default is a no-op. Override to quiesce producers and close resources;
        the framework flushes accepted observations after this method returns.
        """

    def _after_setup(self) -> None:
        """Framework hook run after :meth:`setup`; driver authors do not override it."""

    def _shutdown(self) -> None:
        """Framework hook for orderly shutdown; driver authors override :meth:`stop`."""
        self.stop()


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

    Most drivers should report readings one at a time with :meth:`add`; the
    buffered rows are inserted by :meth:`flush`, which the tick lifecycle calls
    for you. Drivers that already hold their data in bulk can instead pass a
    Polars DataFrame to :meth:`insert_observations` directly, with required
    columns ``ts``, ``ref_name``, and ``value``.

    Either way each stream must have a ``source_id``: pass one per observation
    (or include a ``source_id`` column) for multi-source drivers, or set
    ``self.source_id`` as the default for single-source drivers. Declare every
    stream before reporting rows; the platform performs registration and can
    infer an omitted value kind from the first meaningful values.
    """

    #: Hard in-memory limit. ``add`` rejects a new row once this many accepted
    #: rows are pending or in flight; accepted rows are never silently dropped.
    max_buffered_rows: int = 100_000

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        super().__init__(aq, config)
        # Event drivers add from broker callback threads, so the buffer is
        # guarded even though polling drivers only touch it from the tick.
        self._pending: list[tuple[str, datetime, str, Any]] = []
        self._pending_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._inflight_rows = 0
        self._declaration_lock = threading.Lock()
        self._declarations: dict[tuple[str, str], dict[str, Any]] = {}
        self._registered: set[tuple[str, str]] = set()
        self._kinded: set[tuple[str, str]] = set()
        self._registered_sources: set[str] = set()

    def declare(
        self,
        ref_name: str,
        *,
        source_id: str | None = None,
        value_kind: str | None = None,
        point_uri: str | None = None,
        label: str | None = None,
        unit: str | URIRef | None = None,
        quantity_kind: str | URIRef | None = None,
        medium: str | URIRef | None = None,
        substance: str | URIRef | None = None,
        data_source: str | URIRef | None = None,
        properties: dict[Any, Any] | None = None,
        allow_unit_mismatch: bool = False,
    ) -> None:
        """Declare a stream's identity and semantics.

        Idempotent and batched: call it for every stream every time you read a
        source, and it costs nothing after the first. The declaration is written
        to the graph just before the next insert, so streams always exist before
        their observations do.

        ``unit``, ``quantity_kind``, ``medium`` and ``substance`` accept free
        text — ``unit="gal/min"``, ``substance="chlorine"`` — resolved
        server-side, so a driver never has to look up a URI. They are recorded
        on the stream's external reference, not on ``point_uri``.

        ``value_kind`` is inferred from the first observed values when omitted.
        Repeating an identical declaration is a no-op; changing any metadata
        for an existing identity raises an error, even after registration.

        When ``point_uri`` names a point that already carries its own
        semantics, the two are reconciled at registration: an agreeing or
        one-sided pair is fine, a convertible unit difference is converted at
        read time into the point's unit, and an irreconcilable one is refused.
        Pass ``allow_unit_mismatch=True`` to register such a stream anyway —
        reads then return the point's unit, unconverted, and warn. It covers
        ``unit`` only; a ``medium`` or ``substance`` disagreement has no
        automated remedy and always raises.

        ``source_id`` defaults to the driver's own. Pass it only for drivers
        spanning several datasources, and then pass it consistently: streams are
        identified by the ``(source_id, ref_name)`` pair, matching how their
        observations are identified.
        """
        source = source_id if source_id is not None else self.source_id
        name = str(ref_name)
        if not source or not name:
            raise ValueError("stream declarations require non-empty source_id and ref_name")
        metadata = {
            key: value
            for key, value in {
                "value_kind": value_kind,
                "point_uri": point_uri,
                "label": label,
                "unit": unit,
                "quantity_kind": quantity_kind,
                "medium": medium,
                "substance": substance,
                "data_source": data_source,
                "properties": properties,
            }.items()
            if value is not None
        }
        if allow_unit_mismatch:
            metadata["allow_unit_mismatch"] = True
        key = (source, name)
        with self._declaration_lock:
            previous = self._declarations.get(key)
            if previous is None:
                self._declarations[key] = metadata
                return
            if previous != metadata:
                raise ValueError(
                    f"conflicting declaration for stream {key!r}: "
                    f"was {previous!r}, now {metadata!r}"
                )

    def is_declared(self, ref_name: str, *, source_id: str | None = None) -> bool:
        """Return whether this driver has declared the effective stream identity."""
        source = source_id if source_id is not None else self.source_id
        with self._declaration_lock:
            return (source, str(ref_name)) in self._declarations

    def declare_stream(self, ref_name: str) -> None:
        """Declare one stream discovered while reading a source.

        The tabular ingest drivers call this for every column they find, so a
        subclass can attach unit, quantity kind, medium and substance from a
        mapping file without reimplementing ``read``. The default declares
        identity only, which is all a generic reader can know.
        """
        self.declare(ref_name)

    def register_declared(self, observations: "pl.DataFrame | None" = None) -> None:
        """Write pending declarations, inferring value kinds from *observations*.

        Every observed stream must already have been declared. When a
        declaration omitted ``value_kind``, its first values determine it.
        """
        import polars as pl

        observed: dict[tuple[str, str], list[Any]] = {}
        if observations is not None and not observations.is_empty():
            multi = "source_id" in observations.columns
            group = ["source_id", "ref_name"] if multi else ["ref_name"]
            by_stream = observations.group_by(group).agg(pl.col("value"))
            for row in by_stream.iter_rows(named=True):
                source = str(row["source_id"]) if multi else self.source_id
                observed[(source, str(row["ref_name"]))] = row["value"]

        with self._declaration_lock:
            missing = sorted(set(observed) - set(self._declarations))
            if missing:
                raise UndeclaredStreamError(
                    "observations contain undeclared streams: "
                    + ", ".join(repr(key) for key in missing)
                )
            specs = {
                key: dict(meta)
                for key, meta in self._declarations.items()
                if key not in self._registered
            }
            inferred: set[tuple[str, str]] = set()
            for key, values in observed.items():
                meta = self._declarations[key]
                if key in self._kinded or "value_kind" in meta:
                    continue
                meaningful = [
                    value for value in values
                    if value is not None and not (isinstance(value, str) and not value.strip())
                ]
                if not meaningful:
                    continue
                specs.setdefault(key, dict(meta))["value_kind"] = assign_stream_value_kind(meaningful)
                inferred.add(key)
            sources = sorted({source for source, _ in specs} - self._registered_sources)

        for source in sources:
            self.aq.register_datasource(source)
        if specs:
            self.aq.register_streams([
                {"source_id": source, "ref_name": name, **meta}
                for (source, name), meta in specs.items()
            ])
        with self._declaration_lock:
            self._registered_sources.update(sources)
            self._registered.update(specs)
            self._kinded.update(inferred)
            self._kinded.update(key for key, meta in specs.items() if "value_kind" in meta)

    def add(
        self,
        ref_name: str,
        value: Any,
        ts: datetime | None = None,
        *,
        source_id: str | None = None,
    ) -> None:
        """Buffer a single observation for insertion.

        ``ts`` defaults to the current UTC time; naive datetimes are read as
        UTC. ``source_id`` defaults to ``self.source_id`` when this method is
        called and only needs passing by drivers spanning several datasources.

        Buffered rows are inserted by :meth:`flush` at the end of each tick.
        """
        source = source_id if source_id is not None else self.source_id
        key = (source, str(ref_name))
        with self._declaration_lock:
            if key not in self._declarations:
                raise UndeclaredStreamError(f"observation stream {key!r} was not declared")

        if ts is None:
            ts = datetime.now(timezone.utc)
        elif ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        with self._pending_lock:
            if len(self._pending) + self._inflight_rows >= self.max_buffered_rows:
                raise DriverBufferFull(
                    f"{type(self).__name__} buffer is full "
                    f"(max_buffered_rows={self.max_buffered_rows})"
                )
            self._pending.append((source, ts, str(ref_name), value))

    def flush(self) -> dict[str, Any]:
        """Insert everything buffered by :meth:`add` and clear the buffer.

        Called for you at the end of every tick. On insert failure the rows are
        put back and the exception propagates; they are not retried until the
        next tick, so a failing server is polled at the driver's interval rather
        than on every :meth:`add`.
        """
        with self._flush_lock:
            with self._pending_lock:
                if not self._pending:
                    return {"ok": True, "rows_inserted": 0}
                rows, self._pending = self._pending, []
                self._inflight_rows = len(rows)

            frame = self._pending_frame(rows)
            try:
                result = self.insert_observations(frame)
            except Exception:
                with self._pending_lock:
                    self._pending[:0] = rows
                    self._inflight_rows = 0
                raise

            with self._pending_lock:
                self._inflight_rows = 0
            return result

    def _pending_frame(self, rows: list[tuple[str, datetime, str, Any]]) -> "pl.DataFrame":
        import polars as pl

        return pl.DataFrame({
            "source_id": pl.Series("source_id", [row[0] for row in rows], dtype=pl.Utf8),
            "ts": pl.Series("ts", [row[1] for row in rows]),
            "ref_name": pl.Series("ref_name", [row[2] for row in rows], dtype=pl.Utf8),
            "value": pl.Series("value", [row[3] for row in rows], dtype=pl.Object),
        })

    def insert_observations(self, observations: "pl.DataFrame | None") -> dict[str, Any]:
        import polars as pl

        with timed_debug(logger, "%s.normalize_observations", self.__class__.__name__):
            df = self.normalize_observations(observations)
        if df.is_empty():
            logger.debug("%s.insert_observations: empty after normalize, skipping", self.__class__.__name__)
            return {"ok": True, "rows_inserted": 0}

        self.register_declared(df)

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
            self.to_timestamp(df["ts"]).alias("ts"),
            pl.col("ref_name").cast(pl.Utf8),
            _cast_value_to_utf8(df["value"]),
        ]
        if "source_id" in df.columns:
            exprs.insert(0, pl.col("source_id").cast(pl.Utf8))
        return df.with_columns(exprs).drop_nulls(subset=["ts"])

    def to_timestamp(
        self,
        date_or_timestamp: "pl.Series",
        time: "pl.Series | None" = None,
        *,
        date_format: str | None = None,
        timezone: str = "UTC",
        day_first: bool = False,
    ) -> "pl.Series":
        """Convert source timestamp columns using the shared driver helper."""
        from acquirium.Drivers.tabular import to_timestamp

        return to_timestamp(
            date_or_timestamp,
            time,
            date_format=date_format,
            timezone=timezone,
            day_first=day_first,
        )

    def _coerce_insert_result(self, result: Any, fallback_rows: int) -> dict[str, Any]:
        if isinstance(result, dict):
            coerced = {
                "ok": bool(result.get("ok", True)),
                "rows_inserted": int(result.get("rows_inserted", fallback_rows)),
            }
            if not coerced["ok"]:
                raise RuntimeError(f"observation insertion reported failure: {result!r}")
            return coerced
        return {"ok": True, "rows_inserted": int(result)}

    def _after_setup(self) -> None:
        self.register_declared()

    def _shutdown(self) -> None:
        stop_error: BaseException | None = None
        try:
            self.stop()
        except BaseException as exc:
            stop_error = exc
        try:
            self.flush()
        except Exception:
            with self._pending_lock:
                unsent = len(self._pending) + self._inflight_rows
            logger.exception("%s: final flush failed with %d unsent row(s)", type(self).__name__, unsent)
            raise
        if stop_error is not None:
            raise stop_error


class PollingIngestDriver(IngestDriver):
    """Ingest driver whose data is pulled by the runner on each tick.

    Implement :meth:`read` and report each value with ``self.add(...)``::

        class MyDriver(PollingIngestDriver):
            def read(self):
                self.add("temp", read_sensor())

    Drivers that already hold a tick's worth of data in bulk — a file read, a
    model solve — can implement :meth:`collect` instead and return a frame.
    """

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        super().__init__(aq, config)
        cls = type(self)
        if cls.read is PollingIngestDriver.read and cls.collect is PollingIngestDriver.collect:
            raise TypeError(
                f"{cls.__name__} must implement read() (reporting values with "
                "self.add) or collect() (returning an observation frame)"
            )

    def tick(self) -> None:
        # Retry any batch retained from a previous failed flush before sampling
        # again; a full retry backlog must not prevent the driver recovering.
        self.flush()
        self.read()
        observations = self.collect()
        if observations is not None:
            self.insert_observations(observations)
        self.flush()

    def read(self) -> None:
        """Sample the source, reporting each value with ``self.add(...)``."""

    def collect(self) -> "pl.DataFrame | None":
        """Return this tick's observations in bulk, or ``None`` when using :meth:`read`."""
        return None


class FileIngestDriver(IngestDriver):
    """Ingest driver that pages through files appearing in a watched directory.

    Configure ``source_id``, ``watch_dir``, and ``glob``, then implement
    :meth:`read`. The framework owns discovery, per-file cursors persisted
    across restarts, stream registration, insertion, and error isolation so one
    unreadable file cannot stall the rest.

    Example::

        class MyDriver(FileIngestDriver):
            def read(self, path, cursor):
                df = pl.read_csv(path, skip_rows_after_header=cursor or 0)
                for column in df.columns:
                    self.declare(column)
                observations = df.unpivot(index="ts", variable_name="ref_name",
                                          value_name="value")
                return FileBatch(observations, (cursor or 0) + len(df))

    Required config keys: ``source_id``, ``watch_dir`` (resolved against the
    config directory), and ``glob`` (one pattern or a list).
    """

    def setup(self) -> None:
        """Adopt and validate the required file-source configuration."""
        cfg = self.config.get("driver", {})
        source_id = self._source_id or cfg.get("source_id")
        if not source_id:
            raise ValueError("file ingest drivers require driver.source_id")
        self.source_id = str(source_id)

        configured_watch_dir = cfg.get("watch_dir")
        if not isinstance(configured_watch_dir, str) or not configured_watch_dir.strip():
            raise ValueError("file ingest drivers require driver.watch_dir")
        watch_dir = Path(configured_watch_dir)
        self.watch_dir = (
            watch_dir if watch_dir.is_absolute()
            else (self.config_dir() / watch_dir).resolve()
        )

        configured_glob = cfg.get("glob")
        if isinstance(configured_glob, str):
            patterns = (configured_glob,)
        elif isinstance(configured_glob, (list, tuple)):
            patterns = tuple(configured_glob)
        else:
            raise ValueError("file ingest drivers require driver.glob as a string or list")
        if not patterns or any(
            not isinstance(pattern, str) or not pattern.strip()
            for pattern in patterns
        ):
            raise ValueError("driver.glob patterns must be non-empty strings")
        self.file_patterns = patterns

    def read(self, path: Path, cursor: Any) -> FileBatch:
        """Return observations from *path* and the next persisted cursor.

        ``cursor`` is whatever this method returned for *path* last time, or
        ``None`` on first sight. It must be JSON-serializable; a row offset,
        byte position, or ISO timestamp are all suitable.

        Observations are a frame of ``ts``, ``ref_name``, ``value``. Return an
        unchanged cursor to signal there was nothing new.
        """
        raise NotImplementedError

    # ------------------------------------------------------------ framework

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        super().__init__(aq, config)
        self._cursors: dict[str, Any] = self.state.get("cursors", {})

    def tick(self) -> None:
        # Declarations made during setup() reach the graph even before any file
        # shows up, so a model's bindings are complete from the first tick.
        self.register_declared()

        for path in sorted({
            match
            for pattern in self.file_patterns
            for match in self.watch_dir.rglob(pattern)
            if match.is_file()
        }):
            key = str(path)
            cursor = self._cursors.get(key)
            try:
                batch = self.read(path, cursor)
                if not isinstance(batch, FileBatch):
                    raise TypeError(f"{type(self).__name__}.read() must return FileBatch")
                observations, next_cursor = batch.observations, batch.next_cursor
                json.dumps(next_cursor)
            except Exception:
                logger.exception("%s: failed to read %s", type(self).__name__, path.name)
                continue
            if next_cursor == cursor:
                continue

            if observations is not None and not observations.is_empty():
                result = self.insert_observations(observations)
                if not result.get("ok", False):
                    raise RuntimeError(f"observation insertion failed for {path.name}: {result!r}")
                logger.info(
                    "%s: %s — inserted %d observation(s)",
                    type(self).__name__, path.name, result.get("rows_inserted", 0),
                )

            self._cursors[key] = next_cursor
            self.state.set("cursors", self._cursors)


class EventIngestDriver(IngestDriver):
    """Ingest driver whose data arrives asynchronously and is pushed by callbacks.

    Callbacks report values with ``self.add(...)``; the buffer is flushed on
    each tick, so observations are batched at the driver's configured interval
    rather than inserted one round-trip at a time.
    """

    def tick(self) -> None:
        self.flush()
