from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence, Callable, Optional, TYPE_CHECKING
import inspect

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

from rdflib import Graph as RDFGraph, URIRef, Literal
from rdflib.namespace import RDF, RDFS

import warnings

from acquirium.Client.explore.core import Query
from acquirium.Client.query import Q
from acquirium.Client.app_display import AppsResponse


def _dt_to_iso(v: "str | datetime | None") -> "str | None":
    if v is None:
        return None
    return v.isoformat() if isinstance(v, datetime) else v


def _add_triple(g: "RDFGraph", subj: "URIRef", pred: "URIRef", value: "str | URIRef | None") -> None:
    if value is None:
        return
    if isinstance(value, URIRef):
        g.add((subj, pred, value))
    elif "://" in str(value) or str(value).startswith("urn:"):
        g.add((subj, pred, URIRef(str(value))))
    else:
        g.add((subj, pred, Literal(value)))


def _coerce_resolved(
    resolved: dict[str, "str | None"], name: str, value: "Any",
) -> "str | URIRef | None":
    """Map a raw field value to its resolved URIRef.

    Passes ``None``/``URIRef`` through; warns and keeps the literal when
    plain text did not resolve.
    """
    if value is None:
        return None
    if isinstance(value, URIRef):
        return value
    uri = resolved.get(name)
    if uri is None:
        warnings.warn(
            f"Could not resolve {name!r} value {value!r} to a QUDT URI; "
            "storing as a plain literal.",
            stacklevel=3,
        )
        return value
    return URIRef(uri)


def _build_stream_triples(
    g: "RDFGraph", stream: dict, resolved: dict[str, "str | None"],
) -> None:
    """Write one stream's reference/point/metadata triples into ``g``.

    ``resolved`` is the precomputed ``field -> URI-or-None`` map for this
    stream's semantic fields (typically from ``resolve_point_metadata``);
    pass ``{}`` when there is nothing to resolve.
    """
    point_uri_raw = stream.get("point_uri")
    label = stream.get("label")
    source_id = stream.get("source_id")
    ref_name = stream.get("ref_name")
    ref_uri = None
    if ref_name is not None and source_id is not None:
        ref_uri = compute_ref_uri(source_id, ref_name)
        g.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
        g.add((ref_uri, ACQUIRIUM_REF_NAME,  Literal(ref_name)))
        # Only assert a value kind the caller supplied; a default would
        # contradict a later data-derived kind on the same reference node.
        if stream.get("value_kind") is not None:
            g.add((
                ref_uri,
                ACQUIRIUM_VALUE_KIND,
                Literal(normalize_value_kind(stream["value_kind"])),
            ))
        g.add((ref_uri, STORED_AT,           ACQUIRIUM_DB_URI))
        g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))

    if point_uri_raw is not None:
        subj = URIRef(str(point_uri_raw))
        g.add((subj, RDF.type, VIRTUAL_POINT))
        if label is not None:
            g.add((subj, RDFS.label, Literal(label)))
        _add_triple(g, subj, HAS_UNIT,          _coerce_resolved(resolved, "unit", stream.get("unit")))
        _add_triple(g, subj, HAS_QUANTITY_KIND, _coerce_resolved(resolved, "quantity_kind", stream.get("quantity_kind")))
        _add_triple(g, subj, HAS_MEDIUM,        _coerce_resolved(resolved, "medium", stream.get("medium")))
        _add_triple(g, subj, OF_SUBSTANCE,      _coerce_resolved(resolved, "substance", stream.get("substance")))
        _add_triple(g, subj, DATA_SOURCE, stream.get("data_source"))
        if ref_uri is not None:
            g.add((subj, HAS_EXTERNAL_REFERENCE, ref_uri))

    target = ref_uri if ref_uri is not None else (URIRef(str(point_uri_raw)) if point_uri_raw is not None else None)
    if target is not None:
        for pred, value in (stream.get("properties") or {}).items():
            _add_triple(g, target, pred, value)
from acquirium.Client.client import AcquiriumClient
from acquirium.Apps.base import App
from acquirium.internals.models import AppOutputSpec, AppSpec, compute_ref_uri
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_VALUE_KIND,
    STORED_AT,
    HAS_EXTERNAL_REFERENCE,
    HAS_MEDIUM,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    OF_SUBSTANCE,
    VIRTUAL_POINT,
    DATA_SOURCE,
)
from acquirium.Storage.values import normalize_value_kind

# Known point-metadata fields → the resolver ``kind`` each is resolved as.
# The field name is the semantic role, so callers supply no ``kind``.
POINT_FIELD_KINDS: dict[str, str] = {
    "unit": "unit",
    "quantity_kind": "quantity_kind",
    "medium": "substance",
    "substance": "substance",
}


@dataclass
class Acquirium:
    """
    High level entry point for Acquirium.

    This class is intended to be the **user-facing client API**. It connects to the server and exposes
    a small set of convenience methods that should feel natural for end users.

    """


    # ---------- construction ----------
    def __init__(
            self,
            server_url: str = "localhost",
            server_port: int = 8000,
            use_ssl: bool = False,
            lexicon_path: Optional[Path] = None,
            insert_batch_rows: int = 50_000,
            health_timeout: float | None = 60.0,
        ):
        """Connect to an Acquirium server.

        The constructor waits up to ``health_timeout`` seconds (default 60)
        for the server's ``/health`` to answer, retrying while it boots, and
        raises ``ConnectionError`` if it never does. Pass
        ``health_timeout=None`` (or 0) to skip the check.
        """
        if lexicon_path is not None:
            warnings.warn(
                "lexicon_path is deprecated and ignored. "
                "Text resolution now uses server-side embeddings via /resolve_text.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.client = AcquiriumClient(
            server_url=server_url,
            server_port=server_port,
            use_ssl=use_ssl,
        )
        self.insert_batch_rows = int(insert_batch_rows)
        if self.insert_batch_rows <= 0:
            raise ValueError("insert_batch_rows must be greater than zero")
        if health_timeout:
            self._wait_for_server(health_timeout)

    def _wait_for_server(self, timeout: float) -> None:
        import time as _time
        deadline = _time.monotonic() + timeout
        last_err: Exception | None = None
        while True:
            try:
                self.client.health(timeout=3.0)
                return
            except Exception as e:
                last_err = e
            if _time.monotonic() >= deadline:
                break
            _time.sleep(min(2.0, max(0.1, deadline - _time.monotonic())))
        raise ConnectionError(
            f"Acquirium server at {self.client.base_url} did not answer /health "
            f"within {timeout:.0f}s (last error: {last_err}). Is the server "
            f"running? Start it with: acquirium server --config <config.toml>"
        )

    # ------------------------------------------------------------------
    # GRAPH API
    # ------------------------------------------------------------------

    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace=True,
        *,
        source_id: str,
    ) -> None:
        """
        Insert RDF graph into an explicitly owned deployment data graph.

        The server refreshes the embedding index synchronously before
        responding, so inserted concepts are resolvable once this returns.

        Args:
            rdf_graph: RDF graph content as text.
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the selected graph. If False, appends to it.
            source_id: Data-graph owner. Use ``"plant"`` for the shared plant
                model, or a component's stable source ID.
        """
        self.client.insert_graph(
            rdf_graph,
            format=format,
            replace=replace,
            source_id=source_id,
        )

    def insert_graph_file(
        self,
        path: str | Path,
        format: str | None = None,
        replace: bool = True,
        *,
        source_id: str,
    ) -> None:
        """Read RDF from a file and insert it into an explicitly owned graph."""
        self.client.insert_graph_file(
            path,
            format=format,
            replace=replace,
            source_id=source_id,
        )

    def sparql_update(self, update: str, *, source_id: str) -> dict[str, Any]:
        """Execute a SPARQL update against one explicitly owned data graph.

        Components with a fixed owner use their ``self.sparql_update`` helper
        instead of passing ``source_id`` themselves.
        """
        return self.client.sparql_update(update, source_id=source_id)

    def query(self) -> Query:
        """Create a new empty Query (the explore builder) bound to this instance."""
        return Query(client=self.client)

    def explore(self) -> Query:
        """Alias of :meth:`query`."""
        return self.query()

    def find_entity(
        self,
        *,
        _class: Optional[str] = None,
        alias: Optional[str] = None,
        uri: str | URIRef | None = None,
    ) -> "Q":
        q = Q(client=self.client).find_entity(_class=_class, alias=alias, uri=uri)
        return q

    def find_all_data(self, *, _class: Optional[str] = None, uri: str | URIRef | None = None) -> "Q":
        q = Q(client=self.client).find_all_data(_class=_class, uri=uri)
        return q

    # ------------------------------------------------------------------
    # TIMESERIES API
    # ------------------------------------------------------------------

    def register_datasource(self, source_id: str) -> str:
        """Register a named datasource in the knowledge graph.

        The ``source_id`` is a user-provided string that scopes stream
        ``ref_name`` values so two sources with the same ``ref_name`` never
        produce colliding TimescaleDB keys.  Safe to call on every startup —
        the graph write is idempotent.

        Returns ``source_id``.
        """
        return self.client.register_datasource(source_id)

    def insert_timeseries(
        self,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        *,
        point_uri: Optional[str] = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        """Insert timeseries data for a single stream.

        Args:
            source_id: The registered datasource identifier.
            ref_name: The source-local stream identifier. Combined with
                ``source_id`` to derive the unique TimescaleDB storage key.
            rows: List of (timestamp, value) tuples.
            point_uri: Semantic URI of the measurement point. When provided,
                a ref_uri mapping is registered in the streams table.
            replace: If True, replaces any existing data for this stream.

        Returns:
            dict with ``{"ok": True, "rows_inserted": N}``.
        """
        return self.client.insert_timeseries(
            source_id=source_id,
            ref_name=ref_name,
            rows=rows,
            point_uri=point_uri,
            replace=replace,
        )

    def insert_timeseries_batch(
        self,
        source_id: str,
        streams: dict[str, list[tuple[datetime, Any]]],
    ) -> dict[str, Any]:
        """Insert timeseries data for multiple streams.

        Large inputs are split into bounded requests according to
        ``insert_batch_rows``. Drivers can call this method with their natural
        batch size; the Acquirium facade ref_uris transport/storage chunking.

        Args:
            source_id: The registered datasource identifier.
            streams: Mapping of ref_name → list of (timestamp, value) tuples.

        Returns:
            dict with ``{"ok": True, "rows_inserted": N}``.
        """
        total = 0
        chunk_count = 0
        for chunk in self._iter_insert_batches(streams):
            result = self.client.insert_timeseries_batch(
                source_id,
                chunk,
            )
            total += int(result.get("rows_inserted", 0))
            chunk_count += 1
        return {"ok": True, "rows_inserted": total, "batches": chunk_count}

    def insert_timeseries_arrow(self, source_id: str, table: "pa.Table") -> dict[str, Any]:
        """Insert a (ts, ref_name, value) Arrow table."""
        return self.client.insert_timeseries_arrow(source_id, table)

    @staticmethod
    def _json_safe_value(value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    def _iter_insert_batches(
        self,
        streams: dict[str, list[tuple[datetime, Any]]],
    ) -> Iterable[dict[str, list[tuple[datetime, Any]]]]:
        chunk: dict[str, list[tuple[datetime, Any]]] = {}
        chunk_rows = 0

        for stream, rows in streams.items():
            start = 0
            while start < len(rows):
                capacity = self.insert_batch_rows - chunk_rows
                end = min(start + capacity, len(rows))
                if end > start:
                    chunk[stream] = rows[start:end]
                    chunk_rows += end - start
                    start = end

                if chunk_rows >= self.insert_batch_rows:
                    yield chunk
                    chunk = {}
                    chunk_rows = 0

        if chunk:
            yield chunk

    def resolve_point_metadata(
        self, fields: dict[str, Any], min_score: float = 0.6
    ) -> dict[str, str | None]:
        """Resolve the semantic metadata of a single point/property.

        ``fields`` maps known semantic field names to a raw value::

            aq.resolve_point_metadata({
                "unit": "gal/min",
                "quantity_kind": "flow rate",
                "medium": "water",
            })
            # -> {"unit": "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            #     "quantity_kind": ".../quantitykind/VolumeFlowRate",
            #     "medium": "urn:nawi-water-ontology#Water"}

        The field name *is* the role — no ``kind`` to supply, no label, no
        tuple. Recognised fields (``unit``, ``quantity_kind``, ``medium``,
        ``substance``) resolve against their vocabulary; any other field is
        resolved across all kinds. Related fields reinforce each other (a
        quantity kind disambiguates an ambiguous unit and vice versa).
        Values that already look like URIs / ``URIRef`` / ``None`` pass
        through. Returns ``{field: uri-or-None}``.

        This is the preferred entry point for drivers and stream
        registration. For arbitrary labels and explicit kinds use
        :meth:`AcquiriumClient.resolve` directly.
        """
        record = {
            name: (value, POINT_FIELD_KINDS.get(name))
            for name, value in fields.items()
        }
        try:
            return self.client.resolve(record, min_score=min_score)
        except Exception:
            return {name: None for name in record}


    def graph_version(self) -> int:
        """Return the server's current source-data generation."""
        return self.client.graph_version()

    def graph_status(self) -> dict[str, int | bool]:
        """Return source and derived-query cache generations from the server."""
        return self.client.graph_status()

    def validate_graph(self) -> dict[str, str | bool]:
        """Validate all registered deployment data against ontology shapes."""
        return self.client.validate_graph()

    def reference_uri(self, source_id: str, ref_name: str) -> URIRef:
        """Return the canonical Acquirium reference URI for ``(source_id, ref_name)``."""
        return compute_ref_uri(source_id, ref_name)

    def register_streams(
        self,
        streams: Iterable[dict[str, Any]],
    ) -> None:
        """Declare one or more streams' semantic metadata in one graph insert.

        Each stream item is a dictionary with these commonly used keys:

        - ``point_uri``: optional semantic URI. When provided, a point node is
          created and linked to the external reference. When omitted, only the
          external reference node is written.
        - ``source_id`` and ``ref_name``: source-local stream identity. When
          both are present, Acquirium mints the canonical reference URI and
          writes ``acq:sourceId``, ``acq:refName``, and ``ref:storedAt`` on it.
        - ``label``: optional ``rdfs:label`` written on ``point_uri``.
        - ``data_source``: optional datasource marker written on the point.
        - ``properties``: optional mapping of predicate URIRefs to values,
          written on the reference node (or ``point_uri`` when no ref node exists).

        Plain strings for ``unit``/``quantity_kind``/``medium``/``substance``
        are resolved jointly per stream via :meth:`resolve_point_metadata`;
        URI / URIRef / None values pass through.

        Drivers should still insert rows with ``insert_timeseries_arrow`` using
        the same ``source_id`` and source-local ``ref_name``. Acquirium resolves
        those inserts to the same canonical reference URI internally.
        """
        graphs: dict[str, RDFGraph] = {}
        for stream in streams:
            source_id = stream.get("source_id")
            if not isinstance(source_id, str) or not source_id:
                raise ValueError("each stream registration requires a non-empty source_id")
            graph = graphs.setdefault(source_id, RDFGraph())
            meta = {
                f: stream.get(f)
                for f in POINT_FIELD_KINDS
                if stream.get(f) is not None
            }
            resolved = self.resolve_point_metadata(meta) if meta else {}
            _build_stream_triples(graph, stream, resolved)
        for source_id, graph in graphs.items():
            if len(graph):
                self.client.insert_graph(
                    graph.serialize(format="turtle"),
                    format="turtle",
                    replace=False,
                    source_id=source_id,
                )

    # ------------------------------------------------------------------
    # ACQUIRIUM APPS API
    # ------------------------------------------------------------------

    def register_app(
        self,
        app: App,
        *,
        app_type: str | None = None,
        outputs: list[AppOutputSpec | dict[str, Any]] | None = None,
        depends_on: list[str] | None = None,
        resolve_dependencies: bool = True,
        queries: dict[str, Query] | None = None,
        params: dict[str, Any] | None = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        """Register an Acquirium App with the server.

        If an app with the same name is already registered, the server rejects
        the request unless ``replace=True``, which gracefully tears down the
        existing app (stopping it and cleaning up its graph registration)
        before registering this one.

        ``params`` are stored with the app and passed to ``build_app`` via
        ``ctx.params`` during the (one-time) build phase, so build-time
        configuration — training windows, thresholds — lives with the
        registration rather than the run.
        """
        query_bundle = queries if queries is not None else app.build_query(self)
        if not isinstance(query_bundle, dict):
            # a single Query (or legacy Q) builder
            query_bundle = {"default": query_bundle}

        query_specs = {name: q.to_dict() for name, q in query_bundle.items()}

        deps = depends_on or []
        if not depends_on and resolve_dependencies:
            dep_set: set[str] = set()
            for q in query_bundle.values():
                dep_set.update(q.resolved_nodes())
            deps = sorted(dep_set)

        output_specs: list[AppOutputSpec] = []
        output_items = outputs if outputs is not None else list(getattr(app, "outputs", []) or [])
        for item in output_items:
            if isinstance(item, AppOutputSpec):
                spec_item = item
            elif isinstance(item, dict):
                spec_item = AppOutputSpec(**item)
            else:
                raise TypeError("outputs must be AppOutputSpec or dict")
            output_specs.append(spec_item)

        # The app's Python source is shipped to the server so its AppRunner
        # actor can load and run the class. Prefer an explicit override on the
        # app; otherwise read the class's defining module file.
        source_code = getattr(app, "source_code", None)
        entry_file = getattr(app, "entry_file", None)
        if source_code is None:
            try:
                src_path = inspect.getsourcefile(app.__class__)
                if src_path:
                    source_code = Path(src_path).read_text()
                    if entry_file is None:
                        entry_file = Path(src_path).name
            except Exception:
                source_code = None

        spec = AppSpec(
            name=app.name,
            version=getattr(app, "version", "0.0"),
            app_type=app_type or getattr(app, "app_type", "soft_sensor"),
            app_class=app.__class__.__name__,
            source_code=source_code,
            entry_file=entry_file,
            queries=query_specs,
            outputs=output_specs,
            depends_on=deps,
            params=params or {},
        )
        return self.client.register_app(spec, replace=replace)

    def delete_app(self, app_id: str) -> dict[str, Any]:
        """Gracefully delete a registered app (stop it, clean up its graph
        registration, and remove its persisted source)."""
        return AppsResponse(self.client.delete_app(app_id))

    def run_app(
        self,
        app_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        params: dict[str, Any] | None = None,
        keep_alive: bool = False,
        interval: float = 10.0,
    ) -> dict[str, Any]:
        """Trigger an app execution in its own container via the server."""
        return AppsResponse(self.client.run_app(
            app_id,
            start=start,
            end=end,
            params=params or {},
            keep_alive=keep_alive,
            interval=interval,
        ))

    def stop_app(self, *, app_id: str) -> dict[str, Any]:
        """Stop an app's keep-alive loop."""
        return AppsResponse(self.client.stop_app(app_id=app_id))

    def list_app_runs(self, *, app_id: str | None = None) -> dict[str, Any]:
        """List registered apps, or one app's build/run status if app_id is given."""
        return AppsResponse(self.client.list_app_runs(app_id=app_id))

    def generate_grafana_dashboard(self, grafana_server, api_key):
        return self.client.generate_grafana_dashboard(grafana_server, api_key)

    # ------------------------------------------------------------------
    # LOGGING API (plant-level convenience methods)
    # ------------------------------------------------------------------

    def insert_log(
        self,
        message: str,
        log_time: str | datetime | None = None,
        observation_start: str | datetime | None = None,
        observation_end: str | datetime | None = None,
    ) -> dict:
        """Insert a plant-level log entry (no specific point URI needed).

        Args:
            message: The log message.
            log_time: Timestamp of the log entry (ISO 8601 string or datetime).
                Defaults to now.
            observation_start: Optional observation period start.
            observation_end: Optional observation period end.
        """
        return self.client.insert_log(
            log_time=_dt_to_iso(log_time),
            log_message=message,
            observation_start=_dt_to_iso(observation_start),
            observation_end=_dt_to_iso(observation_end),
        )

    def read_logs(
        self,
        log_time_start: str | datetime | None = None,
        log_time_end: str | datetime | None = None,
        observation_start: str | datetime | None = None,
        observation_end: str | datetime | None = None,
    ) -> "pl.DataFrame":
        """Read plant-level log entries (no query/object needed).

        Returns a list of LogEntry objects for the generic plant URI.
        """
        import polars as pl

        logs = self.client.query_logs(
            log_time_start=_dt_to_iso(log_time_start),
            log_time_end=_dt_to_iso(log_time_end),
            observation_start=_dt_to_iso(observation_start),
            observation_end=_dt_to_iso(observation_end),
        )
        if not logs:
            return pl.DataFrame({"point_uri": [], "message": [], "log_time": [], "observation_start": [], "observation_end": []})
        frames = [log.to_dict() for log in logs]
        schema = {
            "point_uri": pl.Utf8,
            "message": pl.Utf8,
            "log_time": pl.Datetime(time_zone="UTC"),
            "observation_start": pl.Datetime(time_zone="UTC"),
            "observation_end": pl.Datetime(time_zone="UTC"),
        }
        return pl.concat([pl.DataFrame(f, schema=schema) for f in frames], how="vertical").sort("log_time")

    def delete_logs(self) -> dict:
        """Delete all plant-level log entries."""
        return self.client.delete_logs()

    # ------------------------------------------------------------------
    # SPARQL / GRAPH UTILITIES
    # ------------------------------------------------------------------
