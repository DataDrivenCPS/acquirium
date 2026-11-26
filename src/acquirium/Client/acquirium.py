from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

from rdflib import URIRef

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.expr import AttrProxy
from acquirium.apps import App
from acquirium.internals._names import accepts_old_names, new_names
from acquirium.Client.client import AcquiriumClient, CAPACITY_RETRY_S, DEFAULT_HEALTH_REQUEST_TIMEOUT
from acquirium.Client.streams import Stream
from acquirium.Experiments import Point, StudyService
from acquirium.internals.models import MAX_REQUEST_ROWS, compute_stream_uri


def _dt_to_iso(v: "str | datetime | None") -> "str | None":
    if v is None:
        return None
    return v.isoformat() if isinstance(v, datetime) else v


@accepts_old_names
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
            server_url: str = "127.0.0.1",
            server_port: int = 8000,
            use_ssl: bool = False,
            insert_batch_rows: int = 50_000,
            health_timeout: float | None = 60.0,
            token: str | None = None,
        ):
        """Connect to an Acquirium server.

        ``server_url`` is a host name, or a full address such as
        ``https://example.org/api`` for a server behind a reverse proxy. A full
        address carries its own scheme, port and path, so ``server_port`` and
        ``use_ssl`` are ignored. ``token`` is the access token of a server
        that requires one.

        The constructor waits up to ``health_timeout`` seconds (default 60)
        for the server's ``/health`` to answer, retrying while it boots, and
        raises ``ConnectionError`` if it never does. Pass
        ``health_timeout=None`` (or 0) to skip the check.
        """
        self.insert_batch_rows = int(insert_batch_rows)
        if self.insert_batch_rows <= 0:
            raise ValueError("insert_batch_rows must be greater than zero")
        self.client = AcquiriumClient(
            server_url=server_url,
            server_port=server_port,
            use_ssl=use_ssl,
            token=token,
        )
        # Study owns user-facing declarations; the raw HTTP client remains
        # available below for ordinary Acquirium APIs.
        self.study = StudyService(self)
        if health_timeout:
            self._wait_for_server(health_timeout)

    @property
    def address(self) -> str:
        """The HTTP(S) address of the server this client is connected to."""
        return self.client.address

    def _wait_for_server(self, timeout: float) -> None:
        import time as _time
        deadline = _time.monotonic() + timeout
        last_err: Exception | None = None
        while True:
            remaining = deadline - _time.monotonic()
            if remaining <= 0:
                break
            try:
                self.client.health(timeout=min(DEFAULT_HEALTH_REQUEST_TIMEOUT, remaining))
                return
            except Exception as e:
                last_err = e
            remaining = deadline - _time.monotonic()
            if remaining > 0:
                _time.sleep(min(2.0, remaining))
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

        The text matcher searches the bundled ontologies only, so an insert
        does not change what text is matched to.

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

    def insert_metadata(self, uri: "str | URIRef | Stream", values: Mapping[str, Any]) -> dict[str, Any]:
        """Attach metadata to one node (entity or measurement) by URI or CURIE.

        A stream handle from ``aq.stream()`` is accepted in place of the URI:
        the metadata is written on the property the stream is linked to.

        ``values`` is a plain map. A key that names a built-in attribute
        (``unit``, ``medium``, ``label``, ``type``, ...) is written with that
        attribute's predicate, text resolved to a URI. Any other key is a
        user attribute, nested dicts and lists flattened to dotted paths,
        that ``where``/``include``/``options``/``facets`` then accept, and
        ``aq.attr`` completes. Relations of the plant model (``entity``,
        ``measurement``, ...) are not written here::

            aq.insert_metadata("dpr:valve-1", {
                "last_cleaned": "03-12-1999",
                "product_info": {"manufacturer": "Siemens", "year": 2019},
                "tags": ["lab", "critical"],
            })
            aq.query().measurement().where(aq.attr.product_info.year >= 2015)

        Writing a key replaces its previous value; ``None`` removes it. Keys
        not mentioned stay. Metadata lives as long as the node does.
        """
        if isinstance(uri, Stream):
            uri = self.client.stream_property(uri.stream_uri)
        return self.client.insert_metadata({str(uri): values})

    def sparql_update(self, update: str, *, source_id: str) -> dict[str, Any]:
        """Execute a SPARQL update against one explicitly owned data graph.

        Components with a fixed owner use their ``self.sparql_update`` helper
        instead of passing ``source_id`` themselves.
        """
        return self.client.sparql_update(update, source_id=source_id)

    def query(self) -> Query:
        """Create a new empty Query (the explore builder) bound to this instance."""
        return Query(client=self.client)

    @property
    def attr(self) -> AttrProxy:
        """Attribute paths for ``where``: ``aq.attr.unit``, ``aq.attr.product_info.year``.

        Bound to this server, so ``aq.attr.<TAB>`` lists the built-in
        attributes and those written with ``insert_metadata``, and an
        unknown name fails here rather than inside the query. Comparisons
        and ``&``/``|``/``~`` build the expressions ``Query.where`` takes.
        """
        return AttrProxy(self.client)

    def explore(self) -> Query:
        """Alias of :meth:`query`."""
        return self.query()

    def find_all_data(self, *, _class: Optional[str] = None, uri: str | URIRef | None = None) -> Query:
        """Every registered measurement point, as a :class:`Query`.

        Shorthand for ``acq.query().measurement(type=_class, uri=uri)``:
        ``_class`` restricts the points to a class (URI or free text) and
        ``uri`` pins the query to one point.
        """
        attrs = {"type": _class} if _class is not None else {}
        return self.query().measurement(uri=uri, **attrs)

    # ------------------------------------------------------------------
    # TIMESERIES API
    # ------------------------------------------------------------------

    def insert_timeseries(
        self,
        source_id: str,
        stream_name: str,
        rows: list[tuple[datetime, Any]],
        *,
        property_uri: str | None = None,
        request_id: str | None = None,
        replace: bool = False,
        wait: float = 30,
        timeout: float = 60,
        retry_for: float = CAPACITY_RETRY_S,
    ) -> dict[str, Any]:
        """Upsert observations; wait=0 returns after durable acceptance.

        ``replace=True`` keeps exactly the supplied rows, removing readings at
        other timestamps; an empty replacement clears the stream. A replacement
        must fit in one request, with one slot reserved for the clear.
        timeout bounds the HTTP call. A transport failure leaves acceptance
        unknown; retry the same request ID and payload. A request the server
        refuses with 503 is resent for up to ``retry_for`` seconds.
        """
        return self.client.insert_timeseries(
            source_id=source_id,
            stream_name=stream_name,
            rows=rows,
            property_uri=property_uri,
            request_id=request_id,
            replace=replace,
            wait=wait, timeout=timeout, retry_for=retry_for,
        )

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None, replace=(), wait=30,
                                timeout=60, retry_for=CAPACITY_RETRY_S):
        """Upsert ``{stream_name: rows}``; each stream named in ``replace`` holds
        exactly its supplied rows. Large inputs are split into requests of at
        most 10,000 rows, each atomic. Each replacement must fit in one
        request, including its clear."""
        result = self.client.insert_timeseries_batch(
            source_id, streams, request_id=request_id, replace=replace, wait=wait,
            timeout=timeout, retry_for=retry_for,
            batch_rows=min(self.insert_batch_rows, MAX_REQUEST_ROWS),
        )
        return {**result, "batches": len(result["receipts"])}

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, request_id: str | None = None,
        wait: float = 30, timeout: float = 60, retry_for: float = CAPACITY_RETRY_S,
    ) -> dict[str, Any]:
        """Insert a ``(ts, stream_name, value)`` Arrow table. Tables over 10,000
        rows are split into requests in order, each atomic.

        Reuse ``request_id`` and the exact normalized payload on retry.
        Deduplication follows Escapement's bounded native retry window.
        A request the server refuses with 503 is resent for up to
        ``retry_for`` seconds.
        """
        return self.client.insert_timeseries_arrow(
            source_id, table, request_id=request_id, wait=wait, timeout=timeout,
            retry_for=retry_for,
        )

    def resolve_point_metadata(self, *args, **kwargs):
        """Alias of :meth:`resolve_property_metadata`."""
        return self.resolve_property_metadata(*args, **kwargs)

    def resolve_property_metadata(
        self, fields: dict[str, Any], min_score: float = 0.6
    ) -> dict[str, str | None]:
        """Resolve the semantic metadata of a single point/property.

        ``fields`` maps known semantic field names to a raw value::

            aq.resolve_property_metadata({
                "unit": "gal/min",
                "quantity_kind": "flow rate",
                "medium": "water",
            })
            # -> {"unit": "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            #     "quantity_kind": ".../quantitykind/VolumeFlowRate",
            #     "medium": "https://watermetadata.org/ontology/watr#Water"}

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
        return self.client.resolve_property_metadata(fields, min_score=min_score)


    def graph_version(self) -> int:
        """Return the server's current source-data generation."""
        return self.client.graph_version()

    def graph_status(self) -> dict[str, int | bool]:
        """Return source and derived-query cache generations from the server."""
        return self.client.graph_status()

    def validate_graph(self) -> dict[str, str | bool]:
        """Validate all registered deployment data against ontology shapes."""
        return self.client.validate_graph()

    def reference_uri(self, source_id: str, stream_name: str) -> URIRef:
        """Return the canonical Acquirium reference URI for ``(source_id, stream_name)``."""
        return compute_stream_uri(source_id, stream_name)

    def stream(self, source_id_or_uri: str | URIRef, name: str | None = None) -> Stream:
        """Return a handle for one stored stream.

        ``ac.stream(source_id, name)`` names a stream by the source and stream
        name its driver writes with; ``ac.stream(stream_uri)`` wraps a reference
        URI. Use ``stream.between(start, end)`` to select a time range.
        """
        stream_uri = compute_stream_uri(str(source_id_or_uri), name) if name is not None else source_id_or_uri
        return Stream(str(stream_uri), self.client)

    def point(self, uri: str) -> Point:
        """Return a lightweight semantic point handle for experiment variables."""
        return Point(uri)

    def resolve(self, text: str, *, kind: str | None = None, min_score: float = 0.6) -> Point:
        """Resolve vocabulary text to an ontology concept for experiment metadata.

        Free text searches the ontology indexes. Use plant queries to discover
        modeled property instances, then pass their URIs to ``point()``.
        """
        # Keep text matching at the boundary. Persisted variable metadata is
        # always the resolved URI, never an ambiguous natural-language string.
        uri = self.client.resolve(text, kind=kind, top_k=1, min_score=min_score)
        if uri is None:
            raise ValueError(f"could not resolve graph resource {text!r}")
        return Point(str(uri))

    def register_streams(
        self,
        streams: Iterable[dict[str, Any]],
    ) -> None:
        """Declare one or more streams' semantic metadata in one graph insert.

        Each stream item is a dictionary with these commonly used keys:

        - ``property_uri``: optional semantic URI. When provided, the property is
          linked to the stream. When omitted, Acquirium generates a property
          for the stream and marks it ``is_disconnected``. Registering the
          stream again with a ``property_uri`` connects it: the generated
          property is replaced, and its metadata moves to ``property_uri``
          (see :meth:`AcquiriumClient.register_streams` for the rules).
        - ``source_id`` and ``stream_name``: source-local stream identity. When
          both are present, Acquirium mints the canonical reference URI and
          writes ``acq:sourceId``, ``acq:refName``, and ``ref:storedAt`` on it.
        - ``label``: optional ``rdfs:label`` written on ``property_uri``.
        - ``data_source``: optional datasource marker written on the point.
        - ``properties``: optional mapping of predicate URIRefs to values,
          written on the reference node (or ``property_uri`` when no ref node exists).

        Plain strings for ``unit``/``quantity_kind``/``medium``/``substance``
        are resolved jointly per stream via :meth:`resolve_property_metadata`;
        URI / URIRef / None values pass through.

        Drivers should still insert rows with ``insert_timeseries_arrow`` using
        the same ``source_id`` and source-local ``stream_name``. Acquirium resolves
        those inserts to the same canonical stream URI internally.

        The old keys ``point_uri`` and ``ref_name`` are still accepted;
        ``ref_name`` is deprecated.
        """
        streams = [new_names(stream) for stream in streams]
        self.client.register_streams(streams)

    def deploy_app(self, declaration: App, *, dry_run: bool = False) -> dict[str, Any]:
        """Compile semantic inputs and activate a complete native plan.

        With ``dry_run=True`` nothing is published or activated; the result
        reports, per app, the operators the plan would add, rebuild or rebind
        and the groups and input rows that re-evaluates.
        """
        return self.client.deploy_app(declaration.to_dict(), dry_run=dry_run)

    def wait(self, receipt, *, timeout=30):
        return self.client.wait(receipt, timeout=timeout)

    def engine_status(self):
        return self.client.engine_status()

    def engine_snapshot(self):
        return self.client.engine_snapshot()

    def retry_engine(self):
        """Retry failed soft sensors after repairing their code or environment."""
        return self.client.retry_engine()

    def changes(self, *, epoch=0, ordinal=0, limit=1000, timeout=30):
        """Page committed native changes; persist the cursor with sink updates."""
        return self.client.changes(epoch=epoch, ordinal=ordinal, limit=limit, timeout=timeout)

    def delete_timeseries(self, source_id, stream_name, timestamps, *, request_id=None, wait=30, timeout=60,
                          retry_for=CAPACITY_RETRY_S):
        return self.client.delete_timeseries(source_id, stream_name, timestamps, request_id=request_id,
                                             wait=wait, timeout=timeout, retry_for=retry_for)

    def list_apps(self) -> dict[str, Any]:
        """List every deployment, including apps with no matches or errors."""
        return self.client.list_apps()

    def inspect_app(self, name: str) -> dict[str, Any]:
        """Inspect declared output schemas, settings, streams, and progress."""
        return self.client.inspect_app(name)

    def remove_app(self, name: str) -> dict[str, Any]:
        return self.client.remove_app(name)

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
        """Insert a plant-level log entry (no specific property URI needed).

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
            return pl.DataFrame({"property_uri": [], "message": [], "log_time": [], "observation_start": [], "observation_end": []})
        frames = [log.to_dict() for log in logs]
        schema = {
            "property_uri": pl.Utf8,
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
