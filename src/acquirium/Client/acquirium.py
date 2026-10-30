from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

from rdflib import URIRef

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.expr import AttrProxy
from acquirium.apps import App


from acquirium.Client.client import AcquiriumClient, CAPACITY_RETRY_S, DEFAULT_HEALTH_REQUEST_TIMEOUT
from acquirium.Client.streams import Stream
from acquirium.Experiments import Point, StudyService
from acquirium.internals.models import compute_ref_uri


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
            health_timeout: float | None = 60.0,
        ):
        """Connect to an Acquirium server.

        The constructor waits up to ``health_timeout`` seconds (default 60)
        for the server's ``/health`` to answer, retrying while it boots, and
        raises ``ConnectionError`` if it never does. Pass
        ``health_timeout=None`` (or 0) to skip the check.
        """
        self.client = AcquiriumClient(
            server_url=server_url,
            server_port=server_port,
            use_ssl=use_ssl,
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

    def insert_metadata(self, uri: str | URIRef, values: Mapping[str, Any]) -> dict[str, Any]:
        """Attach metadata to one node (entity or measurement) by URI or CURIE.

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

    def register_datasource(self, source_id: str) -> str:
        """Register a named datasource in the knowledge graph.

        The ``source_id`` is a user-provided string that scopes stream
        ``ref_name`` values so two sources with the same ``ref_name`` never
        produce colliding canonical reference URIs. Safe to call on every startup —
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
        request_id: str | None = None,
        wait: float = 30,
        timeout: float = 60,
        retry_for: float = CAPACITY_RETRY_S,
    ) -> dict[str, Any]:
        """Upsert numeric observations; wait=0 returns after durable acceptance.

        timeout bounds the HTTP call. A transport failure leaves acceptance
        unknown; retry the same request ID and payload. A request the server
        refuses with 503 is resent for up to ``retry_for`` seconds.
        """
        return self.client.insert_timeseries(
            source_id=source_id,
            ref_name=ref_name,
            rows=rows,
            request_id=request_id,
            wait=wait, timeout=timeout, retry_for=retry_for,
        )

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None, wait=30, timeout=60,
                                retry_for=CAPACITY_RETRY_S):
        """Submit one atomic bounded request. Chunk larger imports explicitly."""
        return self.client.insert_timeseries_batch(source_id, streams, request_id=request_id,
                                                  wait=wait, timeout=timeout, retry_for=retry_for)

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, request_id: str | None = None,
        wait: float = 30, timeout: float = 60, retry_for: float = CAPACITY_RETRY_S,
    ) -> dict[str, Any]:
        """Insert a ``(ts, ref_name, value)`` Arrow table.

        Reuse ``request_id`` and the exact normalized payload on retry.
        Deduplication follows Escapement's bounded native retry window.
        A request the server refuses with 503 is resent for up to
        ``retry_for`` seconds.
        """
        return self.client.insert_timeseries_arrow(
            source_id, table, request_id=request_id, wait=wait, timeout=timeout,
            retry_for=retry_for,
        )

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
        return self.client.resolve_point_metadata(fields, min_score=min_score)


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

    def stream(self, source_id_or_uri: str | URIRef, name: str | None = None) -> Stream:
        """Return a handle for one stored stream.

        ``ac.stream(source_id, name)`` names a stream by the source and stream
        name its driver writes with; ``ac.stream(ref_uri)`` wraps a reference
        URI. Use ``stream.between(start, end)`` to select a time range.
        """
        ref_uri = compute_ref_uri(str(source_id_or_uri), name) if name is not None else source_id_or_uri
        return Stream(str(ref_uri), self.client)

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
        """Resume the paused native processor after repairing its failure."""
        return self.client.retry_engine()

    def changes(self, *, epoch=0, ordinal=0, limit=1000, timeout=30):
        """Page committed native changes; persist the cursor with sink updates."""
        return self.client.changes(epoch=epoch, ordinal=ordinal, limit=limit, timeout=timeout)

    def delete_timeseries(self, source_id, ref_name, timestamps, *, request_id=None, wait=30, timeout=60,
                          retry_for=CAPACITY_RETRY_S):
        return self.client.delete_timeseries(source_id, ref_name, timestamps, request_id=request_id,
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
    # SPARQL / GRAPH UTILITIES
    # ------------------------------------------------------------------
