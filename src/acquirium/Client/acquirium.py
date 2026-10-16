from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

from rdflib import URIRef

import warnings

from acquirium.Client.explore.core import Query
from acquirium.Client.query import Q
from acquirium.apps import App


def _dt_to_iso(v: "str | datetime | None") -> "str | None":
    if v is None:
        return None
    return v.isoformat() if isinstance(v, datetime) else v


from acquirium.Client.client import AcquiriumClient, DEFAULT_HEALTH_REQUEST_TIMEOUT
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
            lexicon_path: Optional[Path] = None,
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
    ) -> dict[str, Any]:
        """Insert timeseries data for a single stream.

        Args:
            source_id: The registered datasource identifier.
            ref_name: The source-local stream identifier. Combined with
                ``source_id`` to derive the canonical reference URI.
            rows: List of (timestamp, value) tuples.
            point_uri: Compatibility field. Register the point relationship
                with ``register_streams`` before inserting rows.
            replace: Keep exactly the supplied rows, removing omitted timestamps.
                An empty replacement clears the stream.

        Returns:
            dict with ``{"ok": True, "rows_inserted": N}``.
        """
        return self.client.insert_timeseries(
            source_id=source_id,
            ref_name=ref_name,
            rows=rows,
            request_id=request_id,
        )

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None):
        """Submit one atomic bounded request. Chunk larger imports explicitly."""
        return self.client.insert_timeseries_batch(source_id, streams, request_id=request_id)

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, request_id: str | None = None
    ) -> dict[str, Any]:
        """Insert a ``(ts, ref_name, value)`` Arrow table.

        Reuse ``request_id`` and the exact normalized payload on retry.
        Deduplication follows Escapement's bounded native retry window.
        """
        return self.client.insert_timeseries_arrow(
            source_id, table, request_id=request_id
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

    def deploy_app(self, declaration: App) -> dict[str, Any]:
        """Compile semantic inputs and activate a complete native plan."""
        return self.client.deploy_app(declaration.to_dict())

    def wait(self, receipt, *, timeout=30):
        return self.client.wait(receipt, timeout=timeout)

    def engine_status(self):
        return self.client.engine_status()

    def engine_snapshot(self):
        return self.client.engine_snapshot()

    def delete_timeseries(self, source_id, ref_name, timestamps, *, request_id=None):
        return self.client.delete_timeseries(source_id, ref_name, timestamps, request_id=request_id)

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




    # ------------------------------------------------------------------
    # SPARQL / GRAPH UTILITIES
    # ------------------------------------------------------------------
