from typing import Optional, Iterator, Iterable, Any, Callable, TYPE_CHECKING
import re
import uuid
import requests
from requests import HTTPError
from pathlib import Path
import warnings
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    StreamInsert,
    RegisterDatasourceRequest,
    compute_ref_uri,
    looks_like_uri,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Storage.values import normalize_value_kind
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import NamespaceManager, RDF, RDFS
import logging
logger = logging.getLogger(__name__)

DEFAULT_HEALTH_REQUEST_TIMEOUT = 30.0
_CURIE = re.compile(r"[A-Za-z_][\w.-]*:[^\s:]+")

if TYPE_CHECKING:
    import pyarrow as pa

def _raise_for_status(response: requests.Response) -> None:
    """Like response.raise_for_status(), but enriches the HTTPError message with the
    response body. For FastAPI servers this extracts the 'detail' field so the caller
    sees the server-side error message rather than just the status code."""
    try:
        response.raise_for_status()
    except HTTPError as exc:
        detail = response.text
        try:
            parsed = response.json()
            detail = str(parsed.get("detail", parsed))
        except ValueError:
            pass
        raise HTTPError(
            f"{exc}; response body: {detail}",
            response=response,
            request=response.request,
        ) from exc


#: RDF serialisations Acquirium accepts, keyed by file suffix.
RDF_FORMATS = {
    ".ttl": "turtle",
    ".n3": "n3",
    ".xml": "xml",
    ".rdf": "xml",
    ".trix": "trix",
}

# Known point-metadata fields → the resolver ``kind`` each is resolved as.
# The field name is the semantic role, so callers supply no ``kind``.
POINT_FIELD_KINDS: dict[str, str] = {
    "unit": "unit",
    "quantity_kind": "quantity_kind",
    "medium": "substance",
    "substance": "substance",
}

# The same fields → the predicate each is written under on the point. Extend
# both maps together when adding a semantic field; ``data_source`` is a plain
# literal and stays outside (it is not resolved).
POINT_FIELD_PREDICATES: dict[str, URIRef] = {
    "unit": HAS_UNIT,
    "quantity_kind": HAS_QUANTITY_KIND,
    "medium": HAS_MEDIUM,
    "substance": OF_SUBSTANCE,
}


def _add_triple(g: Graph, subj: URIRef, pred: URIRef, value: "str | URIRef | None") -> None:
    if value is None:
        return
    if isinstance(value, URIRef):
        g.add((subj, pred, value))
    elif "://" in str(value) or str(value).startswith("urn:"):
        g.add((subj, pred, URIRef(str(value))))
    else:
        g.add((subj, pred, Literal(value)))


def _coerce_resolved(
    resolved: dict[str, "str | None"], name: str, value: Any,
) -> "str | URIRef | None":
    """Map a raw field value to its resolved URIRef.

    Passes ``None``/``URIRef`` through; warns and returns ``None`` when
    plain text did not resolve, so the field is skipped as if it had not
    been supplied.
    """
    if value is None:
        return None
    if isinstance(value, URIRef):
        return value
    uri = resolved.get(name)
    if uri is None:
        warnings.warn(
            f"Could not resolve {name!r} value {value!r} to a QUDT URI; "
            "skipping",
            stacklevel=3,
        )
        return None
    return URIRef(uri)


def _build_stream_triples(
    g: Graph, stream: dict, resolved: dict[str, "str | None"],
    existing: dict[str, "str | None"],
    units_compatible: "Callable[[Any, Any], bool]",
) -> None:
    """Write one stream's reference/point/metadata triples into ``g``.

    ``resolved`` is the precomputed ``field -> URI-or-None`` map for this
    stream's semantic fields (typically from ``resolve_point_metadata``);
    pass ``{}`` when there is nothing to resolve.

    ``existing`` is the metadata the point already carries in the server
    graph (from ``AcquiriumClient._point_metadata``); pass ``{}`` for a
    point that does not exist yet. A provided field the point lacks is
    added; a conflicting value raises ``ValueError``. Units are the one
    exception: a provided unit that differs from the point's is accepted
    when ``units_compatible`` says the two are convertible, and is written
    on the external reference as the unit the raw rows are stored in.
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
        g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium Escapement")))

    if point_uri_raw is not None:
        subj = URIRef(str(point_uri_raw))
    else:
        if ref_uri is None:
            raise ValueError(
                "a stream registration needs a point_uri or a "
                "(source_id, ref_name) pair to mint one"
            )
        subj = URIRef(f"{ref_uri}__point")
        if label is None:
            label = ref_name

    g.add((subj, RDF.type, VIRTUAL_POINT))
    if label is not None:
        g.add((subj, RDFS.label, Literal(label)))

    for field, pred in POINT_FIELD_PREDICATES.items():
        provided = _coerce_resolved(resolved, field, stream.get(field))
        if provided is None:
            continue
        current = existing.get(field)
        if current is None:
            _add_triple(g, subj, pred, provided)
        elif str(current) == str(provided):
            if field == "unit" and ref_uri is not None:
                _add_triple(g, ref_uri, HAS_UNIT, provided)
        elif field != "unit":
            raise ValueError(
                f"{field} mismatch for point <{subj}>: the point has "
                f"<{current}>, the stream provided <{provided}>"
            )
        elif not units_compatible(provided, current):
            raise ValueError(
                f"unit mismatch for point <{subj}>: the point has <{current}>, "
                f"the stream provided <{provided}> and they are not convertible"
            )
        elif ref_uri is None:
            raise ValueError(
                f"stream for point <{subj}> stores data in <{provided}> but has "
                "no (source_id, ref_name) reference to record the storage unit on"
            )
        else:
            # Convertible: the provided unit is what the raw rows are stored
            # in; data access defaults to the point's unit and converts.
            _add_triple(g, ref_uri, HAS_UNIT, provided)

    data_source = stream.get("data_source")
    if data_source is not None:
        current = existing.get("data_source")
        if current is None:
            _add_triple(g, subj, DATA_SOURCE, data_source)
        elif str(current) != str(data_source):
            raise ValueError(
                f"data_source mismatch for point <{subj}>: the point has "
                f"{current!r}, the stream provided {data_source!r}"
            )

    if ref_uri is not None:
        g.add((subj, HAS_EXTERNAL_REFERENCE, ref_uri))

    target = ref_uri if ref_uri is not None else subj
    for pred, value in (stream.get("properties") or {}).items():
        _add_triple(g, target, pred, value)


class AcquiriumClient:
    # 127.0.0.1 rather than "localhost": the server listens on IPv4 only, and
    # "localhost" resolves to ::1 first on most systems. The failed IPv6
    # attempt costs about 2 s per TCP connection on Windows (issue #85).
    def __init__(self,
                 server_url: str = "127.0.0.1",
                 server_port: int = 8000,
                 use_ssl: bool = False):
        self.base_url = f"{'https' if use_ssl else 'http'}://{server_url}:{server_port}"
        # One Session per client so HTTP keep-alive reuses the TCP connection.
        # Bare requests.get/post open a new connection per call, which paid the
        # connect cost (and the ::1 miss above) on every request.
        self._http = requests.Session()
        self._namespaces_cache = None

    @property
    def address(self) -> str:
        """The HTTP(S) address of the server this client uses."""
        return self.base_url

    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = True,
        *,
        source_id: str,
    ) -> None:
        """
        Insert RDF graph into an explicitly owned deployment data graph.

        The server refreshes the embedding index synchronously before
        responding, so inserted concepts are resolvable once this returns.

        Args:
            rdf_graph: RDF graph content as text.
            format: Format of the RDF data [turtle | n3 | xml | trix].
            replace: If True, replaces the selected graph. If False, appends to it.
            source_id: Data-graph owner. Use ``"plant"`` for the shared plant
                model, or a component's stable source ID.
        """
        if not isinstance(rdf_graph, str):
            raise TypeError(
                "rdf_graph must be RDF content as a string; "
                "use insert_graph_file() for paths"
            )
        url = f"{self.base_url}/insert_graph"
        data = {
            "rdf_graph": rdf_graph,
            "format": format,
            "replace": replace,
        }
        data["source_id"] = source_id
        response = self._http.post(url, json=data)
        _raise_for_status(response)

    def insert_graph_file(
        self,
        path: str | Path,
        format: str | None = None,
        replace: bool = True,
        *,
        source_id: str,
    ) -> None:
        """Read RDF from *path* and insert it into an explicitly owned graph."""
        source_path = Path(path)
        if not source_path.is_file():
            raise FileNotFoundError(f"Graph file not found: {source_path}")
        resolved_format = format or RDF_FORMATS.get(source_path.suffix.lower())
        if resolved_format is None:
            raise ValueError(
                f"cannot infer RDF format from {source_path.suffix!r}; pass format explicitly"
            )
        self.insert_graph(
            source_path.read_text(),
            format=resolved_format,
            replace=replace,
            source_id=source_id,
        )

    def timeseries_df(
        self,
        uri: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
        order: Order = "asc",
        timeout: float = 60.0,
        *,
        value_mode: str = "default",
        epoch: int | None = None,
    ) -> pl.DataFrame:
        """
        Fetch the full timeseries payload and return a single Polars DataFrame.
        Best for small to medium responses.
        """
        url = f"{self.base_url}/timeseries"
        params = {
            "uri": uri,
            "start": start,
            "end": end,
            "limit": limit,
            "order": order,
            "value_mode": value_mode,
            "epoch": epoch,
        }
        headers = {"Accept": "application/vnd.apache.arrow.stream"}

        with self._http.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            _raise_for_status(r)

            try:
                # r.raw is a file-like object
                reader = ipc.RecordBatchStreamReader(r.raw)
                # Collect all batches into a single table
                tables = [pl.from_arrow(batch) for batch in reader]
                # The Arrow reader stops at the IPC end-of-stream marker and
                # leaves the HTTP chunked terminator unread, so urllib3 would
                # close the socket instead of returning it to the pool. Drain
                # to EOF so the next request reuses the connection.
                r.raw.read()
                if tables:
                    return pl.concat(tables)
                else:
                    import pyarrow as pa
                    return pl.from_arrow(pa.Table.from_batches([], schema=reader.schema))
            except Exception as e:
                raise RuntimeError("could not read timeseries Arrow response") from e
            

    def timeseries_batches(
        self,
        uri: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
        order: Order = "asc",
        *,
        value_mode: str = "default",
        epoch: int | None = None,
        timeout: float = 60.0,
    ) -> Iterator[pl.DataFrame]:
        """
        Stream an Arrow IPC response and yield Polars DataFrames per RecordBatch.

        This requires the server to return a *single* Arrow IPC stream that contains
        multiple record batches (not multiple concatenated streams).
        """
        url = f"{self.base_url}/timeseries"
        params = {
            "uri": uri,
            "start": start,
            "end": end,
            "limit": limit,
            "order": order,
            "value_mode": value_mode,
            "epoch": epoch,
        }
        headers = {"Accept": "application/vnd.apache.arrow.stream"}

        # stream=True means requests won't buffer the full body into memory
        with self._http.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            _raise_for_status(r)

            # r.raw is a file-like object
            reader = ipc.RecordBatchStreamReader(r.raw)

            for batch in reader:
                # batch is a pyarrow.RecordBatch; convert to Polars
                yield pl.from_arrow(batch)
            # Drain to EOF so the pooled connection is reused (see timeseries_df).
            r.raw.read()

    def timeseries_info_batch(self, uris: list[str]) -> dict:
        """Fetch lightweight stats (row_count, earliest, latest) for multiple URIs in one request."""
        from acquirium.internals.models import TimeseriesInfo
        url = f"{self.base_url}/timeseries_info"
        response = self._http.post(url, json={"uris": uris})
        _raise_for_status(response)
        data = response.json()
        return {uri: TimeseriesInfo.model_validate(info) for uri, info in data.items()}

    def health(self, timeout: float = DEFAULT_HEALTH_REQUEST_TIMEOUT) -> dict:
        """GET /health; raises on connection failure or non-200."""
        response = self._http.get(f"{self.base_url}/health", timeout=timeout)
        _raise_for_status(response)
        return response.json()

    def sparql_query(
        self,
        sparql: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
    ) -> dict:
        """
        Execute a SPARQL query against the graph store.

        Args:
            sparql: The SPARQL query string.
            include_dependencies: Whether to include ontology/shape triples.
            wait_for_fresh: Wait for pending inference instead of using the
                last complete published graph.

        Returns:
            The SPARQL query result as a dictionary.
        """
        # POST, not GET: resolved traversal edges inject potentially large
        # VALUES blocks, and long query strings blow the server's URL limit
        # ("Invalid HTTP request received").
        url = f"{self.base_url}/sparql_json"
        response = self._http.post(
            url,
            json={
                "query": sparql,
                "include_dependencies": include_dependencies,
                "wait_for_fresh": wait_for_fresh,
            },
        )
        _raise_for_status(response)
        payload = response.json()
        if "boolean" in payload:
            return {"columns": [], "rows": [[bool(payload["boolean"])]]}
        if "head" not in payload or "results" not in payload:
            return payload
        columns = payload["head"].get("vars", [])
        rows = [
            [binding.get(column, {}).get("value") for column in columns]
            for binding in payload["results"].get("bindings", [])
        ]
        return {"columns": columns, "rows": rows}

    def sparql_update(self, update: str, *, source_id: str) -> dict:
        """Execute a SPARQL UPDATE against one explicitly owned data graph."""
        url = f"{self.base_url}/sparql_update"
        data = {"update": update}
        data["source_id"] = source_id
        response = self._http.post(url, json=data)
        _raise_for_status(response)
        return response.json()

    def validate_graph(self) -> dict[str, str | bool]:
        """Validate all registered deployment data against ontology shapes."""
        response = self._http.post(f"{self.base_url}/validate_graph")
        _raise_for_status(response)
        return response.json()

    def namespace_manager(self) -> NamespaceManager:
        """Return the Graph that has the ``prefix -> namespace URI`` map from the server.

        Stores (caches) in a rdf Graph object for use by other methods. 
        """
        if self._namespaces_cache is None:
            url = f"{self.base_url}/namespace/list"
            response = self._http.get(url)
            _raise_for_status(response)
            self._namespaces_cache = Graph()
            for prefix, ns_uri in response.json().items():
                self._namespaces_cache.bind(prefix, ns_uri)
        return self._namespaces_cache.namespace_manager
      
    def compact_uri(self, item: str|URIRef) -> str:
        """Return ``prefix:local`` for a URI using bound namespaces.

        Longest-prefix match against ``list_namespaces``. Falls back to the
        bare local name (``strip_namespace``) when no prefix is bound, and
        passes non-URI strings through unchanged.
        """
        s = str(item)
        nm = self.namespace_manager()
        try:
            return nm.curie(s, generate=True)
        except:
            raise ValueError(f"Cannot compact '{s}': no matching namespace for URI and not a valid URI format")


    def expand_uri(self, text: Any) -> str:
        """Expand a ``prefix:local`` CURIE to a full URI using bound namespaces.

        Passes already-full URIs (``urn:``, ``http://``, ``https://``)
        through unchanged. Non-string inputs are cast to ``str``. Returns
        the input unchanged if the prefix is not bound.
        """
        s = str(text)
        nm = self.namespace_manager()
        try:
            return str(nm.expand_curie(s))
        except Exception:
            raise ValueError(f"Cannot expand '{s}': no matching namespace for CURIE and not a full URI")

    def _expand_curie(self, text: str) -> Optional[str]:
        """Expand ``prefix:local`` when the server binds the prefix, else None."""
        if not _CURIE.fullmatch(text):
            return None
        prefix = text.split(":", 1)[0]
        nm = self.namespace_manager()
        if nm.store.namespace(prefix) is None:
            return None
        return str(nm.expand_curie(text))

    def resolve(
        self,
        query: "str | dict[str, tuple[Any, Optional[str]]]",
        kind: Optional[str] = None,
        *,
        top_k: int = 1,
        min_score: float = 0.5,
        context: Optional[list[str]] = None,
    ) -> "Optional[str] | list[dict] | dict[str, Optional[str]]":
        """Resolve free text to ontology/QUDT URIs — the one resolution method.

        Three forms, chosen by the input:

        - ``resolve("mg/l", "unit")`` -> best URI or ``None``. Values that
          already look like URIs pass through unchanged, and CURIEs with a
          prefix bound on the server expand without text matching.
        - ``resolve("mg/l", "unit", top_k=3)`` -> ranked candidate dicts
          (``uri``/``score``/``match_stage``/...), for disambiguation UIs and
          debugging what a text almost matched.
        - ``resolve({"eu": ("gal/min", "unit"), "qty": ("flow", "quantity_kind")})``
          -> ``{label: URI-or-None}``; the fields are resolved **jointly**, so
          a confident sibling disambiguates an ambiguous one. Labels are
          echoed back unchanged; ``kind`` comes from each tuple.

        ``context`` (single-text form only) is an optional list of
        already-chosen URIs used to break symbol ambiguity.
        """
        if isinstance(query, dict):
            out: dict[str, Optional[str]] = {}
            to_resolve: dict[str, tuple[str, Optional[str]]] = {}
            for name, (text, k) in query.items():
                if text is None:
                    out[name] = None
                elif looks_like_uri(text):
                    out[name] = str(text)
                elif (expanded := self._expand_curie(str(text))) is not None:
                    out[name] = expanded
                else:
                    to_resolve[name] = (str(text), k)
            if to_resolve:
                body = {
                    "fields": [
                        {"name": n, "text": t, "kind": k}
                        for n, (t, k) in to_resolve.items()
                    ],
                    "top_k": 1,
                    "min_score": min_score,
                }
                response = self._http.post(f"{self.base_url}/resolve_record", json=body)
                _raise_for_status(response)
                matches = response.json().get("matches", {})
                for name in to_resolve:
                    m = matches.get(name) or []
                    out[name] = m[0]["uri"] if m else None
            return out

        text = str(query)
        uri = text if looks_like_uri(text) else self._expand_curie(text)
        if uri is not None:
            if top_k == 1:
                return uri
            return [{"uri": uri, "kind": kind, "score": 1.0, "match_stage": "passthrough"}]
        params: dict[str, Any] = {"text": text, "top_k": top_k, "min_score": min_score}
        if kind:
            params["kind"] = kind
        if context:
            params["context"] = context
        response = self._http.get(f"{self.base_url}/resolve_text", params=params)
        _raise_for_status(response)
        matches = response.json().get("matches", [])
        if top_k == 1:
            return matches[0]["uri"] if matches else None
        return matches

    def resolve_point_metadata(
        self, fields: dict[str, Any], min_score: float = 0.6
    ) -> dict[str, str | None]:
        """Resolve the semantic metadata of a single point/property.

        ``fields`` maps known semantic field names to a raw value::

            client.resolve_point_metadata({
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
        :meth:`resolve` directly.
        """
        record = {
            name: (value, POINT_FIELD_KINDS.get(name))
            for name, value in fields.items()
        }
        try:
            return self.resolve(record, min_score=min_score)
        except Exception:
            return {name: None for name in record}

    def resolve_conversion(
        self,
        from_unit: str,
        to_unit: str,
        *,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> dict:
        """Resolve a from/to unit pair to a *convertible* match plus factors.

        Each side may be a URI (pinned) or free text; the server picks the
        best-ranked candidate pair that is actually compatible for
        conversion, so a non-convertible near-match never shadows a
        convertible one. Raises ``ValueError`` (with the candidate lists)
        when no compatible pair exists.

        Example::

            resolve_conversion("mg/l", "grams per liter")
            # -> {"from": {"uri": ".../MilliGM-PER-L", "multiplier": ...},
            #     "to":   {"uri": ".../GM-PER-L", ...},
            #     "factors": {"from_multiplier": ..., "to_uri": ..., ...}}
        """
        url = f"{self.base_url}/resolve_conversion"
        response = self._http.post(url, json={
            "from_unit": str(from_unit), "to_unit": str(to_unit),
            "top_k": top_k, "min_score": min_score,
        })
        if not response.ok:
            detail = (response.json().get("detail", response.text)
                      if response.headers.get("content-type", "").startswith("application/json")
                      else response.text)
            raise ValueError(f"resolve_conversion failed: {detail}")
        return response.json()

    def embedding_status(self) -> dict:
        """
        Get the current status of embedding index builds.

        Returns:
            A dictionary with per-index status (graph, qudt).
        """
        url = f"{self.base_url}/embedding_status"
        response = self._http.get(url)
        _raise_for_status(response)
        return response.json()

    def graph_version(self) -> int:
        """Return the server's current source-data generation.

        Use :meth:`graph_status` when a caller also needs to know whether the
        derived query cache has caught up.
        """
        return int(self.graph_status()["source_version"])

    def graph_status(self) -> dict[str, int | bool]:
        """Return source and derived-query cache generations from the server."""
        url = f"{self.base_url}/graph_version"
        response = self._http.get(url)
        _raise_for_status(response)
        return response.json()

    # -------------------- Unit conversion --------------------

    def resolve_unit(self, identifier: str) -> dict:
        """Resolve a unit identifier to its QUDT metadata via the server.

        Example::

            resolve_unit("gal/min")   # off a flow-meter tag
            # -> {"uri": "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            #     "label": "US Gallon per Minute", "symbol": "gal/min",
            #     "quantity_kind": ".../quantitykind/VolumeFlowRate",
            #     "multiplier": 6.30901964e-05, "offset": 0.0}
        """
        url = f"{self.base_url}/resolve_unit"
        response = self._http.post(url, json={"identifier": identifier})
        if not response.ok:
            detail = response.json().get("detail", response.text) if response.headers.get("content-type", "").startswith("application/json") else response.text
            raise ValueError(f"resolve_unit failed: {detail}")
        return response.json()

    def get_conversion_factors(self, from_unit: str, to_unit: str) -> dict:
        """Get pre-computed conversion factors between two units.

        Returns dict with from_multiplier, from_offset, to_multiplier, to_offset, compatible.
        """
        url = f"{self.base_url}/conversion_factors"
        response = self._http.post(url, json={"from_unit": from_unit, "to_unit": to_unit})
        if not response.ok:
            detail = response.json().get("detail", response.text) if response.headers.get("content-type", "").startswith("application/json") else response.text
            raise ValueError(f"conversion_factors failed: {detail}")
        return response.json()


    def list_apps(self) -> dict:
        """Read summaries of every deployed app from the server."""
        response = self._http.get(f"{self.base_url}/apps", timeout=30)
        _raise_for_status(response)
        return response.json()

    def inspect_app(self, name: str) -> dict:
        """Read declared schemas and the latest binding status for an app."""
        from urllib.parse import quote
        response = self._http.get(
            f"{self.base_url}/apps/{quote(name, safe='')}", timeout=30
        )
        _raise_for_status(response)
        return response.json()

    def deploy_app(self, definition: dict) -> dict:
        response = self._http.put(
            f"{self.base_url}/apps/{definition['name']}", json=definition
        )
        _raise_for_status(response)
        return response.json()

    def remove_app(self, name: str) -> dict:
        response = self._http.delete(f"{self.base_url}/apps/{name}")
        _raise_for_status(response)
        return response.json()




    def resolve_storage_keys(self, uris: list[str]) -> dict[str, str]:
        """Map each point URI to its canonical ref URI used by timeseries APIs.

        An already-canonical ref URI passes through. The database may map the
        returned URI to an internal integer key.
        """
        if not uris:
            return {}
        url = f"{self.base_url}/resolve_storage_keys"
        response = self._http.post(url, json={"uris": uris})
        _raise_for_status(response)
        return response.json()

    def register_datasource(self, source_id: str) -> str:
        """Register a named datasource. Returns source_id."""
        url = f"{self.base_url}/register_datasource"
        response = self._http.post(url, json=RegisterDatasourceRequest(source_id=source_id).model_dump())
        _raise_for_status(response)
        return response.json()["source_id"]

    def _units_compatible(self, from_unit: Any, to_unit: Any) -> bool:
        """True when the server can convert between the two unit URIs."""
        try:
            factors = self.get_conversion_factors(str(from_unit), str(to_unit))
            return bool(factors.get("compatible"))
        except Exception:
            return False

    def _point_metadata(self, point_uri: str) -> dict[str, "str | None"]:
        """Return the semantic metadata a point already has in the graph.

        Keys match the stream-registration field names (``unit``,
        ``quantity_kind``, ``medium``, ``substance``, ``data_source``);
        a field the point lacks maps to ``None``. Returns ``{}`` for a
        point the graph does not know at all.
        """
        fields = {**POINT_FIELD_PREDICATES, "data_source": DATA_SOURCE}
        select = " ".join(f"?{name}" for name in fields)
        optionals = "\n".join(
            f"            OPTIONAL {{ <{point_uri}> <{pred}> ?{name} . }}"
            for name, pred in fields.items()
        )
        sparql = f"SELECT {select} WHERE {{\n{optionals}\n        }}"
        result = self.sparql_query(sparql)
        rows = result.get("rows") or []
        if not rows:
            return {}
        return dict(zip(result["columns"], rows[0]))

    def register_streams(
        self,
        streams: Iterable[dict[str, Any]],
    ) -> None:
        """Declare one or more streams' semantic metadata in one graph insert.

        Each stream item is a dictionary with these commonly used keys:

        - ``point_uri``: optional semantic URI. When provided, the point is
          linked to the external reference and its provided metadata is
          checked against what the graph already has (see below). When
          omitted, a dummy point (``<ref_uri>__point``) is minted and
          linked to the external reference.
        - ``source_id`` and ``ref_name``: source-local stream identity. When
          both are present, Acquirium mints the canonical reference URI and
          writes ``acq:sourceId``, ``acq:refName``, and ``ref:storedAt`` on it.
        - ``label``: optional ``rdfs:label`` written on the point. A dummy
          point without one gets its ``ref_name`` (e.g. the CSV column name).
        - ``data_source``: optional datasource marker written on the point.
        - ``properties``: optional mapping of predicate URIRefs to values,
          written on the reference node (or the point when no ref node exists).

        Plain strings for ``unit``/``quantity_kind``/``medium``/``substance``
        are resolved jointly per stream via :meth:`resolve_point_metadata`;
        URI / URIRef / None values pass through.

        For a ``point_uri`` the graph already knows, a provided field the
        point lacks is added to it; a value that conflicts with the point's
        raises ``ValueError`` before anything is inserted. Units are the
        one exception: a provided unit that differs from the point's is
        accepted when the two are convertible, and is written on the
        external reference as the unit the raw rows are stored in — data
        access defaults to the point's unit and converts automatically.
        A non-convertible unit raises ``ValueError``.

        Drivers should still insert rows with ``insert_timeseries_arrow`` using
        the same ``source_id`` and source-local ``ref_name``. Acquirium resolves
        those inserts to the same canonical reference URI internally.
        """
        graphs: dict[str, Graph] = {}
        for stream in streams:
            source_id = stream.get("source_id")
            if not isinstance(source_id, str) or not source_id:
                raise ValueError("each stream registration requires a non-empty source_id")
            graph = graphs.setdefault(source_id, Graph())
            meta = {
                f: stream.get(f)
                for f in POINT_FIELD_KINDS
                if stream.get(f) is not None
            }
            resolved = self.resolve_point_metadata(meta) if meta else {}
            point_uri = stream.get("point_uri")
            existing = self._point_metadata(str(point_uri)) if point_uri is not None else {}
            _build_stream_triples(graph, stream, resolved, existing, self._units_compatible)
        for source_id, graph in graphs.items():
            if len(graph):
                self.insert_graph(
                    graph.serialize(format="turtle"),
                    format="turtle",
                    replace=False,
                    source_id=source_id,
                )

    def insert_timeseries(self, *, source_id, ref_name, rows, request_id=None, wait=30, timeout=60):
        return self.insert_timeseries_batch(source_id, {ref_name: rows}, request_id=request_id,
                                            wait=wait, timeout=timeout)

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None, wait=30, timeout=60):
        request_id = request_id or str(uuid.uuid4())
        payload = [StreamInsert(source_id=source_id, ref_name=name, values=rows,
                               request_id=request_id) for name, rows in streams.items()]
        response = self._http.post(f"{self.base_url}/insert_timeseries",
                                   json=[item.model_dump(mode="json") for item in payload],
                                   params={"wait": wait}, timeout=timeout)
        _raise_for_status(response)
        return response.json()

    def wait(self, receipt, *, timeout=30):
        """Wait for a database-scoped sequence returned by an accepted request."""
        import math
        import time
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("timeout must be finite and nonnegative")
        sequence = receipt["sequence"]
        deadline = time.monotonic() + timeout
        while True:
            status = self.engine_status()
            if status["engine"]["applied"] >= sequence:
                return self.engine_snapshot()
            if status["error"]:
                raise RuntimeError(f"accepted request {sequence} is blocked: {status['error']}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"accepted request {sequence} remains pending")
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))

    def engine_status(self):
        response = self._http.get(f"{self.base_url}/engine/status")
        _raise_for_status(response)
        return response.json()

    def engine_snapshot(self):
        response = self._http.get(f"{self.base_url}/engine/snapshot")
        _raise_for_status(response)
        return response.json()

    def retry_engine(self):
        """Resume accepted work after repairing a failed function/environment."""
        response = self._http.post(f"{self.base_url}/engine/retry")
        _raise_for_status(response)
        return response.json()

    def changes(self, *, epoch=0, ordinal=0, limit=1000, timeout=30):
        """Read a bounded page of committed changes after an exclusive cursor."""
        response = self._http.get(f"{self.base_url}/engine/changes",
                                 params={"epoch": epoch, "ordinal": ordinal, "limit": limit},
                                 timeout=timeout)
        _raise_for_status(response)
        return response.json()

    def delete_timeseries(self, source_id, ref_name, timestamps, *, request_id=None, wait=30, timeout=60):
        from acquirium.core import time_ns
        response = self._http.post(f"{self.base_url}/delete_timeseries", json={
            "source_id": source_id, "ref_name": ref_name,
            "timestamps": [time_ns(value) for value in timestamps],
            "request_id": request_id or str(uuid.uuid4())}, params={"wait": wait}, timeout=timeout)
        _raise_for_status(response)
        return response.json()

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, request_id: Optional[str] = None,
        wait=30, timeout=60,
    ) -> dict[str, Any]:
        import io
        import pyarrow as pa
        table_with_sid = table.append_column("source_id", pa.repeat(source_id, len(table)))
        buf = io.BytesIO()
        with ipc.new_stream(buf, table_with_sid.schema) as writer:
            writer.write_table(table_with_sid)
        buf.seek(0)
        headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
        headers["X-Acquirium-Request-Id"] = request_id or str(uuid.uuid4())
        response = self._http.post(
            f"{self.base_url}/insert_timeseries_arrow",
            data=buf,
            headers=headers,
            params={"wait": wait}, timeout=timeout,
        )
        _raise_for_status(response)
        return response.json()
