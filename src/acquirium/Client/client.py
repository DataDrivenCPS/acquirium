from typing import Optional, Iterator, Iterable, Any, Callable, Mapping, TYPE_CHECKING
from datetime import datetime, timezone
import json
import re
import requests
from requests import HTTPError
from pathlib import Path
from urllib.parse import quote
import warnings
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    LogEntry,
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

from acquirium.StreamMetadata import (
    POINT_FIELD_KINDS, POINT_FIELD_PREDICATES, _add_triple, _coerce_resolved, _build_stream_triples,
)

def _split_stream_metadata(stream: dict) -> tuple[dict, dict]:
    """Return ``(stream with metadata fields merged in, remaining metadata)``.

    A ``metadata`` key naming a top-level stream field (``unit``, ``label``,
    ...) is the same declaration and merges into the stream; giving both
    with different values is an error. Everything else (user attributes,
    other built-ins) is written on the point by the metadata builder.
    """
    from acquirium.Client.metadata import STREAM_FIELDS

    meta = dict(stream.get("metadata") or {})
    merged = {k: v for k, v in stream.items() if k != "metadata"}
    rest: dict = {}
    for key, value in meta.items():
        if key in STREAM_FIELDS:
            current = merged.get(key)
            if current is not None and str(current) != str(value):
                raise ValueError(
                    f"stream gives {key}={current!r} and metadata gives {key}={value!r}")
            merged[key] = value
        else:
            rest[key] = value
    return merged, rest


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
        self._namespaces_cache: dict[str, str] | None = None

    @property
    def address(self) -> str:
        """The HTTP(S) address of the server this client uses."""
        return self.base_url

    # These intentionally thin methods are transport only. The public client
    # turns them into Study/Experiment/Variable objects in Experiments.py.
    def define_experiment(self, name: str) -> dict: return self._experiment_post("/experiments/templates", {"name": name})
    def sync_experiment_metadata(self) -> dict:
        try:
            return self._experiment_post("/experiments/metadata/sync", {})
        except HTTPError as error:
            if error.response is not None and error.response.status_code == 404:
                raise RuntimeError(
                    f"The server at {self.base_url} does not support experiment metadata. "
                    "Restart the Acquirium server with the current checkout. "
                    "Then restart the notebook kernel and rerun its cells."
                ) from error
            raise
    def declare_experiment_variable(self, template_id: str, **body: Any) -> dict: return self._experiment_post(f"/experiments/templates/{template_id}/variables", body)
    def declare_experiment_variables(self, template_id: str, declarations: list[dict]) -> list[dict]:
        return self._experiment_post(f"/experiments/templates/{template_id}/variables/batch", declarations)
    def list_experiment_studies(self, name: str | None = None) -> list[dict]:
        return self._experiment_get("/experiments/templates", {"name": name})
    def get_experiment_study(self, identifier: str) -> dict:
        return self._experiment_get("/experiments/templates/lookup", {"identifier": identifier})
    def list_experiment_variables(self, template_id: str) -> list[dict]:
        return self._experiment_get(f"/experiments/templates/{template_id}/variables")
    def start_experiment(self, template_id: str, metadata: dict | None = None) -> dict: return self._experiment_post(f"/experiments/templates/{template_id}/runs", {"metadata": metadata or {}})
    def list_experiments(self, template_id: str, **filters: Any) -> list[dict]:
        if "run_ids" in filters:
            return self._experiment_post(f"/experiments/templates/{template_id}/runs/query", self._experiment_params(filters))
        return self._experiment_get(f"/experiments/templates/{template_id}/runs", self._experiment_params(filters))
    def get_experiment(self, run_id: str) -> dict:
        return self._experiment_get(f"/experiments/runs/{run_id}")
    def record_experiment_batch(self, run_id: str, entries: list[dict]) -> list[dict]:
        return self._experiment_post(f"/experiments/runs/{run_id}/record", entries)

    def observe_experiment(self, run_id: str, variable_id: str, **body: Any) -> dict: return self._experiment_post(f"/experiments/runs/{run_id}/variables/{quote(variable_id, safe='')}/observations", body)
    def observe_experiment_batch(self, run_id: str, observations: list[dict]) -> list[dict]:
        return self._experiment_post(f"/experiments/runs/{run_id}/observations/batch", observations)
    def annotate_experiment_stream(self, run_id: str, ref_uri: str, *, start: str, end: str | None = None, metadata: dict | None = None) -> dict:
        return self._experiment_post(f"/experiments/runs/{run_id}/stream-annotations", {"ref_uri": ref_uri, "start": start, "end": end, "metadata": {} if metadata is None else metadata})
    def close_experiment_stream_annotation(self, annotation_id: str, *, end: str) -> dict:
        return self._experiment_post(f"/experiments/stream-annotations/{annotation_id}/close", {"end": end})
    def list_experiment_stream_annotations(self, **filters: Any) -> list[dict]:
        return self._experiment_get("/experiments/stream-annotations", self._experiment_params(filters))
    def list_experiment_observations(self, template_id: str, **filters: Any) -> list[dict]:
        if "run_ids" in filters:
            return self._experiment_post(f"/experiments/templates/{template_id}/observations/query", self._experiment_params(filters))
        return self._experiment_get(f"/experiments/templates/{template_id}/observations", self._experiment_params(filters))
    def finish_experiment(self, run_id: str, failed: bool = False, error: Any = None) -> dict: return self._experiment_post(f"/experiments/runs/{run_id}/{'fail' if failed else 'finish'}", {"error": error})
    def delete_experiments(self, run_ids: list[str]) -> dict: return self._experiment_post("/experiments/runs/delete", {"run_ids": list(run_ids)})
    def delete_experiment_study(self, template_id: str) -> dict: return self._experiment_post(f"/experiments/templates/{template_id}/delete", {})
    def clear_experiments(self, *, confirm: bool = False) -> dict: return self._experiment_post("/experiments/clear", {"confirm": confirm})
    def experiment_usage(self, *, template_id: str | None = None, run_ids: list[str] | None = None) -> dict:
        return self._experiment_post("/experiments/usage", {"template_id": template_id, "run_ids": run_ids})
    def attach_experiment_file(self, run_id: str, variable_id: str, path: str | Path, media_type: str | None = None) -> dict:
        # Send bytes, not a client-local filename. The server owns the durable
        # artifact copy so experiments survive the originating machine.
        source = Path(path)
        return self.attach_experiment_bytes(run_id, variable_id, source.read_bytes(), filename=source.name, media_type=media_type)
    def attach_experiment_bytes(self, run_id: str, variable_id: str, data: bytes, *, filename: str, media_type: str | None = None) -> dict:
        from acquirium.ExperimentValues import artifact_entry
        from hashlib import sha256
        self.record_experiment_batch(run_id, [artifact_entry(variable_id, data, filename=filename, media_type=media_type)])
        return {"digest": sha256(data).hexdigest(), "filename": filename, "byte_length": len(data)}
    def experiment_artifact(self, digest: str) -> bytes:
        response = self._http.get(f"{self.base_url}/experiments/artifacts/{digest}"); _raise_for_status(response); return response.content
    def _experiment_post(self, path: str, body: dict | list) -> Any:
        response = self._http.post(f"{self.base_url}{path}", json=body); _raise_for_status(response); return response.json()
    def _experiment_get(self, path: str, params: dict[str, Any] | None = None):
        response = self._http.get(
            f"{self.base_url}{path}",
            params={key: value for key, value in (params or {}).items() if value is not None},
        )
        _raise_for_status(response)
        return response.json()
    @staticmethod
    def _experiment_params(filters: dict[str, Any]) -> dict[str, Any]:
        params = dict(filters)
        if params.get("metadata") is not None:
            params["metadata"] = json.dumps(params["metadata"], sort_keys=True)
        for key in ("started_after", "started_before"):
            value = params.get(key)
            if isinstance(value, datetime):
                params[key] = value.isoformat()
        return params

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
        # the graph may bind new prefixes: read the list again when next needed
        self._namespaces_cache = None

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
        }
        headers = {"Accept": "application/vnd.apache.arrow.stream"}

        with self._http.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            r.raise_for_status()

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
                    return pl.DataFrame()
            except Exception as e:
                logger.error(f"Error reading Arrow IPC stream: {e}")
                return pl.DataFrame()
            

    def timeseries_batches(
        self,
        uri: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None,
        order: Order = "asc",
        *,
        value_mode: str = "default",
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
        }
        headers = {"Accept": "application/vnd.apache.arrow.stream"}

        # stream=True means requests won't buffer the full body into memory
        with self._http.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            r.raise_for_status()

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

        Accepts a CURIE only: a full URI (``urn:``, ``http://``, ...) or an
        unbound prefix raises ``ValueError``. Non-string inputs are cast to
        ``str``. Use :meth:`node_uri` where either form is acceptable.
        """
        s = str(text)
        nm = self.namespace_manager()
        try:
            return str(nm.expand_curie(s))
        except Exception as e:
            raise ValueError(f"Cannot expand '{s}': no matching namespace for CURIE and not a full URI")

    def node_uri(self, text: Any) -> str:
        """A node given as a full URI or a bound CURIE, as a full URI string."""
        s = str(text)
        if s.startswith(("urn:", "http://", "https://")):
            return s
        return self.expand_uri(s)

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
        response.raise_for_status()
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
        response.raise_for_status()
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

    def insert_log(
        self,
        point_uri: Optional[str] = None,
        log_time: Optional[str] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        log_message: str = "",
    ) -> dict:
        """
        Insert a log entry for a given point URI.

        Args:
            point_uri: The URI of the time series point. If None, defaults to the
                generic plant URI on the server.
            log_time: The timestamp of the log entry in ISO 8601 format.
                If None, uses the current time.
            log_message: The log message.
        Returns:
            A dictionary with the result of the insertion.
        """
        if log_time is None:
            log_time = datetime.now(timezone.utc).isoformat()
        url = f"{self.base_url}/insert_log"
        data = {
            "log_timestamp": log_time,
            "observation_start": observation_start,
            "observation_end": observation_end,
            "message": log_message,
        }
        if point_uri is not None:
            data["point_uri"] = point_uri
        response = self._http.post(url, params=data)
        response.raise_for_status()
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

    def reprocess_app(self, name: str, start: datetime, end: datetime) -> dict:
        response = self._http.post(
            f"{self.base_url}/apps/{name}/reprocess",
            params={"start": start.isoformat(), "end": end.isoformat()},
        )
        _raise_for_status(response)
        return response.json()

    def check_app(self, definition: dict, limit: int | None = None,
                  search_path: str | None = None) -> dict:
        params = {}
        if limit is not None:
            params["limit"] = limit
        if search_path is not None:
            params["search_path"] = search_path
        response = self._http.post(
            f"{self.base_url}/apps/check", json=definition, params=params
        )
        _raise_for_status(response)
        return response.json()

    def materialization_dag(self) -> dict:
        response = self._http.get(f"{self.base_url}/materialization/dag")
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
        response.raise_for_status()
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
        from acquirium.Client.metadata import plan_write, update_text
        from acquirium.Storage.graph_registry import METADATA_SOURCE_ID

        graphs: dict[str, Graph] = {}
        # the ``metadata`` maps per point; a later stream on the same point
        # wins key by key
        extras: dict[str, dict[str, Any]] = {}
        for stream in streams:
            stream, extra = _split_stream_metadata(stream)
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
            subj = _build_stream_triples(graph, stream, resolved, existing, self._units_compatible)
            if extra:
                extras.setdefault(str(subj), {}).update(extra)
        # Plan the metadata before writing anything, so a bad key or a value
        # that does not resolve fails the whole registration.
        namespaces = self._namespace_uris() if extras else ()
        writes = [plan_write(subj, extra, role="data", resolve=self._resolve_one,
                             namespaces=namespaces)
                  for subj, extra in extras.items()]
        for source_id, graph in graphs.items():
            if len(graph):
                self.insert_graph(
                    graph.serialize(format="turtle"),
                    format="turtle",
                    replace=False,
                    source_id=source_id,
                )
        # Same map and same update as insert_metadata: each key is replaced
        # in the metadata graph, so registering again with a changed value
        # leaves only the new one. Runs after the stream triples, which make
        # the point a node the metadata graph may describe.
        update = update_text(writes)
        if update:
            self.sparql_update(update, source_id=METADATA_SOURCE_ID)

    def _resolve_one(self, text: str, kind: str) -> Optional[str]:
        """Resolve one text value of the given kind to a URI, or None."""
        hits = self.resolve({"v": (text, kind)}, min_score=0.4) or {}
        return hits.get("v")

    def node_roles(self, uris: Iterable[str]) -> dict[str, str]:
        """``{uri: "data" | "entity"}`` for the nodes the data graphs know.

        A node is a data node when it carries an external reference, the
        same rule the explore layer's measurement nodes follow. A URI the
        graphs do not mention (as subject or object) is left out, and so is
        one only user attributes mention.
        """
        from acquirium.Client.explore.attributes import ATTR_NS

        uris = list(dict.fromkeys(str(u) for u in uris))
        roles: dict[str, str] = {}
        for i in range(0, len(uris), 500):
            chunk = " ".join(f"<{u}>" for u in uris[i:i + 500])
            # a URI that only user attributes mention (a link saved as a
            # value) is not a node: metadata on it would be pruned at once
            sparql = (
                "SELECT DISTINCT ?s ?ref\nWHERE {\n"
                f"  VALUES ?s {{ {chunk} }}\n"
                f'  {{ ?s ?p ?o . FILTER(!STRSTARTS(STR(?p), "{ATTR_NS}")) }}\n'
                f'  UNION {{ ?x ?q ?s . FILTER(!STRSTARTS(STR(?q), "{ATTR_NS}")) }}\n'
                f"  OPTIONAL {{ ?s <{HAS_EXTERNAL_REFERENCE}> ?ref . }}\n}}"
            )
            res = self.sparql_query(sparql, include_dependencies=False)
            cols = res.get("columns", [])
            si = cols.index("s") if "s" in cols else 0
            ri = cols.index("ref") if "ref" in cols else 1
            for row in res.get("rows", []):
                s = str(row[si])
                if row[ri] is not None or s not in roles:
                    roles[s] = "data" if row[ri] is not None else "entity"
        return roles

    def _namespace_uris(self) -> tuple[str, ...]:
        """The namespace URIs the server binds, from the cached prefix list:
        a URI-looking metadata value under one of them is written as a URI
        (see :func:`acquirium.Client.metadata.to_term`)."""
        return tuple(str(ns) for _, ns in self.namespace_manager().namespaces())

    def insert_metadata(self, records: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        """Write value maps on nodes: ``{node_uri: {key: value}}``.

        See :mod:`acquirium.Client.metadata` for what a value map may hold.
        Subjects may be CURIEs. Every subject must already exist in a data
        graph; text on a URI-valued built-in is resolved. A URI-looking
        string is written as a URI when it falls under a namespace the
        server binds, else as text; pass a ``URIRef`` to write a URI
        regardless. Writes land in the
        reserved metadata source graph as one SPARQL update, so each key is
        replaced atomically and nothing outside that graph is touched.
        """
        from acquirium.Client.metadata import plan_write, update_text
        from acquirium.Storage.graph_registry import METADATA_SOURCE_ID

        subjects = {self.node_uri(uri): values for uri, values in records.items()}
        if not subjects:
            return {"ok": True, "nodes": 0}
        roles = self.node_roles(subjects)
        missing = sorted(set(subjects) - set(roles))
        if missing:
            raise ValueError(f"unknown node(s), not in any data graph: {missing}")
        namespaces = self._namespace_uris()
        writes = [
            plan_write(uri, values, role=roles[uri], resolve=self._resolve_one,
                       namespaces=namespaces)
            for uri, values in subjects.items()
        ]
        update = update_text(writes)
        if not update:
            return {"ok": True, "nodes": len(writes)}
        result = self.sparql_update(update, source_id=METADATA_SOURCE_ID)
        return {**result, "nodes": len(writes)}

    def metadata_documents(self, subjects: Iterable[str]) -> dict[str, dict]:
        """Read complete typed metadata from the authoritative source graph."""
        from acquirium.Metadata import loads

        response = self._http.post(f"{self.base_url}/metadata/documents", json=list(subjects))
        _raise_for_status(response)
        return {subject: loads(document) for subject, document in response.json().items()}

    def insert_timeseries(
        self,
        *,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        point_uri: Optional[str] = None,
        replace: bool = False,
        publication_id: Optional[str] = None,
    ) -> dict:
        url = f"{self.base_url}/insert_timeseries"
        body = StreamInsert(
            source_id=source_id,
            ref_name=ref_name,
            point_uri=point_uri,
            replace=replace,
            values=rows,
            publication_id=publication_id,
        )
        response = self._http.post(url, json=[body.model_dump(mode="json")])
        _raise_for_status(response)
        return response.json()

    def insert_timeseries_batch(
        self,
        source_id: str,
        streams: dict[str, list[tuple[datetime, Any]]],
    ) -> dict:
        """Insert timeseries data for multiple streams in one HTTP request.

        Args:
            source_id: The registered datasource identifier.
            streams: Mapping of ref_name → list of (timestamp, value) tuples.
        """
        url = f"{self.base_url}/insert_timeseries"
        payload = [
            StreamInsert(
                source_id=source_id,
                ref_name=rn,
                values=rows,
            )
            for rn, rows in streams.items()
        ]
        response = self._http.post(url, json=[s.model_dump(mode="json") for s in payload])
        _raise_for_status(response)
        return response.json()

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, publication_id: Optional[str] = None
    ) -> dict[str, Any]:
        import io
        import pyarrow as pa
        table_with_sid = table.append_column("source_id", pa.repeat(source_id, len(table)))
        buf = io.BytesIO()
        with ipc.new_stream(buf, table_with_sid.schema) as writer:
            writer.write_table(table_with_sid)
        buf.seek(0)
        headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
        if publication_id is not None:
            headers["X-Acquirium-Publication-Id"] = publication_id
        response = self._http.post(
            f"{self.base_url}/insert_timeseries_arrow",
            data=buf,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()

    def query_logs(
        self,
        point_uri: Optional[str] = None,
        log_time_start: Optional[str] = None,
        log_time_end: Optional[str] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
    ) -> list[LogEntry]:
        """
        Query log entries for a given point URI within optional time intervals.

        Args:
            point_uri: The URI of the time series point. If None, defaults to the
                generic plant URI (retrieves plant-level logs).
            log_time_start: Start of the log time interval in ISO 8601 format.
            log_time_end: End of the log time interval in ISO 8601 format.
            observation_start: Start of the observation time interval in ISO 8601 format.
            observation_end: End of the observation time interval in ISO 8601 format.

        Returns:
            A list of log entries matching the query.
        """
        url = f"{self.base_url}/query_logs"
        params = {
            "point_uri": point_uri,
            "log_time_start": log_time_start,
            "log_time_end": log_time_end,
            "observation_start": observation_start,
            "observation_end": observation_end,
        }
        params = {k: v for k, v in params.items() if v is not None}
        response = self._http.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return [LogEntry.model_validate(x) for x in data]

    def delete_logs(self, point_uri: Optional[str] = None) -> dict:
        """
        Delete all log entries for a given point URI.

        Args:
            point_uri: The URI of the time series point. If None, defaults to the
                generic plant URI.
        """
        url = f"{self.base_url}/delete_logs"
        params = {}
        if point_uri is not None:
            params["point_uri"] = point_uri
        response = self._http.delete(url, params=params)
        response.raise_for_status()
        return response.json()

