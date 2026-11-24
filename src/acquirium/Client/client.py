from typing import Optional, Iterator, Iterable, Any, Mapping, TYPE_CHECKING
from datetime import datetime
import json
import re
import time
import uuid
import warnings
import requests
from requests import HTTPError
from pathlib import Path
from urllib.parse import quote
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    MAX_REQUEST_ROWS,
    Order,
    StreamInsert,
    LogEntry, compute_stream_uri,
    looks_like_uri,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Client.explore.attributes import ATTR_NS, REGISTRY, RESOLVE_MIN_SCORE, raw_predicate
from acquirium.Client.metadata import is_uri, plan_write, update_text
from acquirium.Storage.graph_registry import METADATA_SOURCE_ID
from acquirium.Storage.values import normalize_value_mode
from rdflib import Graph, URIRef
from rdflib.namespace import NamespaceManager, RDFS
import logging
from acquirium.internals._names import accepts_old_names, new_names
logger = logging.getLogger(__name__)

DEFAULT_HEALTH_REQUEST_TIMEOUT = 30.0
# Seconds a write the server refuses with 503 is resent for by default. A 503
# means a full inbox, or the engine or its database unavailable; nothing
# committed, so the same request can be sent again.
CAPACITY_RETRY_S = 60.0
_CURIE = re.compile(r"[A-Za-z_][\w.-]*:[^\s:]+")


def _insert_chunks(streams, replace, size=MAX_REQUEST_ROWS):
    """Split ``{stream_name: rows}`` into requests of at most ``size`` rows, as
    ``(stream_name, rows, replace)`` items. A replaced stream's clear counts as a
    row and travels with its first piece, so it lands before that stream's
    rows. Every stream appears at least once, so the server checks the name
    of a stream with no rows too."""
    chunk, used = [], 0
    for name, rows in streams.items():
        start, clear = 0, name in replace
        while True:
            if chunk and used + clear >= size:
                yield chunk
                chunk, used = [], 0
            room = size - used - clear
            piece = rows[start:start + room]
            chunk.append((name, piece, clear))
            used += len(piece) + clear
            start, clear = start + room, False
            if start >= len(rows):
                break
    if chunk:
        yield chunk


def _send_in_order(chunks, request_id, wait, send):
    """Send each chunk as its own request with ``send(chunk, request_id, wait)``
    and combine the replies. Several chunks get ``request_id:index``, so a
    retry resends the same requests; only the last waits for publication,
    which covers the ones before it."""
    result = {"ok": True, "rows_inserted": 0, "receipts": []}
    for index, chunk in enumerate(chunks):
        last = index == len(chunks) - 1
        reply = send(chunk, request_id if len(chunks) == 1 else f"{request_id}:{index}",
                     wait if last else 0)
        result["rows_inserted"] += reply["rows_inserted"]
        result["receipts"] += reply["receipts"]
    return result

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
    POINT_FIELD_KINDS, POINT_FIELD_PREDICATES, _build_stream_triples, _coerce_resolved,
)

# Fields a stream registration already takes at top level; a ``metadata``
# key naming one of them is merged into the stream.
STREAM_FIELDS = frozenset({"unit", "quantity_kind", "medium", "substance", "data_source", "label"})


# Built-in keys of a property that hold one value: see ``_settle_builtins``.
SETTLED_KEYS = ("unit", "quantity_kind", "medium", "substance", "label")


def _builtin_removal(nodes: Mapping[str, Iterable[str]]) -> str:
    """A SPARQL update removing built-in keys from nodes, ``{node: [key]}``:
    every predicate the key is read from, and the text it was resolved from."""
    ops = []
    for node, keys in nodes.items():
        predicates = [
            pred for key in dict.fromkeys(keys)
            for pred in (*REGISTRY[key].predicates, raw_predicate(key))
        ]
        if predicates:
            terms = " || ".join(f"?p = <{pred}>" for pred in predicates)
            ops.append(f"DELETE {{ <{node}> ?p ?o }}\nWHERE {{\n  <{node}> ?p ?o .\n  FILTER({terms})\n}}")
    return " ;\n".join(ops)


def _split_stream_metadata(stream: dict) -> tuple[dict, dict]:
    """Return ``(stream with metadata fields merged in, remaining metadata)``.

    A ``metadata`` key naming a top-level stream field (``unit``, ``label``,
    ...) is the same declaration and merges into the stream; giving both
    with different values is an error. Everything else (user attributes,
    other built-ins) is written on the point by the metadata builder.
    """
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


@accepts_old_names
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
    def record_experiment_batch(self, run_id: str, entries: list[dict], *, timeout: float = CAPACITY_RETRY_S) -> list[dict]:
        """Record a batch, resending it for up to ``timeout`` seconds while the
        server answers 503."""
        return self._post_admitted(f"/experiments/runs/{run_id}/record", retry_for=timeout,
                                   json=entries).json()

    def observe_experiment(self, run_id: str, variable_id: str, **body: Any) -> dict: return self._experiment_post(f"/experiments/runs/{run_id}/variables/{quote(variable_id, safe='')}/observations", body)
    def observe_experiment_batch(self, run_id: str, observations: list[dict]) -> list[dict]:
        return self._experiment_post(f"/experiments/runs/{run_id}/observations/batch", observations)
    def annotate_experiment_stream(self, run_id: str, stream_uri: str, *, start: str, end: str | None = None, metadata: dict | None = None) -> dict:
        return self._experiment_post(f"/experiments/runs/{run_id}/stream-annotations", {"stream_uri": stream_uri, "start": start, "end": end, "metadata": {} if metadata is None else metadata})
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
        epoch: int | None = None,
        end_ns: int | None = None,
        after: int | None = None,
        view: str = "current",
    ) -> pl.DataFrame:
        """
        Fetch the full timeseries payload and return a single Polars DataFrame.
        Best for small to medium responses. Bounds are half-open; ``end_ns``
        gives the end in UTC nanoseconds instead of ``end``. ``after`` is a
        request sequence the read waits for and includes.
        ``view`` selects the final replacement ("current"), original input
        ("raw"), or exact physical stream ("stored") at the read's epoch.
        """
        value_mode = normalize_value_mode(value_mode)
        url = f"{self.base_url}/timeseries"
        params = {
            "uri": uri,
            "start": start,
            "end": end,
            "end_ns": end_ns,
            "limit": limit,
            "order": order,
            "value_mode": value_mode,
            "epoch": epoch,
            "after": after,
            "view": view,
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
        after: int | None = None,
        view: str = "current",
    ) -> Iterator[pl.DataFrame]:
        """
        Stream an Arrow IPC response and yield Polars DataFrames per RecordBatch.

        This requires the server to return a *single* Arrow IPC stream that contains
        multiple record batches (not multiple concatenated streams).
        """
        value_mode = normalize_value_mode(value_mode)
        url = f"{self.base_url}/timeseries"
        params = {
            "uri": uri,
            "start": start,
            "end": end,
            "limit": limit,
            "order": order,
            "value_mode": value_mode,
            "epoch": epoch,
            "view": view,
            "after": after,
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

    def timeseries_info_batch(self, uris: list[str], *, view="current") -> dict:
        """Fetch current stats (row_count, earliest, latest) for multiple URIs in one request."""
        from acquirium.internals.models import TimeseriesInfo
        url = f"{self.base_url}/timeseries_info"
        response = self._http.post(url, json={"uris": uris, "view": view})
        _raise_for_status(response)
        data = response.json()
        return {uri: TimeseriesInfo.model_validate(info) for uri, info in data.items()}

    def timeseries_exists_batch(self, uris: list[str], *, view="current") -> set[str]:
        """The URIs among ``uris`` that have at least one row, in one request.

        Cheaper than ``timeseries_info_batch``, which counts every row.
        """
        response = self._http.post(f"{self.base_url}/timeseries_exists", json={"uris": uris, "view": view})
        _raise_for_status(response)
        return set(response.json()["uris"])

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
        except Exception:
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

    def resolve_point_metadata(self, *args, **kwargs):
        """Alias of :meth:`resolve_property_metadata`."""
        return self.resolve_property_metadata(*args, **kwargs)

    def resolve_property_metadata(
        self, fields: dict[str, Any], min_score: float = 0.6
    ) -> dict[str, str | None]:
        """Resolve the semantic metadata of a single point/property.

        ``fields`` maps known semantic field names to a raw value::

            client.resolve_property_metadata({
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

    def insert_log(
        self,
        property_uri: Optional[str] = None,
        log_time: Optional[str] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        log_message: str = "",
    ) -> dict:
        """
        Insert a log entry for a given property URI.

        Args:
            property_uri: The URI of the time series point. If None, defaults to the
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
        if property_uri is not None:
            data["property_uri"] = property_uri
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

    def deploy_app(self, definition: dict, *, dry_run: bool = False) -> dict:
        response = self._http.put(
            f"{self.base_url}/apps/{definition['name']}",
            json=definition,
            params={"dry_run": "true"} if dry_run else None,
        )
        _raise_for_status(response)
        return response.json()

    def remove_app(self, name: str) -> dict:
        response = self._http.delete(f"{self.base_url}/apps/{name}")
        _raise_for_status(response)
        return response.json()

    def stream_views(self, uris: list[str]) -> dict[str, dict]:
        """For each stream reference, the streams its ``current`` and ``raw``
        views read now, and the unit (or None) its current view reports."""
        if not uris:
            return {}
        response = self._http.post(f"{self.base_url}/stream_views", json={"uris": uris})
        _raise_for_status(response)
        return response.json()

    def resolve_storage_keys(self, uris: list[str], *, view="current", epoch=None) -> dict[str, str]:
        """Resolve points and streams to the requested replacement view.

        ``current`` selects the chain head, ``raw`` the base, and ``stored``
        the requested stage. ``epoch`` selects a historical plan.
        """
        if not uris:
            return {}
        url = f"{self.base_url}/resolve_storage_keys"
        response = self._http.post(url, json={"uris": uris, "view": view, "epoch": epoch})
        _raise_for_status(response)
        return response.json()

    def _units_compatible(self, from_unit: Any, to_unit: Any) -> bool:
        """True when the server can convert between the two unit URIs."""
        try:
            factors = self.get_conversion_factors(str(from_unit), str(to_unit))
            return bool(factors.get("compatible"))
        except Exception:
            return False

    def _property_metadata(self, property_uri: str) -> dict[str, "str | None"]:
        """Return the semantic metadata a point already has in the graph.

        Keys match the stream-registration field names (``unit``,
        ``quantity_kind``, ``medium``, ``substance``, ``data_source``);
        a field the point lacks maps to ``None``. Returns ``{}`` for a
        point the graph does not know at all.
        """
        fields = {**POINT_FIELD_PREDICATES, "data_source": DATA_SOURCE}
        select = " ".join(f"?{name}" for name in fields)
        optionals = "\n".join(
            f"            OPTIONAL {{ <{property_uri}> <{pred}> ?{name} . }}"
            for name, pred in fields.items()
        )
        sparql = f"SELECT {select} WHERE {{\n{optionals}\n        }}"
        result = self.sparql_query(sparql)
        rows = result.get("rows") or []
        if not rows:
            return {}
        return dict(zip(result["columns"], rows[0]))

    def _builtin_values(self, property_uri: str) -> dict[str, set[str]]:
        """``{key: {value, ...}}`` for the unit, quantity kind, medium and
        substance of a property, read from every predicate the key has
        (``insert_metadata`` writes the medium of a property with another
        predicate than a registration does)."""
        keys = {
            pred: key for key in POINT_FIELD_PREDICATES for pred in REGISTRY[key].predicates
        }
        terms = " ".join(f"<{pred}>" for pred in keys)
        result = self.sparql_query(
            f"SELECT ?p ?o WHERE {{ VALUES ?p {{ {terms} }} <{property_uri}> ?p ?o . }}",
            include_dependencies=False,
        )
        found: dict[str, set[str]] = {}
        for pred, value in result.get("rows") or []:
            found.setdefault(keys[str(pred)], set()).add(str(value))
        return found

    def _stream_properties(self, stream_uris: Iterable[str]) -> dict[str, list[str]]:
        """``{stream_uri: [property_uri, ...]}`` for the streams the graph
        links to a property; a stream without one is left out."""
        uris = list(dict.fromkeys(str(u) for u in stream_uris))
        found: dict[str, list[str]] = {}
        for i in range(0, len(uris), 500):
            chunk = " ".join(f"<{u}>" for u in uris[i:i + 500])
            result = self.sparql_query(
                f"SELECT DISTINCT ?s ?p WHERE {{ VALUES ?s {{ {chunk} }} "
                f"?p <{HAS_EXTERNAL_REFERENCE}> ?s . }}",
                include_dependencies=False,
            )
            for stream_uri, property_uri in result.get("rows") or []:
                found.setdefault(str(stream_uri), []).append(str(property_uri))
        return {uri: sorted(properties) for uri, properties in found.items()}

    def stream_property(self, stream_uri: str) -> str:
        """The property a stream is linked to: where its metadata is written."""
        properties = self._stream_properties([str(stream_uri)]).get(str(stream_uri), [])
        if not properties:
            raise ValueError(f"stream <{stream_uri}> is not registered")
        if len(properties) > 1:
            raise ValueError(
                f"stream <{stream_uri}> is connected to several properties "
                f"({', '.join(properties)}); pass the URI of one of them"
            )
        return properties[0]

    def _node_labels(self, node_uris: Iterable[str]) -> dict[str, list[str]]:
        """``{node_uri: [label, ...]}``, sorted; a node without a label is
        left out."""
        uris = list(dict.fromkeys(str(u) for u in node_uris))
        found: dict[str, list[str]] = {}
        for i in range(0, len(uris), 500):
            chunk = " ".join(f"<{u}>" for u in uris[i:i + 500])
            result = self.sparql_query(
                f"SELECT DISTINCT ?n ?label WHERE {{ VALUES ?n {{ {chunk} }} "
                f"?n <{RDFS.label}> ?label . }}",
                include_dependencies=False,
            )
            for node, label in result.get("rows") or []:
                found.setdefault(str(node), []).append(str(label))
        return {uri: sorted(labels) for uri, labels in found.items()}

    def register_streams(
        self,
        streams: Iterable[dict[str, Any]],
    ) -> None:
        """Declare one or more streams' semantic metadata in one graph insert.

        Each stream item is a dictionary with these commonly used keys:

        - ``property_uri``: optional semantic URI, or a CURIE with a prefix
          the server binds (``"wbs:P1-out-pressure"``). When provided, the point is
          linked to the external reference and its provided metadata is
          checked against what the graph already has (see below). When
          omitted, a dummy point (``<stream_uri>__point``) is minted, linked
          to the external reference and marked ``acq:isDisconnected``
          (``where(is_disconnected=True)`` lists such streams).
        - ``source_id`` and ``stream_name``: source-local stream identity. When
          both are present, Acquirium mints the canonical reference URI and
          writes ``acq:sourceId``, ``acq:refName``, and ``ref:storedAt`` on it.
        - ``label``: optional ``rdfs:label`` written on the point. A dummy
          point without one gets its ``stream_name`` (e.g. the CSV column name).
          Registering again with a ``label`` replaces the label this source
          wrote; registering again without one keeps it.
        - ``data_source``: optional datasource marker written on the point.
        - ``properties``: optional mapping of predicate URIRefs to values,
          written on the reference node (or the point when no ref node exists).

        Plain strings for ``unit``/``quantity_kind``/``medium``/``substance``
        are resolved jointly per stream via :meth:`resolve_property_metadata`;
        URI / URIRef / None values pass through.

        For a ``property_uri`` the graph already knows, a provided field the
        point lacks is added to it; a value that conflicts with the point's
        raises ``ValueError`` before anything is inserted. Units are the
        one exception: a provided unit that differs from the point's is
        accepted when the two are convertible, and is written on the
        external reference as the unit the raw rows are stored in — data
        access defaults to the point's unit and converts automatically.
        A non-convertible unit raises ``ValueError``.

        A stream registered without a ``property_uri`` can be connected
        later: register it again with one. The dummy point is then replaced
        by ``property_uri``, and what was attached to it moves along: its
        unit, quantity kind and medium go through the checks above, so a
        mismatch raises ``ValueError`` and nothing is written. Its substance,
        data source and the keys of its own are written on ``property_uri``
        where it has none; where it has a different value, it keeps its own
        and a warning says so (use ``insert_metadata`` to change it). A value
        given in the call wins over both. The dummy point's label moves only
        when ``property_uri`` has none. Once connected, registering the
        stream again without a ``property_uri`` describes that property.

        Drivers should still insert rows with ``insert_timeseries_arrow`` using
        the same ``source_id`` and source-local ``stream_name``. Acquirium resolves
        those inserts to the same canonical reference URI internally.
        """
        # ``point_uri`` and the deprecated ``ref_name`` are still accepted
        items = [_split_stream_metadata(new_names(stream)) for stream in streams]
        for stream, _ in items:
            source_id = stream.get("source_id")
            if not isinstance(source_id, str) or not source_id:
                raise ValueError("each stream registration requires a non-empty source_id")
            # a property_uri may be a CURIE, as metadata() prints them
            given = stream.get("property_uri")
            if given is not None and not str(given).startswith(("urn:", "http://", "https://")):
                expanded = self._expand_curie(str(given))
                if expanded is not None:
                    stream["property_uri"] = expanded

        def stream_uri_of(stream: dict) -> Optional[str]:
            if stream.get("stream_name") is None:
                return None
            return str(compute_stream_uri(stream["source_id"], stream["stream_name"]))

        # What the graph already holds for these streams: the properties each
        # one is linked to, and the labels of every property involved.
        linked = self._stream_properties(
            uri for uri in (stream_uri_of(stream) for stream, _ in items) if uri is not None
        )
        nodes: list[str] = []
        for stream, _ in items:
            stream_uri = stream_uri_of(stream)
            if stream.get("property_uri") is not None:
                nodes.append(str(stream["property_uri"]))
            if stream_uri is not None:
                nodes.append(f"{stream_uri}__point")
                nodes.extend(linked.get(stream_uri, []))
        labels = self._node_labels(nodes)

        graphs: dict[str, Graph] = {}
        # the ``metadata`` maps per point; a later stream on the same point
        # wins key by key
        extras: dict[str, dict[str, Any]] = {}
        # properties whose label is being replaced, per source
        relabeled: dict[str, list[str]] = {}
        # generated properties replaced by a plant property, per source
        replaced: dict[str, list[str]] = {}
        # built-in keys of a generated property that get a new value, per
        # source: {source_id: {node: [key, ...]}}
        rewritten: dict[str, dict[str, list[str]]] = {}
        for stream, extra in items:
            source_id = stream["source_id"]
            graph = graphs.setdefault(source_id, Graph())
            stream_uri = stream_uri_of(stream)
            generated = f"{stream_uri}__point" if stream_uri is not None else None
            properties = linked.get(stream_uri, []) if stream_uri is not None else []
            property_uri = stream.get("property_uri")
            carried: dict[str, Any] = {}
            connecting = False
            if property_uri is None and generated not in properties and properties:
                # Already connected: registering again without a property_uri
                # describes the property the stream is connected to.
                if len(properties) > 1:
                    raise ValueError(
                        f"stream {stream['source_id']!r}/{stream['stream_name']!r} is "
                        f"connected to several properties ({', '.join(properties)}); "
                        "pass property_uri to choose one"
                    )
                property_uri = properties[0]
                stream = {**stream, "property_uri": property_uri}
            elif (property_uri is not None and generated in properties
                    and str(property_uri) != generated):
                # Connecting a stream that was registered on its own: the
                # generated property is replaced by the given one, and what
                # was attached to it moves along. A value given in this call
                # wins over the one the generated property had.
                stream, carried = self._carry_generated_property(
                    stream, extra, generated, str(property_uri), labels)
                replaced.setdefault(source_id, []).append(generated)
                connecting = True
            meta = {
                f: stream.get(f)
                for f in POINT_FIELD_KINDS
                if stream.get(f) is not None
            }
            resolved = self.resolve_property_metadata(meta) if meta else {}
            existing = self._property_metadata(str(property_uri)) if property_uri is not None else {}
            if property_uri is None and generated in properties:
                # Registered before, still on its own. The property is
                # Acquirium's, so a new value replaces the old one (as
                # insert_metadata does) instead of being checked against it.
                held = self._builtin_values(generated)
                changed = [
                    f for f in POINT_FIELD_PREDICATES
                    if held.get(f)
                    and (resolved.get(f) is not None or is_uri(stream.get(f)))
                    and held[f] != {str(_coerce_resolved(resolved, f, stream.get(f)))}
                ]
                if changed:
                    rewritten.setdefault(source_id, {})[generated] = changed
            # A property keeps one label per source: a given label replaces
            # the ones this source wrote, and the stream_name default is
            # only used while the generated property has no label.
            node = str(property_uri) if property_uri is not None else generated
            current = labels.get(node, []) if node is not None else []
            if stream.get("label") is None:
                if property_uri is None and current:
                    stream = {**stream, "label": current[0]}
            elif current != [str(stream["label"])]:
                relabeled.setdefault(source_id, []).append(str(node))
            try:
                subj = _build_stream_triples(graph, stream, resolved, existing, self._units_compatible)
            except ValueError as exc:
                if not connecting:
                    raise
                raise ValueError(
                    f"{exc}. The stream's value comes from its earlier registration; "
                    "pass the key in this call to say which value is right"
                ) from None
            moved = {**carried, **extra}
            if moved:
                extras.setdefault(str(subj), {}).update(moved)
        writes = self._plan_metadata(extras, dict.fromkeys(extras, "data"))
        for source_id, graph in graphs.items():
            if relabeled.get(source_id):
                self.sparql_update(
                    " ;\n".join(
                        f"DELETE WHERE {{ <{node}> <{RDFS.label}> ?label }}"
                        for node in dict.fromkeys(relabeled[source_id])
                    ),
                    source_id=source_id,
                )
            removal = _builtin_removal(rewritten.get(source_id, {}))
            if removal:
                self.sparql_update(removal, source_id=source_id)
        # A value insert_metadata wrote on a generated property is replaced
        # as well, so the property is left with the one value of this call.
        own = {
            node: keys for nodes in rewritten.values() for node, keys in nodes.items()
        }
        for source_id, nodes in relabeled.items():
            for node in nodes:
                if node.endswith("__point"):
                    own.setdefault(node, [])
                    own[node] = [*own[node], "label"]
        removal = _builtin_removal(own)
        if removal:
            self.sparql_update(removal, source_id=METADATA_SOURCE_ID)
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
        self._write_metadata(writes)
        # Last, so a failure above leaves the generated properties in place.
        for source_id, generated_nodes in replaced.items():
            removal = " ;\n".join(
                f"DELETE WHERE {{ <{node}> ?p ?o }}" for node in dict.fromkeys(generated_nodes)
            )
            self.sparql_update(removal, source_id=source_id)
            self.sparql_update(removal, source_id=METADATA_SOURCE_ID)

    def _carry_generated_property(
        self, stream: dict, extra: dict, generated: str, property_uri: str,
        labels: Mapping[str, list[str]],
    ) -> tuple[dict, dict[str, Any]]:
        """What moves from a stream's generated property to ``property_uri``.

        Returns the stream with the generated property's built-in fields
        filled in where the call gave none, and the keys of its own to write
        on ``property_uri``. Where both properties have a value and the two
        differ, ``property_uri`` keeps its own and a warning says so. The
        unit, quantity kind and medium are the exception: they are carried
        regardless, so the checks of a registration with a ``property_uri``
        apply to them and a mismatch raises.
        """
        from acquirium.Metadata import dumps

        def kept(key: str, ours: Any, theirs: Any) -> None:
            warnings.warn(
                f"{key!r} of stream {stream['source_id']!r}/{stream['stream_name']!r} "
                f"is not moved: <{property_uri}> has {theirs!r}, the stream had "
                f"{ours!r}. Use insert_metadata() to change it",
                stacklevel=4,
            )

        stream = dict(stream)
        had = self._property_metadata(generated)
        has = self._property_metadata(property_uri)
        for field in (*POINT_FIELD_KINDS, "data_source"):
            value = had.get(field)
            if stream.get(field) is not None or value is None:
                continue
            current = has.get(field)
            if (field in ("substance", "data_source") and current is not None
                    and str(current) != str(value)):
                kept(field, value, current)
                continue
            stream[field] = URIRef(str(value)) if field in POINT_FIELD_KINDS else value
        # The default label is the stream_name, which names the generated
        # property only; a label that was given moves when the property has none.
        if stream.get("label") is None and not labels.get(property_uri):
            given = [label for label in labels.get(generated, []) if label != stream.get("stream_name")]
            if given:
                stream["label"] = given[0]
        documents = self.metadata_documents([generated, property_uri])
        ours, theirs = documents.get(generated, {}), documents.get(property_uri, {})
        carried: dict[str, Any] = {}
        for key, value in ours.items():
            if key in STREAM_FIELDS or key in extra:
                continue
            if key not in theirs:
                carried[key] = value
            elif dumps(theirs[key]) != dumps(value):
                kept(key, value, theirs[key])
        return stream, carried

    def _resolve_one(self, text: str, kind: str) -> Optional[str]:
        """Resolve one text value of the given kind to a URI, or None."""
        hits = self.resolve({"v": (text, kind)}, min_score=RESOLVE_MIN_SCORE) or {}
        return hits.get("v")

    def node_roles(self, uris: Iterable[str]) -> dict[str, str]:
        """``{uri: "data" | "entity"}`` for the nodes the data graphs know.

        A node is a data node when it carries an external reference, the
        same rule the explore layer's measurement nodes follow. A URI the
        graphs do not mention (as subject or object) is left out, and so is
        one only user attributes mention.
        """
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
        subjects = {self.node_uri(uri): values for uri, values in records.items()}
        if not subjects:
            return {"ok": True, "nodes": 0}
        roles = self.node_roles(subjects)
        missing = sorted(set(subjects) - set(roles))
        if missing:
            raise ValueError(f"unknown node(s), not in any data graph: {missing}")
        writes = self._plan_metadata(subjects, roles)
        for owner, nodes in self._settle_builtins(subjects, roles, writes).items():
            self.sparql_update(_builtin_removal(nodes), source_id=owner)
        return {**self._write_metadata(writes), "nodes": len(writes)}

    def asserted_values(self, subjects: Iterable[str]) -> dict[str, list[list[str]]]:
        """``{node: [[predicate, value, source_id], ...]}``: what the plant
        model and the stream registrations wrote on nodes, without what
        ``insert_metadata`` wrote."""
        response = self._http.post(f"{self.base_url}/metadata/asserted", json=list(subjects))
        _raise_for_status(response)
        return response.json()

    def _settle_builtins(self, subjects: Mapping[str, Mapping[str, Any]],
                         roles: Mapping[str, str], writes: list) -> dict[str, dict[str, list[str]]]:
        """Apply the rule of ``register_streams`` to the built-in keys of a
        property: unit, quantity kind, medium, substance and label.

        - A property Acquirium generated for a stream holds one value: the
          value another source wrote is removed. Returned as
          ``{source_id: {node: [key, ...]}}`` for the caller to delete.
        - On any other property the value of the plant model, or of the
          stream registration that wrote it, stays. A different value
          raises ``ValueError``. The label is left out of this check: a
          property has one label per source.
        """
        wanted = {
            uri: [key for key in SETTLED_KEYS if key in values]
            for uri, values in subjects.items() if roles.get(uri) == "data"
        }
        wanted = {uri: keys for uri, keys in wanted.items() if keys}
        if not wanted:
            return {}
        asserted = self.asserted_values(wanted)
        planned = {str(w.subject): w for w in writes}
        removals: dict[str, dict[str, list[str]]] = {}
        for uri, keys in wanted.items():
            rows = asserted.get(uri, [])
            generated = any(pred == str(IS_DISCONNECTED) for pred, _, _ in rows)
            for key in keys:
                predicates = set(REGISTRY[key].predicates)
                if generated:
                    for pred, _, owner in rows:
                        if pred in predicates or pred == raw_predicate(key):
                            kept = removals.setdefault(owner, {}).setdefault(uri, [])
                            if key not in kept:
                                kept.append(key)
                    continue
                if key == "label" or subjects[uri][key] is None:
                    continue
                held = {value: owner for pred, value, owner in rows if pred in predicates}
                given = [str(o) for s, p, o in planned[uri].triples if str(p) in predicates]
                different = [value for value in given if value not in held]
                if held and different:
                    value, owner = next(iter(held.items()))
                    raise ValueError(
                        f"{key} mismatch for <{uri}>: it has <{value}>, written by "
                        f"source {owner!r}, and insert_metadata gave <{different[0]}>. "
                        "Change it where it was written"
                    )
        return removals

    def _plan_metadata(self, records: Mapping[str, Mapping[str, Any]],
                       roles: Mapping[str, str]) -> list:
        """One :class:`~acquirium.Client.metadata.Write` per node, planned
        before anything is written so a bad key or a value that does not
        resolve fails the whole call."""
        namespaces = self._namespace_uris() if records else ()
        return [plan_write(uri, values, role=roles[uri], resolve=self._resolve_one,
                           namespaces=namespaces)
                for uri, values in records.items()]

    def _write_metadata(self, writes: list) -> dict[str, Any]:
        """Apply planned writes as one update to the metadata source graph."""
        update = update_text(writes)
        if not update:
            return {"ok": True}
        return self.sparql_update(update, source_id=METADATA_SOURCE_ID)

    def metadata_documents(self, subjects: Iterable[str]) -> dict[str, dict]:
        """Read complete typed metadata from the authoritative source graph."""
        from acquirium.Metadata import loads

        response = self._http.post(f"{self.base_url}/metadata/documents", json=list(subjects))
        _raise_for_status(response)
        return {subject: loads(document) for subject, document in response.json().items()}

    def _post_admitted(self, path: str, *, retry_for: float, **kwargs: Any) -> requests.Response:
        """POST ``path``, resending it after ``Retry-After`` while the server
        answers 503, for up to ``retry_for`` seconds. Raises for any other
        error, or for the last 503."""
        deadline = time.monotonic() + retry_for
        while True:
            response = self._http.post(f"{self.base_url}{path}", **kwargs)
            if response.status_code != 503 or time.monotonic() >= deadline:
                _raise_for_status(response)
                return response
            delay = float(response.headers.get("Retry-After", 1))
            logger.debug("%s refused (%s), resending in %.1f s", path, response.text[:200], delay)
            time.sleep(delay)

    def insert_timeseries(self, *, source_id, stream_name, rows, property_uri=None, request_id=None, replace=False,
                          wait=30, timeout=60, retry_for=CAPACITY_RETRY_S):
        # property_uri is a compatibility argument. Registration owns the link.
        return self.insert_timeseries_batch(source_id, {stream_name: rows}, request_id=request_id,
                                            replace=[stream_name] if replace else (),
                                            wait=wait, timeout=timeout, retry_for=retry_for)

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None, replace=(), wait=30,
                                timeout=60, retry_for=CAPACITY_RETRY_S, batch_rows=MAX_REQUEST_ROWS):
        """Upsert ``{stream_name: rows}``. Each stream named in ``replace`` holds
        exactly its supplied rows afterwards.

        Up to ``MAX_REQUEST_ROWS`` rows go in one atomic request. Larger inputs
        are split into several, sent in order; only the last waits for
        publication, and the result lists every request's receipt. Retrying
        with the same ``request_id`` and streams resends the same requests.
        """
        replace = set(replace)
        if replace - streams.keys():
            raise ValueError(f"replace names streams not in the insert: {sorted(replace - streams.keys())}")
        if not 0 < batch_rows <= MAX_REQUEST_ROWS:
            raise ValueError(f"batch_rows must be between 1 and {MAX_REQUEST_ROWS}")
        chunks = list(_insert_chunks(streams, replace, size=batch_rows))

        def send(chunk, request_id, wait):
            payload = [StreamInsert(source_id=source_id, stream_name=name, values=rows, replace=clear,
                                    request_id=request_id).model_dump(mode="json")
                       for name, rows, clear in chunk]
            return self._post_admitted("/insert_timeseries", retry_for=retry_for, json=payload,
                                       params={"wait": wait}, timeout=timeout).json()
        return _send_in_order(chunks, request_id or str(uuid.uuid4()), wait, send)

    def wait(self, receipt, *, timeout=30):
        """Wait for a database-scoped sequence returned by an accepted request."""
        import math
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("timeout must be finite and nonnegative")
        sequence = receipt["sequence"]
        deadline = time.monotonic() + timeout
        while True:
            status = self.engine_status()
            if status["applied"] >= sequence:
                return self.engine_snapshot()
            if status["fault"]:
                raise RuntimeError(f"accepted request {sequence} is blocked: {status['fault']}")
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
        """Request another attempt for failed soft sensors after repairing the cause."""
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

    def delete_timeseries(self, source_id, stream_name, timestamps, *, request_id=None, wait=30, timeout=60,
                          retry_for=CAPACITY_RETRY_S):
        from acquirium.core import time_ns
        return self._post_admitted("/delete_timeseries", retry_for=retry_for, json={
            "source_id": source_id, "stream_name": stream_name,
            "timestamps": [time_ns(value) for value in timestamps],
            "request_id": request_id or str(uuid.uuid4())}, params={"wait": wait}, timeout=timeout).json()

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, request_id: Optional[str] = None,
        wait=30, timeout=60, retry_for=CAPACITY_RETRY_S,
    ) -> dict[str, Any]:
        """Insert a ``(ts, stream_name, value)`` table, split into requests of at
        most ``MAX_REQUEST_ROWS`` rows as ``insert_timeseries_batch`` splits."""
        import io
        import pyarrow as pa
        chunks = [table.slice(offset, MAX_REQUEST_ROWS)
                  for offset in range(0, len(table), MAX_REQUEST_ROWS)]

        def send(chunk, request_id, wait):
            chunk = chunk.append_column("source_id", pa.repeat(source_id, len(chunk)))
            buf = io.BytesIO()
            with ipc.new_stream(buf, chunk.schema) as writer:
                writer.write_table(chunk)
            headers = {"Content-Type": "application/vnd.apache.arrow.stream",
                       "X-Acquirium-Request-Id": request_id}
            # Bytes rather than the buffer, so a resend sends the whole body again.
            return self._post_admitted("/insert_timeseries_arrow", retry_for=retry_for,
                                       data=buf.getvalue(), headers=headers,
                                       params={"wait": wait}, timeout=timeout).json()
        return _send_in_order(chunks, request_id or str(uuid.uuid4()), wait, send)

    def query_logs(
        self,
        property_uri: Optional[str] = None,
        log_time_start: Optional[str] = None,
        log_time_end: Optional[str] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
    ) -> list[LogEntry]:
        """
        Query log entries for a given property URI within optional time intervals.

        Args:
            property_uri: The URI of the time series point. If None, defaults to the
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
            "property_uri": property_uri,
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

    def delete_logs(self, property_uri: Optional[str] = None) -> dict:
        """
        Delete all log entries for a given property URI.

        Args:
            property_uri: The URI of the time series point. If None, defaults to the
                generic plant URI.
        """
        url = f"{self.base_url}/delete_logs"
        params = {}
        if property_uri is not None:
            params["property_uri"] = property_uri
        response = self._http.delete(url, params=params)
        response.raise_for_status()
        return response.json()
