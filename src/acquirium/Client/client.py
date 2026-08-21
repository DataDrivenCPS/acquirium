from typing import Optional, Iterator, Any, TYPE_CHECKING
from datetime import datetime, timezone
import base64
import json
import requests
from requests import HTTPError
from pathlib import Path
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    LogEntry,
    AppSpec,
    AppStopRequest,
    StreamInsert,
    RegisterDatasourceRequest,
    looks_like_uri,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Grafana.grafana_dashboard_creator import GrafanaDashboardCreator
from rdflib import Graph, URIRef
from rdflib.namespace import NamespaceManager
import logging
logger = logging.getLogger(__name__)

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


class AcquiriumClient:
    def __init__(self,
                 server_url: str = "localhost",
                 server_port: int = 8000,
                 use_ssl: bool = False):
        self.base_url = f"{'https' if use_ssl else 'http'}://{server_url}:{server_port}"
        self.grafana = GrafanaDashboardCreator(
            title="Acquirium Grafana Dashboard",
            tags=["acquirium"],
        )
        self._namespaces_cache: dict[str, str] | None = None

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
        response = requests.post(url, json=data)
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

        with requests.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            r.raise_for_status()

            try:
                # r.raw is a file-like object
                reader = ipc.RecordBatchStreamReader(r.raw)
                # Collect all batches into a single table
                tables = [pl.from_arrow(batch) for batch in reader]
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
        with requests.get(url, params=params, headers=headers, stream=True, timeout=timeout) as r:
            r.raise_for_status()

            # r.raw is a file-like object
            reader = ipc.RecordBatchStreamReader(r.raw)

            for batch in reader:
                # batch is a pyarrow.RecordBatch; convert to Polars
                yield pl.from_arrow(batch)

    def timeseries_info_batch(self, uris: list[str]) -> dict:
        """Fetch lightweight stats (row_count, earliest, latest) for multiple URIs in one request."""
        from acquirium.internals.models import TimeseriesInfo
        url = f"{self.base_url}/timeseries_info"
        response = requests.post(url, json={"uris": uris})
        _raise_for_status(response)
        data = response.json()
        return {uri: TimeseriesInfo.model_validate(info) for uri, info in data.items()}

    def health(self, timeout: float = 3.0) -> dict:
        """GET /health; raises on connection failure or non-200."""
        response = requests.get(f"{self.base_url}/health", timeout=timeout)
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
        response = requests.post(
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
        response = requests.post(url, json=data)
        _raise_for_status(response)
        return response.json()

    def validate_graph(self) -> dict[str, str | bool]:
        """Validate all registered deployment data against ontology shapes."""
        response = requests.post(f"{self.base_url}/validate_graph")
        _raise_for_status(response)
        return response.json()

    def namespace_manager(self) -> NamespaceManager:
        """Return the Graph that has the ``prefix -> namespace URI`` map from the server.

        Stores (caches) in a rdf Graph object for use by other methods. 
        """
        if self._namespaces_cache is None:
            url = f"{self.base_url}/namespace/list"
            response = requests.get(url)
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
        except Exception as e:
            raise ValueError(f"Cannot expand '{s}': no matching namespace for CURIE and not a full URI")

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
          already look like URIs pass through unchanged.
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
                response = requests.post(f"{self.base_url}/resolve_record", json=body)
                _raise_for_status(response)
                matches = response.json().get("matches", {})
                for name in to_resolve:
                    m = matches.get(name) or []
                    out[name] = m[0]["uri"] if m else None
            return out

        text = str(query)
        if looks_like_uri(text):
            if top_k == 1:
                return text
            return [{"uri": text, "kind": kind, "score": 1.0, "match_stage": "passthrough"}]
        params: dict[str, Any] = {"text": text, "top_k": top_k, "min_score": min_score}
        if kind:
            params["kind"] = kind
        if context:
            params["context"] = context
        response = requests.get(f"{self.base_url}/resolve_text", params=params)
        _raise_for_status(response)
        matches = response.json().get("matches", [])
        if top_k == 1:
            return matches[0]["uri"] if matches else None
        return matches

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
        response = requests.post(url, json={
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
        response = requests.get(url)
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
        response = requests.get(url)
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
        response = requests.post(url, json={"identifier": identifier})
        if not response.ok:
            detail = response.json().get("detail", response.text) if response.headers.get("content-type", "").startswith("application/json") else response.text
            raise ValueError(f"resolve_unit failed: {detail}")
        return response.json()

    def get_conversion_factors(self, from_unit: str, to_unit: str) -> dict:
        """Get pre-computed conversion factors between two units.

        Returns dict with from_multiplier, from_offset, to_multiplier, to_offset, compatible.
        """
        url = f"{self.base_url}/conversion_factors"
        response = requests.post(url, json={"from_unit": from_unit, "to_unit": to_unit})
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
        response = requests.post(url, params=data)
        response.raise_for_status()
        return response.json()

    def register_app(self, spec: AppSpec, *, replace: bool = False) -> dict:
        url = f"{self.base_url}/apps/register"
        response = requests.post(
            url, json=spec.model_dump(mode="json"), params={"replace": replace}
        )
        _raise_for_status(response)
        return response.json()

    def register_transformation(self, definition: dict) -> dict:
        response = requests.post(f"{self.base_url}/transformations/register", json=definition)
        _raise_for_status(response)
        return response.json()

    def create_artifact_request(self, request: dict) -> dict:
        response = requests.post(f"{self.base_url}/artifact-requests", json=request)
        _raise_for_status(response)
        return response.json()

    def lease_artifact_request(self, owner: str) -> dict | None:
        response = requests.post(f"{self.base_url}/artifact-requests/lease", json={"owner": owner})
        _raise_for_status(response)
        return response.json()["lease"]

    def complete_artifact_request(self, request_id: str, *, owner: str, attempt: int,
                                  data: bytes, media_type: str = "application/octet-stream",
                                  metadata: dict | None = None, metrics: dict | None = None) -> dict:
        response = requests.post(f"{self.base_url}/artifact-requests/{request_id}/complete", json={
            "owner": owner, "attempt": attempt,
            "data_base64": base64.b64encode(data).decode("ascii"), "media_type": media_type,
            "metadata": metadata or {}, "metrics": metrics or {},
        })
        _raise_for_status(response)
        return response.json()

    def fail_artifact_request(self, request_id: str, *, owner: str, attempt: int, error: dict) -> dict:
        response = requests.post(f"{self.base_url}/artifact-requests/{request_id}/fail", json={
            "owner": owner, "attempt": attempt, "error": error,
        })
        _raise_for_status(response)
        return response.json()

    def set_transformation_status(self, name: str, status: str) -> dict:
        if status not in {"active", "paused"}:
            raise ValueError("transformation status must be active or paused")
        response = requests.post(f"{self.base_url}/transformations/{name}/{'start' if status == 'active' else 'pause'}")
        _raise_for_status(response)
        return response.json()

    def transformation_status(self, name: str) -> dict:
        response = requests.get(f"{self.base_url}/transformations/{name}")
        _raise_for_status(response)
        return response.json()

    def list_transformations(self) -> dict:
        response = requests.get(f"{self.base_url}/transformations")
        _raise_for_status(response)
        return response.json()

    def rebind_transformation(self, name: str) -> dict:
        response = requests.post(f"{self.base_url}/transformations/{name}/rebind")
        _raise_for_status(response)
        return response.json()

    def reconcile_transformation(self, name: str) -> dict:
        response = requests.post(f"{self.base_url}/transformations/{name}/reconcile")
        _raise_for_status(response)
        return response.json()

    def preview_transformation(self, name: str):
        response = requests.post(f"{self.base_url}/transformations/{name}/preview")
        _raise_for_status(response)
        return ipc.open_stream(response.content).read_all()

    def promote_state_revision(self, revision_id: str, *, policy: str = "prospective",
                               effective_from: str | None = None) -> dict:
        payload = {"policy": policy, "effective_from": effective_from}
        response = requests.post(f"{self.base_url}/state-revisions/{revision_id}/promote", json=payload)
        _raise_for_status(response)
        return response.json()

    def delete_app(self, app_id: str) -> dict:
        url = f"{self.base_url}/apps/delete"
        response = requests.post(url, json={"app_id": app_id})
        _raise_for_status(response)
        return response.json()

    def stop_app(self, *, app_id: str) -> dict:
        url = f"{self.base_url}/apps/stop"
        req = AppStopRequest(app_id=app_id)
        response = requests.post(url, json=req.model_dump(mode="json"))
        response.raise_for_status()
        return response.json()

    def start_app(self, app_id: str, *, params: Optional[dict] = None) -> dict:
        """Start (or resume) a continuous app: bootstrap, resume, or
        reconcile depending on its durable state (continuous_batch.md's
        ``start_app``). Returns ``{"status": ..., "generation": ...}``."""
        url = f"{self.base_url}/apps/start"
        response = requests.post(url, json={"app_id": app_id, "params": params or {}})
        _raise_for_status(response)
        return response.json()

    def reset_app(self, app_id: str) -> dict:
        """Start a new generation for an app and reconcile it from canonical
        history (topology change, code replace, or an explicit reset)."""
        url = f"{self.base_url}/apps/reset"
        response = requests.post(url, json={"app_id": app_id})
        _raise_for_status(response)
        return response.json()

    # ---- internal continuous-batch endpoints (actor-facing) ----

    def app_runtime(self, app_id: str) -> Optional[dict]:
        """Return ``{"status", "generation", "topology_version"}`` for *app_id*,
        or None if it has never been registered."""
        url = f"{self.base_url}/internal/apps/{app_id}/runtime"
        response = requests.get(url)
        if response.status_code == 404:
            return None
        _raise_for_status(response)
        return response.json()

    def set_app_status(self, app_id: str, status: str) -> dict:
        """Report a lifecycle status transition an actor makes on its own
        (e.g. ``failed`` after repeated batch errors)."""
        url = f"{self.base_url}/internal/apps/{app_id}/status"
        response = requests.post(url, json={"status": status})
        _raise_for_status(response)
        return response.json()

    def resume_status(self, app_id: str, generation: int) -> dict:
        """``{"has_subscriptions", "resumable"}`` -- tells an actor's start()
        whether to bootstrap, resume, or reconcile."""
        url = f"{self.base_url}/internal/apps/{app_id}/resume_status"
        response = requests.get(url, params={"generation": generation})
        _raise_for_status(response)
        return response.json()

    def reset_app_runtime(self, app_id: str) -> dict:
        """Start a new generation for *app_id* at the storage layer.
        Returns ``{"generation": ...}``."""
        url = f"{self.base_url}/internal/apps/{app_id}/reset"
        response = requests.post(url)
        _raise_for_status(response)
        return response.json()

    def begin_bootstrap(
        self, app_id: str, *, input_ref_uris: list[str], output_ref_uris: list[str]
    ) -> dict:
        """Snapshot input history into staging; returns
        ``{"bootstrap_id", "app_id", "generation", "streams"}``."""
        url = f"{self.base_url}/internal/apps/{app_id}/bootstrap/begin"
        response = requests.post(
            url, json={"input_ref_uris": input_ref_uris, "output_ref_uris": output_ref_uris}
        )
        _raise_for_status(response)
        return response.json()

    def finalize_bootstrap(self, bootstrap_id: str) -> dict:
        url = f"{self.base_url}/internal/bootstrap/{bootstrap_id}/finalize"
        response = requests.post(url)
        _raise_for_status(response)
        return response.json()

    def next_app_batch(
        self, app_id: str, generation: int, *, target_keys: int = 50_000
    ) -> Optional[dict]:
        """Return the next pending batch, or None when nothing is pending.

        The dict carries ``rows`` (an Arrow table) plus the batch's
        ``batch_id``/``batch_kind``/``generation``/``has_more``/``inputs``
        and, for a bootstrap page, ``bootstrap_id``/``end_ordinal`` --
        exactly the ``acquirium_batch`` schema metadata the server attaches
        (continuous_batch_plan.md Decision 6).
        """
        import pyarrow as pa

        url = f"{self.base_url}/internal/apps/{app_id}/batches/next"
        response = requests.post(url, json={"generation": generation, "target_keys": target_keys})
        if response.status_code == 204:
            return None
        _raise_for_status(response)
        reader = ipc.RecordBatchStreamReader(pa.BufferReader(response.content))
        table = reader.read_all()
        meta_raw = (table.schema.metadata or {}).get(b"acquirium_batch")
        if meta_raw is None:
            raise ValueError("batch response is missing acquirium_batch schema metadata")
        return {"rows": table, **json.loads(meta_raw)}

    def commit_app_batch(
        self,
        app_id: str,
        batch_id: str,
        *,
        generation: int,
        batch_kind: str,
        rows: "pa.Table",
        inputs: Optional[list[dict]] = None,
        bootstrap_id: Optional[str] = None,
        end_ordinal: Optional[int] = None,
        webhook_intents: Optional[list[dict]] = None,
    ) -> dict:
        """Commit one processed batch's output ``rows``.

        For ``batch_kind="tail"``, ``inputs`` (the batch's consumed ranges)
        and ``webhook_intents`` are required; for ``"bootstrap"``,
        ``bootstrap_id``/``end_ordinal`` (from the :meth:`next_app_batch`
        response) are required instead.
        """
        import io

        metadata: dict[str, Any] = {"generation": generation, "batch_kind": batch_kind}
        if batch_kind == "bootstrap":
            metadata["bootstrap_id"] = bootstrap_id
            metadata["end_ordinal"] = end_ordinal
        else:
            metadata["inputs"] = inputs or []
            metadata["webhook_intents"] = webhook_intents or []

        table = rows.replace_schema_metadata(
            {b"acquirium_commit": json.dumps(metadata).encode("utf-8")}
        )
        buf = io.BytesIO()
        with ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        buf.seek(0)
        url = f"{self.base_url}/internal/apps/{app_id}/batches/{batch_id}/commit"
        response = requests.post(
            url, data=buf, headers={"Content-Type": "application/vnd.apache.arrow.stream"}
        )
        _raise_for_status(response)
        return response.json()

    def list_app_runs(self, *, app_id: Optional[str] = None) -> dict:
        url = f"{self.base_url}/apps/list"
        params = {"app_id": app_id} if app_id else None
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def resolve_storage_keys(self, uris: list[str]) -> dict[str, str]:
        """Map each point_uri (or already-canonical ref_uri) to its storage key."""
        if not uris:
            return {}
        url = f"{self.base_url}/resolve_storage_keys"
        response = requests.post(url, json={"uris": uris})
        _raise_for_status(response)
        return response.json()

    def register_datasource(self, source_id: str) -> str:
        """Register a named datasource. Returns source_id."""
        url = f"{self.base_url}/register_datasource"
        response = requests.post(url, json=RegisterDatasourceRequest(source_id=source_id).model_dump())
        response.raise_for_status()
        return response.json()["source_id"]

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
        response = requests.post(url, json=[body.model_dump(mode="json")])
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
        response = requests.post(url, json=[s.model_dump(mode="json") for s in payload])
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
        response = requests.post(
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
        response = requests.get(url, params=params)
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
        response = requests.delete(url, params=params)
        response.raise_for_status()
        return response.json()


    ### Grafana dashboard related methods
    def add_gauge_panel(self, prop_dict: dict) -> None:
        self.grafana.add_gauge(prop_dict)

    def add_time_series_panel(self, title: str, prop_dicts: list[dict]) -> None:
        self.grafana.add_time_series(title, prop_dicts)

    def generate_grafana_dashboard(self, server: str, api_key: str) -> None:
        self.grafana.generate_dashboard()
        self.grafana.upload_dashboard(server, api_key)
