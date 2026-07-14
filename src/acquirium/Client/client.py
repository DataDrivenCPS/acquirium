from typing import Optional, Iterator, Any
from datetime import datetime
import requests
from requests import HTTPError
from pathlib import Path
from rdflib import URIRef
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    LogEntry,
    AppSpec,
    AppRunRequest,
    AppStopRequest,
    StreamInsert,
    RegisterDatasourceRequest,
    looks_like_uri,
    split_record_uri_inputs,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Grafana.grafana_dashboard_creator import GrafanaDashboardCreator
from rdflib import Graph, URIRef
from rdflib.namespace import NamespaceManager
import logging
logger = logging.getLogger(__name__)


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


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace: bool = True) -> None:
        """
        Insert RDF graph into the graph store to the main graph.

        The server refreshes the embedding index synchronously before
        responding, so inserted concepts are resolvable once this returns.

        Args:
            :param rdf_graph: `pathlib.Path` like object, or string.
            In the case of a string the string it can be either:
                - graph content as text
                - location of the source file
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
        """
        if isinstance(rdf_graph, Path):
            if not rdf_graph.is_file():
                raise FileNotFoundError(f"Graph file not found: {rdf_graph}")
            with open(rdf_graph, "r") as f:
                rdf_graph = f.read()
        elif isinstance(rdf_graph, str):
            if rdf_graph.strip().startswith(("<", "@", "#")) or "\n" in rdf_graph and p.suffix:
                # Looks like RDF content (has RDF markers or multiple lines) so treat as content
                pass
            else:
                try:
                    # Treat as file path and attempt to read           
                    p = Path(rdf_graph)
                    if p.is_file():
                        with open(p, "r") as f:
                            rdf_graph = f.read()
                    else:
                        raise FileNotFoundError                
                except Exception:
                    # Looks like a file path (no RDF content markers, single line, has extension) but doesn't exist
                    raise FileNotFoundError(f"Graph file not found: {rdf_graph}")
        else:
            raise ValueError("rdf_graph must be a string or Path object")


        url = f"{self.base_url}/insert_graph"
        data = {
            "rdf_graph": rdf_graph,
            "format": format,
            "replace": replace,
        }
        response = requests.post(url, json=data)
        _raise_for_status(response)


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

    def sparql_query(self, sparql: str, use_union: bool = True) -> dict:
        """
        Execute a SPARQL query against the graph store.

        Args:
            sparql: The SPARQL query string.
            use_union: Whether to use UNION for optional patterns.

        Returns:
            The SPARQL query result as a dictionary.
        """
        url = f"{self.base_url}/sparql_json"
        data = {
            "query": sparql,
            "use_union": use_union,
        }
        response = requests.get(url, params=data)
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

    def resolve_text(
        self,
        text: str,
        kind: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.5,
        context: Optional[list[str]] = None,
    ) -> list[dict]:
        """Resolve natural language text to ontology URIs via the server's embedding matcher.

        ``context`` is an optional list of already-chosen URIs used to break
        symbol ambiguity (e.g. resolving "kg" given a Mass quantity kind).

        Example::

            # unit string read from a chlorine-analyzer tag description
            resolve_text("mg/L", kind="unit", top_k=1)
            # -> [{"uri": "http://qudt.org/vocab/unit/MilliGM-PER-L",
            #      "kind": "unit", "score": 1.0, "match_stage": "exact", ...}]
        """
        url = f"{self.base_url}/resolve_text"
        params: dict[str, Any] = {"text": text, "top_k": top_k, "min_score": min_score}
        if kind:
            params["kind"] = kind
        if context:
            params["context"] = context
        response = requests.get(url, params=params)
        _raise_for_status(response)
        return response.json().get("matches", [])

    def resolve_concept(
        self,
        text: str,
        kind: Optional[str] = None,
        context: Optional[list[str]] = None,
        min_score: float = 0.5,
    ) -> Optional[str]:
        """Resolve text to a single best ontology/QUDT URI, or ``None``.

        The one coordination point for concept normalization shared by the
        query builder and stream registration. A value that already looks
        like a URI is passed through unchanged; otherwise the server's
        unified resolver (data-graph + deterministic unit converter + QUDT,
        with optional ``context`` disambiguation) is consulted and the top
        match's URI returned.

        Example::

            # turbidity-sensor unit cell from a CSV
            resolve_concept("NTU", kind="unit")
            # -> "http://qudt.org/vocab/unit/NTU"
            resolve_concept("http://qudt.org/vocab/unit/NTU")  # passthrough
            # -> "http://qudt.org/vocab/unit/NTU"
        """
        if looks_like_uri(text):
            return text
        matches = self.resolve_text(
            text, kind=kind, top_k=1, min_score=min_score, context=context
        )
        return matches[0]["uri"] if matches else None

    def resolve_record(
        self,
        fields: dict[str, tuple[str, Optional[str]]],
        top_k: int = 5,
        min_score: float = 0.5,
        context: Optional[list[str]] = None,
    ) -> dict[str, list[dict]]:
        """Jointly resolve a record's fields (server ``/resolve_record``).

        ``fields`` maps a caller-chosen label to ``(text, kind)``. The
        label is echoed back unchanged as the result key and is never read
        by the resolver; resolution is driven by ``(text, kind)``.
        ``context`` is an optional list of already-chosen sibling URIs
        that should participate in disambiguation even when those siblings
        themselves do not need resolving. Returns the ranked matches per
        label; related fields (e.g. a unit and its quantity kind) reinforce
        each other server-side. The example labels below mimic a historian
        export's column headers (real-source feel).

        Example::

            resolve_record({"FIT-101.EU":  ("gal/min", "unit"),
                            "FIT-101.QTY": ("flow rate", "quantity_kind")})
            # -> {"FIT-101.EU":  [{"uri": ".../unit/GAL_US-PER-MIN", ...}, ...],
            #     "FIT-101.QTY": [{"uri": ".../quantitykind/VolumeFlowRate",
            #                      ...}, ...]}
        """
        body = {
            "fields": [
                {"name": n, "text": t, "kind": k} for n, (t, k) in fields.items()
            ],
            "top_k": top_k,
            "min_score": min_score,
        }
        if context:
            body["context"] = context
        response = requests.post(f"{self.base_url}/resolve_record", json=body)
        _raise_for_status(response)
        return response.json().get("matches", {})

    def resolve_record_uris(
        self,
        fields: dict[str, tuple[Any, Optional[str]]],
        min_score: float = 0.5,
    ) -> dict[str, str | URIRef | None]:
        """Jointly resolve a record to one best URI per field, or ``None``.

        Keys are caller-chosen labels echoed back unchanged (never read by
        the resolver); ``(text, kind)`` drives resolution. Per-field URI
        passthrough (like :meth:`resolve_concept`); the rest are resolved
        together so a confident field disambiguates an ambiguous sibling.
        ``None`` inputs and unresolved fields map to ``None``. URI/URIRef
        inputs pass through unchanged and also become disambiguating context
        for the remaining text fields. Example labels below mimic a historian
        export's column headers.

        Example::

            resolve_record_uris({"FIT-101.EU":  ("gal/min", "unit"),
                                 "FIT-101.QTY": ("flow rate", "quantity_kind")})
            # -> {"FIT-101.EU":  "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            #     "FIT-101.QTY": ".../quantitykind/VolumeFlowRate"}
        """
        out, to_resolve, context = split_record_uri_inputs(fields)
        if to_resolve:
            matches = self.resolve_record(
                to_resolve,
                top_k=1,
                min_score=min_score,
                context=context or None,
            )
            for name in to_resolve:
                m = matches.get(name) or []
                out[name] = m[0]["uri"] if m else None
        return out

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
        """Return the server's current graph version counter.

        The counter is bumped on every graph mutation. Workers can poll this
        to detect when their cached query needs to be rebuilt.
        """
        url = f"{self.base_url}/graph_version"
        response = requests.get(url)
        response.raise_for_status()
        return int(response.json().get("version", 0))

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
            log_time = datetime.now().isoformat()
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

    def register_app(self, spec: AppSpec) -> dict:
        url = f"{self.base_url}/apps/register"
        response = requests.post(url, json=spec.model_dump(mode="json"))
        response.raise_for_status()
        return response.json()

    def run_app(
        self,
        app_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        params: Optional[dict] = None,
        keep_alive: bool = False,
        interval: float = 10.0,
    ) -> dict:
        url = f"{self.base_url}/apps/run"
        req = AppRunRequest(
            app_id=app_id,
            start=start,
            end=end,
            params=params or {},
            keep_alive=keep_alive,
            interval=interval,
        )
        response = requests.post(url, json=req.model_dump(mode="json"))
        response.raise_for_status()
        return response.json()

    def stop_app(self, *, run_id: Optional[str] = None, app_id: Optional[str] = None) -> dict:
        url = f"{self.base_url}/apps/stop"
        req = AppStopRequest(run_id=run_id, app_id=app_id)
        response = requests.post(url, json=req.model_dump(mode="json"))
        response.raise_for_status()
        return response.json()

    def list_app_runs(self, *, app_id: Optional[str] = None) -> dict:
        url = f"{self.base_url}/apps/list"
        params = {"app_id": app_id} if app_id else None
        response = requests.get(url, params=params)
        response.raise_for_status()
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
    ) -> dict:
        url = f"{self.base_url}/insert_timeseries"
        body = StreamInsert(
            source_id=source_id,
            ref_name=ref_name,
            point_uri=point_uri,
            replace=replace,
            values=rows,
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

    def insert_timeseries_arrow(self, source_id: str, table: "pa.Table") -> dict[str, Any]:
        import io
        import pyarrow as pa
        table_with_sid = table.append_column("source_id", pa.repeat(source_id, len(table)))
        buf = io.BytesIO()
        with ipc.new_stream(buf, table_with_sid.schema) as writer:
            writer.write_table(table_with_sid)
        buf.seek(0)
        response = requests.post(
            f"{self.base_url}/insert_timeseries_arrow",
            data=buf,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
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
