from typing import Optional, Iterator, Any
from datetime import datetime
import requests
from requests import HTTPError
from pathlib import Path
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    LogEntry,
    AppSpec,
    AppRunRequest,
    AppStopRequest,
    StreamInsert,
    StreamBindRequest,
    StreamRow,
    RegisterDatasourceRequest,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Grafana.grafana_dashboard_creator import GrafanaDashboardCreator

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


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace: bool = True, wait_for_embedding: bool = False) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: `pathlib.Path` like object, or string.
            In the case of a string the string it can be either:
                - graph content as text
                - location of the source file
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
            wait_for_embedding: If True, the server will block until the embedding
                index rebuild is complete before returning. Default False.
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


        if wait_for_embedding:
            logger.info("acquirium client: requesting server to rebuild embedding index (waiting)...")

        url = f"{self.base_url}/insert_graph"
        data = {
            "rdf_graph": rdf_graph,
            "format": format,
            "replace": replace,
            "wait_for_embedding": wait_for_embedding,
        }
        response = requests.post(url, json=data)
        _raise_for_status(response)

        if wait_for_embedding:
            logger.info("acquirium client: server embedding index rebuild complete")


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

    def resolve_text(
        self,
        text: str,
        kind: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> list[dict]:
        """Resolve natural language text to ontology URIs via the server's embedding matcher."""
        url = f"{self.base_url}/resolve_text"
        params: dict[str, Any] = {"text": text, "top_k": top_k, "min_score": min_score}
        if kind:
            params["kind"] = kind
        response = requests.get(url, params=params)
        _raise_for_status(response)
        return response.json().get("matches", [])

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
        """Resolve a unit identifier to its QUDT metadata via the server."""
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

    def list_streams(
        self,
        *,
        bound: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> list[StreamRow]:
        """List rows from the ``streams`` table.

        ``bound=True`` returns only rows already linked to a ``point_uri``,
        ``bound=False`` only unassigned rows, ``None`` returns all.
        """
        params: dict[str, Any] = {"offset": offset}
        if bound is not None:
            params["bound"] = "true" if bound else "false"
        if limit is not None:
            params["limit"] = limit
        response = requests.get(f"{self.base_url}/streams", params=params)
        _raise_for_status(response)
        return [StreamRow(**r) for r in response.json().get("streams", [])]

    def bind_stream(
        self,
        *,
        point_uri: str,
        ref_uri: Optional[str] = None,
        source_id: Optional[str] = None,
        ref_name: Optional[str] = None,
    ) -> StreamRow:
        """Bind a stream to a ``point_uri``. Identify by ``ref_uri`` or by
        ``(source_id, ref_name)``."""
        body = StreamBindRequest(
            point_uri=point_uri,
            ref_uri=ref_uri,
            source_id=source_id,
            ref_name=ref_name,
        )
        response = requests.post(
            f"{self.base_url}/streams/bind",
            json=body.model_dump(mode="json"),
        )
        _raise_for_status(response)
        return StreamRow(**response.json())

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
