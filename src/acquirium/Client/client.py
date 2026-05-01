from typing import Optional, Iterator, Any
from datetime import datetime
import requests
import os
from pathlib import Path
import polars as pl
import pyarrow.ipc as ipc
from acquirium.internals.models import (
    Order,
    LogEntry,
    AppSpec,
    AppRunRequest,
    AppStopRequest,
    InsertTimeseriesRequest,
)
from acquirium.internals.internals_namespaces import *
from acquirium.Grafana.grafana_dashboard_creator import GrafanaDashboardCreator

import logging
logger = logging.getLogger(__name__)

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
        response.raise_for_status()
        ingestion_result = self.ingest_external_references_from_graph()
        if ingestion_result:
            logger.info(f"acquirium client: external references ingested: {ingestion_result}")

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
        response.raise_for_status()
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
        response.raise_for_status()
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
        response.raise_for_status()
        return response.json().get("matches", [])

    def ingest_status(self) -> dict:
        """
        Get the current status of data ingestion tasks.

        Returns:
            A dictionary with ingestion status details.
        """
        url = f"{self.base_url}/ingest_status"
        response = requests.get(url)
        response.raise_for_status()
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

    def is_ongoing_ingest(self) -> bool:
        """
        Check if there are ongoing ingestion tasks.

        Returns:
            True if there are ongoing ingestion tasks, False otherwise.
        """
        status = self.ingest_status()
        return status.get("scheduled_tasks", 0) > 0

    def ingest_external_references_from_graph(self) -> dict:
        """
        Query the server graph for CSV/Parquet external references.
        Read those files locally (host filesystem) and upload bytes to server for ingestion.
        Returns counts.
        """
        q = f"""
            SELECT ?data ?ref ?path ?timeCol ?valueCol
            WHERE {{
              ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
              ?ref a <{FILE_REFERENCE}> .
              OPTIONAL {{ ?ref <{FILE_LOCATION}> ?path . }}
              OPTIONAL {{ ?ref <{TIME_COLUMN_ID}> ?timeCol . }}
              OPTIONAL {{ ?ref <{VALUE_COLUMN_ID}> ?valueCol . }}
            }}
        """

        res = self.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        ok = 0
        skipped = 0
        failed = 0

        url = f"{self.base_url}/ingest_external_reference"

        for data_uri, ref_uri, path, time_col, value_col in rows:
            if not path:
                skipped += 1
                continue

            # path is likely a quoted literal from SPARQL JSON results
            p_str = str(path).strip().strip('"').strip("'")
            p = Path(p_str)

            if not p.is_absolute():
                # Interpret relative paths relative to the client's current working directory
                p = (Path.cwd() / p).resolve()

            if not p.exists():
                print(f"External reference file not found: {p} (skipping)")
                failed += 1
                continue

            suffix = p.suffix.lower()
            if suffix == ".parquet":
                ref_type = str(PARQUET_REF)
            elif suffix in {".csv", ".tsv"}:
                ref_type = str(CSV_REF)
            else:
                logger.warning("Unsupported external reference file type: %s", p)
                skipped += 1
                continue

            time_column_no = int(str(time_col).strip('"')) if time_col else 0
            value_column_no = int(str(value_col).strip('"')) if value_col else 1

            try:
                with open(p, "rb") as f:
                    content = f.read()

                files = {"file": (p.name, content, "application/octet-stream")}
                data = {
                    "data_uri": str(data_uri),
                    "ref_uri": str(ref_uri),
                    "ref_type": str(ref_type),
                    "time_column_no": str(time_column_no),
                    "value_column_no": str(value_column_no),
                }

                r = requests.post(url, data=data, files=files)
                r.raise_for_status()
                ok += 1
            except Exception as exc:
                detail = ""
                if hasattr(exc, "response") and exc.response is not None:
                    try:
                        detail = exc.response.json().get("detail", "")
                    except Exception:
                        detail = exc.response.text[:200]
                logger.warning("Failed to ingest %s: %s %s", p.name, exc, detail)
                failed += 1

        return {"ok": ok, "skipped": skipped, "failed": failed, "total": len(rows)}

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

    def insert_timeseries(
        self,
        *,
        ref_uri: str,
        rows: list[tuple[datetime, Any]],
        point_uri: Optional[str] = None,
        replace: bool = False,
    ) -> dict:
        url = f"{self.base_url}/insert_timeseries"
        params = {"ref_uri": ref_uri, "replace": replace}
        if point_uri:
            params["point_uri"] = point_uri
        req = InsertTimeseriesRequest(values=rows)
        response = requests.post(url, params=params, json=req.model_dump(mode="json"))
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
