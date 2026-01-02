from typing import Optional, Iterator
import requests
import os
from pathlib import Path
from acquirium.TextMatch.service import TextMatchService
import polars as pl
# import pyarrow as pa
import pyarrow.ipc as ipc
# from io import BytesIO
from acquirium.internals.models import Order
from acquirium.internals.internals_namespaces import *

class AcquiriumClient:
    def __init__(self, 
                 server_url: str = "localhost", 
                 server_port: int = 8000, 
                 use_ssl: bool = False,
                 lexicon_path: Optional[Path] = None):
        self.base_url = f"{'https' if use_ssl else 'http'}://{server_url}:{server_port}"
        self.textmatch_service = TextMatchService.from_client(
                client=self,
                lexicon_path=lexicon_path,
            )


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace: bool = True) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: `pathlib.Path` like object, or string. 
            In the case of a string the string it can be either:
                - graph content as text
                - location of the source file
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
        """
        if (isinstance(rdf_graph, str) and os.path.isfile(rdf_graph) )or Path(rdf_graph).is_file() or (isinstance(rdf_graph, Path) and rdf_graph.is_file()):
            with open(rdf_graph, "r") as f:
                rdf_graph = f.read()
        else:
            rdf_graph = rdf_graph
        
        url = f"{self.base_url}/insert_graph"
        data = {
            "rdf_graph": rdf_graph,
            "format": format,
            "replace": replace
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        self.ingest_external_references_from_graph()


    def timeseries_df(
        self,
        point_uri: str,
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
            "point_uri": point_uri,
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
                print(f"Error reading Arrow IPC stream: {e}")
                return pl.DataFrame()
            

    def timeseries_batches(
        self,
        point_uri: str,
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
            "point_uri": point_uri,
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
            SELECT ?data ?ref ?type ?path ?timeCol ?valueCol
            WHERE {{
              ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
              ?ref a ?type .
              OPTIONAL {{ ?ref <{REF_PATH}> ?path . }}
              OPTIONAL {{ ?ref <{REF_TIME_COL}> ?timeCol . }}
              OPTIONAL {{ ?ref <{REF_VALUE_COL}> ?valueCol . }}
              FILTER(?type IN (<{PARQUET_REF}>, <{CSV_REF}>))
            }}
        """

        res = self.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        ok = 0
        skipped = 0
        failed = 0

        url = f"{self.base_url}/ingest_external_reference"

        for data_uri, ref_uri, ref_type, path, time_col, value_col in rows:
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
                failed += 1
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
            except Exception:
                failed += 1

        return {"ok": ok, "skipped": skipped, "failed": failed, "total": len(rows)}