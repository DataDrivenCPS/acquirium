from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import logging
from time import perf_counter
from rdflib import Graph

from acquirium.Storage import OxigraphGraphStore, TimescaleStore, TimeseriesStore
from acquirium.Internals.qudt_units import QUDTUnitConverter
from acquirium.soft_sensors import SoftSensorRunner
from acquirium.Internals.internals_namespaces import *

import json
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any

from acquirium.mqtt_ingestion import MQTTIngestService, MQTTStreamSpec


DEFAULT_DATA_DIR = Path(".acquirium")
DEFAULT_DB_NAME = "acquirium"


@dataclass
class Manager:
    timescale: TimeseriesStore
    graph_store: OxigraphGraphStore
    sensors: SoftSensorRunner
    qudt_converter: QUDTUnitConverter | None = None
    backend: str = "timescale"

    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        pg_dsn: str | None = None,
        graph_path: str | Path | None = None,
        ontoenv_root: str | Path | None = None,
        graph_name: str | None = None,
        ontology_dependencies: list[str] | None = None,
        qudt_graph: Graph | None = None,
        qudt_converter: QUDTUnitConverter | None = None,
        soft_sensor_dir: str | Path | None = None,
        recreate: bool = False,
    ):
        if recreate:
            logging.info("acquirium: recreating data directory and database")
            if data_dir is not None:
                base = Path(data_dir)
            else:
                base = DEFAULT_DATA_DIR
            if base.exists():
                import shutil
                shutil.rmtree(base)
                print(f"Deleted data directory {base}")
            
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(name)s %(message)s",
            )
        start = perf_counter()

        # Determine data directory and graph database paths
        base = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        base.mkdir(parents=True, exist_ok=True)
        graph_path = Path(graph_path) if graph_path is not None else base / ".oxigraph"
        ontoenv_root = Path(ontoenv_root) if ontoenv_root is not None else base

        # Setup Timescale/Postgres connection
        effective_dsn = pg_dsn or os.getenv("PG_DSN")
        if not effective_dsn:
            raise ValueError("Timescale/Postgres DSN not provided. Set pg_dsn or PG_DSN.")
        timescale: TimeseriesStore = TimescaleStore(
            dsn=effective_dsn,
            recreate=recreate,
        )

        converter = qudt_converter
        if converter is None and qudt_graph is not None:
            converter = QUDTUnitConverter(qudt_graph)

        graph = OxigraphGraphStore(
            store_path=graph_path,
            env_root=ontoenv_root,
            qudt_converter=converter,
        )

        if ontology_dependencies:
            for dep in ontology_dependencies:
                graph.register_ontology(dep)
                logging.info("acquirium: registered ontology dependency via ontoenv: %s", dep)
        if graph_name:
            graph.ensure_ontology_root(graph_name, ontology_dependencies or [])
            logging.info(
                "acquirium: ensured ontology root %s with imports %s",
                graph_name,
                ontology_dependencies or [],
            )
        if ontology_dependencies:
            graph.refresh_union()
            logging.info("acquirium: refreshed union graph after imports")

        sensors = SoftSensorRunner(timescale, graph, module_dir=soft_sensor_dir or base)
        sensors.load_registry()
        logging.info(
            "acquirium: services ready backend=timescale data_dir=%s db=%s graph=%s sensor_dir=%s elapsed_ms=%.1f",
            base,
            getattr(timescale, "db_path", None),
            graph_path,
            soft_sensor_dir or base,
            (perf_counter() - start) * 1000,
        )

        # Assign dataclass fields
        self.timescale = timescale
        self.graph_store = graph
        self.sensors = sensors
        self.qudt_converter = converter
        self.backend = "timescale"

        self.data_dir = base
        self._ingest_cache_path = base / "ingest_cache.json"
        self._ingest_cache_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="acquirium-ingest")
        self._pending_ingests: list[Future] = []
        self.pg_dsn = effective_dsn
        self.mqtt_ingest = MQTTIngestService(pg_dsn=effective_dsn)
        self._connect_mqtt_streams_from_graph()
        
    
    @classmethod
    def from_env(cls) -> Manager:
        return cls(
            data_dir=os.getenv("ACQUIRIUM_DATA_DIR"),
            pg_dsn=os.getenv("PG_DSN"),
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            graph_name=os.getenv("ACQUIRIUM_GRAPH_NAME"),
            ontology_dependencies=os.getenv("ACQUIRIUM_ONTOLOGY_DEPENDENCIES", "").split(",") if os.getenv("ACQUIRIUM_ONTOLOGY_DEPENDENCIES") else None,
            soft_sensor_dir=os.getenv("ACQUIRIUM_SOFT_SENSOR_DIR"),
            recreate=os.getenv("ACQUIRIUM_RECREATE", "false").lower() == "true",
        )

    def _load_ingest_cache(self) -> dict[str, Any]:
        if not self._ingest_cache_path.exists():
            return {}
        try:
            return json.loads(self._ingest_cache_path.read_text())
        except Exception:
            return {}

    def _connect_mqtt_streams_from_graph(self) -> int:
        """
        Scan graph for MQTTReference nodes attached to data nodes by hasExternalReference
        and start background subscribers.
        Returns number of subscriptions ensured.
        """
        q = f"""
        SELECT ?data ?ref ?broker ?port ?topic ?tkey ?vkey
        WHERE {{
          ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref a <{MQTT_REFERENCE}> .
          OPTIONAL {{ ?ref <{BROKER}> ?broker . }}
          OPTIONAL {{ ?ref <{PORT}> ?port . }}
          OPTIONAL {{ ?ref <{TOPIC}> ?topic . }}
          OPTIONAL {{ ?ref <{TIME_KEY}> ?tkey . }}
          OPTIONAL {{ ?ref <{VALUE_KEY}> ?vkey . }}
        }}
        """
        res = self.graph_store.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        count = 0
        for data_uri, ref_uri, broker, port, topic, tkey, vkey in rows:
            print(data_uri, ref_uri, broker, port, topic, tkey, vkey)
            broker_s = (broker or "localhost").strip('"')
            port_s = (port or "1883").strip('"')
            topic_s = (topic or "").strip('"')
            if not topic_s:
                continue

            spec = MQTTStreamSpec(
                point_uri=str(data_uri),
                ref_uri=str(ref_uri),
                broker=broker_s,
                port=int(port_s),
                topic=topic_s,
                time_key=(tkey or "Timestamp").strip('"'),
                value_key=(vkey or "Value").strip('"'),
            )
            self.mqtt_ingest.ensure_subscribed(spec)
            count += 1

        return count

    def _ingest_external_references_async(self) -> list[Future]:
        """
        Scan the graph for external references and ingest backing files asynchronously.

        Returns: list of futures (you can ignore them, or inspect .done(), .exception(), etc.)
        """

        # 1) query graph for data_node -> ref_node pairs and ref type + path
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
        res = self.graph_store.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        futures: list[Future] = []
        for data_uri, ref_uri, ref_type, path, time_col, value_col in rows:
            if not path:
                continue
            file_path = Path(str(path).strip('"'))

            # 2) compute hash and consult cache
            if not file_path.exists():
                continue
            
            if not time_col:
                time_column_no = 0
            else:
                time_column_no = int(str(time_col).strip('"'))
            if not value_col:
                value_column_no = 1
            else:
                value_column_no = int(str(value_col).strip('"'))

            digest = self._file_sha256(file_path)
            cache_key = f"{ref_uri}"
            with self._ingest_cache_lock:
                cache = self._load_ingest_cache()
                prev: dict = cache.get(cache_key, {})
                if prev.get("sha256") == digest:
                    continue
                # mark “scheduled” now to avoid duplicate scheduling in fast successive calls
                cache[cache_key] = {"sha256": digest, "status": "scheduled", "path": str(file_path)}
                self._save_ingest_cache(cache)

            # 3) schedule ingestion in background thread
            fut = self._executor.submit(
                _ingest_reference_worker,
                pg_dsn=self.pg_dsn,
                data_uri=str(data_uri),
                ref_uri=str(ref_uri),
                ref_type=str(ref_type),
                file_path=str(file_path),
                cache_path=str(self._ingest_cache_path),
                time_column_no=time_column_no,
                value_column_no=value_column_no,
                lock=self._ingest_cache_lock,
            )
            futures.append(fut)
            self._pending_ingests.append(fut)

        return futures


    def _save_ingest_cache(self, cache: dict[str, Any]) -> None:
        tmp = self._ingest_cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, indent=2, sort_keys=True, default=str))
        tmp.replace(self._ingest_cache_path)

    def _file_sha256(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    
    ###########################################
    #################### API ###############
    ###########################################


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace = True) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: An `xml.sax.xmlreader.InputSource`, file-like object,
            `pathlib.Path` like object, or string. In the case of a string the string
            is the location of the source.
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
        """
        
        try:
            self.graph_store.insert_graph(rdf_graph, format=format, replace=replace)
            logging.info("acquirium: inserted graph into store, refreshing text match indexes")
            # refresh matcher using updated union graph
            logging.info("acquirium: inserted graph into store, now ingesting data")
            self._ingest_external_references_async()
            self._connect_mqtt_streams_from_graph()
            
        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise
    
    def timeseries_batch(
        self,
        point_uri: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        order: str = "asc",
        batch_size: int = 50_000,
    ) :
        """
        Retrieve time series data for a given point URI within an optional time range.

        Args:
            point_uri: The URI of the time series point.
            start: Optional start time in ISO format.
            end: Optional end time in ISO format.
            limit: Optional maximum number of results to return.
            order: Order of the results, either "asc" or "desc".

        Returns:
            An iterator that yields batches of time series data as Arrow RecordBatches.
        """
        return self.timescale.timeseries(
            point_uri=point_uri,
            start=start,
            end=end,
            limit=limit,
            order=order,
            batch_size=batch_size,
        )
    

    def sparql_dict(self, query: str, use_union: bool = True) -> dict[str, Any]:
        """
        Execute a SPARQL query against the graph store and return results in dict format.

        Args:
            query: The SPARQL query string.
            use_union: Whether to use the union graph for the query.

        Returns:
            A dictionary containing the query results.
            {"cols": [...], "rows": [...]}
        """
        return self.graph_store.sparql_query(query, use_union=use_union)

    def ingest_status(self) -> dict[str, Any]:
        """
        Get the status of ongoing and past ingestion tasks.

        Returns:
            A dictionary containing the ingestion status.
        """
        with self._ingest_cache_lock:
            cache = self._load_ingest_cache()
        
        errors = {k: v for k, v in cache.items() if v.get("status") == "error"}
        done = {k: v for k, v in cache.items() if v.get("status") == "done"}
        scheduled = {k: v for k, v in cache.items() if v.get("status") == "scheduled"}
        return {
            "total_tasks": len(cache),
            "done_tasks": len(done),
            "scheduled_tasks": len(scheduled),
            "error_tasks": len(errors),
        }
    def close(self) -> None:
        try:
            self._executor.shutdown(wait=False, cancel_futures=False)
        except Exception:
            pass
        try:
            self.mqtt_ingest.stop()
        except Exception:
            pass
        self.timescale.close()
        self.graph_store.close()


###########################################
## Background ingestion worker
###########################################

def _ingest_reference_worker(*, pg_dsn: str, data_uri: str, ref_uri: str, ref_type: str, file_path: str, cache_path: str, time_column_no: int, value_column_no:int, lock) -> None:
    import polars as pl
    from datetime import datetime
    from pathlib import Path
    import json
    import time

    from acquirium.Storage.timescale_store import TimescaleStore  # adjust import to your layout

    cache_p = Path(cache_path)
    lock_p = cache_p.with_suffix(".lock")

    def load_cache() -> dict:
        if not cache_p.exists():
            return {}
        try:
            return json.loads(cache_p.read_text())
        except Exception:
            return {}

    def save_cache(cache: dict) -> None:
        tmp = cache_p.with_suffix(f".tmp.{int(time.time())}")
        tmp.write_text(json.dumps(cache, indent=2, sort_keys=True, default=str))
        tmp.replace(cache_p)

    try:
        ts = TimescaleStore(dsn=pg_dsn)

        p = Path(file_path)

        if ref_type == str(PARQUET_REF):
            df = pl.read_parquet(p,columns=[time_column_no,value_column_no],new_columns=['ts','value'])
        elif ref_type == str(CSV_REF):  # CSV_REF
            df = pl.read_csv(p,columns=[time_column_no,value_column_no],new_columns=['ts','value'])
        else:
            # should not happen due to prior filtering
            raise ValueError(f"Unsupported reference type: {ref_type}")

        # Example assumption: long-form file
        # the data_uri is used as the point id, so add point_id column
        df = df.with_columns(pl.lit(data_uri).alias("point_uri"))
        df = df.select(["point_uri","ts", "value"])
        # Parse time if needed; your ingestion expects datetime objects
        # If your time is already datetime, this is fine.
        if df.schema.get("ts") == pl.Utf8:
            df = df.with_columns(pl.col("ts").str.to_datetime())
        # force all values to utf8 strings
        if df.schema.get("value") != pl.Utf8:
            df = df.with_columns(pl.col("value").cast(pl.Utf8))
        result = ts.bulk_insert_polars(df)

        ts.close()

        if result and result >= 0:
            logging.info("Ingested %d rows for %s from %s", result, data_uri, file_path)
            with lock:
                # Update cache as done
                cache = load_cache()
                entry = cache.get(ref_uri, {})
                entry["status"] = "done"
                entry["ingested_at"] = time.time()
                entry["rows_ingested"] = result
                cache[ref_uri] = entry
                save_cache(cache)
        else:
            raise RuntimeError(f"Ingestion failed for {data_uri} from {file_path}")
    except Exception as exc:
        with lock:
            # Update cache as error
            cache = load_cache()
            entry = cache.get(ref_uri, {})
            entry["status"] = "error"
            entry["error"] = str(exc)
            cache[ref_uri] = entry
            save_cache(cache)
            raise
