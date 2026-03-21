from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os
import logging
from time import perf_counter
from rdflib import Graph, URIRef, Node, Literal, RDF, RDFS

from acquirium.Storage import OxigraphGraphStore, TimescaleStore, PGReferenceRegistry, PGReferenceInfo, resolve_dsn
from acquirium.internals.qudt_units import QUDTUnitConverter
from acquirium.internals.models import LogEntry, TimeIntervalModel, AppSpec, AppRunRequest
from acquirium.internals.internals_namespaces import *
from acquirium.internals.app_utils import app_uri_for, make_stream_ref_uri

import json
import hashlib
import re
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any
import shutil
import docker
from docker.errors import DockerException, NotFound as ContainerNotFound
from acquirium.Server.mqtt_ingestion import MQTTIngestService, MQTTStreamSpec
from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher, _split_local_name
from acquirium.TextMatch.qudt_store import QUDTStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("acquirium.manager")
logger.setLevel(logging.INFO)

DEFAULT_DATA_DIR = Path(".acquirium")
DEFAULT_DB_NAME = "acquirium"

def _wipe_dir_contents(base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for p in base.iterdir():
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()

@dataclass
class Manager:
    timescale: TimescaleStore
    graph_store: OxigraphGraphStore
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
        recreate: bool = False,
    ):
        if recreate:
            logging.info("acquirium: recreating data directory and database")
            if data_dir is not None:
                base = Path(data_dir)
            else:
                base = DEFAULT_DATA_DIR
            if base.exists():
                _wipe_dir_contents(base)
                print(f"Deleted data directory contents: {base}")
            
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
        timescale: TimescaleStore = TimescaleStore(
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


        # Assign dataclass fields
        self.timescale = timescale
        self.graph_store = graph
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
        self.pg_registry = PGReferenceRegistry()
        self._scan_pg_references_from_graph()
        self.app_storage_root = Path(
            os.getenv("ACQUIRIUM_APP_STORAGE_ROOT", str(self.data_dir / "apps"))
        )
        self.app_storage_root.mkdir(parents=True, exist_ok=True)
        self._app_runs: dict[str, dict[str, Any]] = {}
        self._app_runs_lock = threading.Lock()

        # Initialize Docker client for spawning app containers
        try:
            self._docker = docker.from_env()
            self._docker.ping()
            logger.info("acquirium: connected to Docker daemon")
        except DockerException as e:
            logger.warning("acquirium: Docker not available, app execution disabled: %s", e)
            self._docker = None

        _emb_model = os.getenv("ACQUIRIUM_EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")

        # Dual matchers: graph (class/predicate) and QUDT (unit/quantity_kind)
        self._graph_matcher = EmbeddingMatcher(
            model_name=_emb_model,
            cache_dir=base / "embedding_cache" / "graph",
        )
        self._qudt_matcher = EmbeddingMatcher(
            model_name=_emb_model,
            cache_dir=base / "embedding_cache" / "qudt",
        )
        self._qudt_store = QUDTStore(data_dir=base)

        # Kept for backward compat — points to graph matcher
        self.embedding_matcher = self._graph_matcher

        # Embedding index status tracking
        self._embedding_status_lock = threading.Lock()
        self._embedding_status: dict[str, dict[str, Any]] = {
            "graph": {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
            "qudt":  {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
        }

        # Startup: graph index (sync if cache hit, background if miss)
        self._startup_graph_index()
        # Startup: QUDT index (always background)
        self._executor.submit(self._startup_qudt_task)

    
    @classmethod
    def from_env(cls) -> Manager:
        return cls(
            data_dir=os.getenv("ACQUIRIUM_DATA_DIR"),
            pg_dsn=os.getenv("PG_DSN"),
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            graph_name=os.getenv("ACQUIRIUM_GRAPH_NAME"),
            ontology_dependencies=os.getenv("ACQUIRIUM_ONTOLOGY_DEPENDENCIES", "").split(",") if os.getenv("ACQUIRIUM_ONTOLOGY_DEPENDENCIES") else None,
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
            logger.info("Found MQTT reference: %s %s %s %s %s %s %s",
                        data_uri, ref_uri, broker, port, topic, tkey, vkey)
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

    def _scan_pg_references_from_graph(self) -> int:
        """Scan graph for PGReference nodes and register them in the registry."""
        q = f"""
        SELECT ?data ?ref ?dsn ?host ?port ?db ?user ?pass ?table ?query ?tcol ?vcol ?pfilter
        WHERE {{
          ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref a <{PG_REFERENCE}> .
          OPTIONAL {{ ?ref <{PG_DSN}> ?dsn . }}
          OPTIONAL {{ ?ref <{PG_HOST}> ?host . }}
          OPTIONAL {{ ?ref <{PG_PORT}> ?port . }}
          OPTIONAL {{ ?ref <{PG_DB}> ?db . }}
          OPTIONAL {{ ?ref <{PG_USER}> ?user . }}
          OPTIONAL {{ ?ref <{PG_PASS}> ?pass . }}
          OPTIONAL {{ ?ref <{PG_TABLE}> ?table . }}
          OPTIONAL {{ ?ref <{PG_QUERY}> ?query . }}
          OPTIONAL {{ ?ref <{PG_TIME_COL}> ?tcol . }}
          OPTIONAL {{ ?ref <{PG_VALUE_COL}> ?vcol . }}
          OPTIONAL {{ ?ref <{PG_POINT_FILTER}> ?pfilter . }}
        }}
        """
        res = self.graph_store.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        count = 0
        for row in rows:
            (data_uri, ref_uri, dsn, host, port, db, user, passwd,
             table, custom_query, tcol, vcol, pfilter) = row
            try:
                s = lambda v: str(v).strip().strip('"') if v else None
                resolved = resolve_dsn(
                    dsn=s(dsn), host=s(host), port=s(port),
                    db=s(db), user=s(user), password=s(passwd),
                )
                info = PGReferenceInfo(
                    dsn=resolved,
                    table=s(table),
                    custom_query=s(custom_query),
                    time_col=s(tcol) or "time",
                    value_col=s(vcol) or "value",
                    point_filter=s(pfilter),
                )
                self.pg_registry.register(str(ref_uri), info)
                count += 1
            except Exception:
                logger.warning("Failed to register PGReference %s", ref_uri, exc_info=True)

        if count:
            logger.info("Registered %d PGReference(s) from graph", count)
        return count

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

    
    # ----- Embedding index methods -----

    def _extract_concepts_for_embedding(self) -> list[dict[str, Any]]:
        """Extract classes and predicates from the union graph for embedding.

        QUDT Units and QuantityKinds are handled separately by QUDTStore/_qudt_matcher.
        """
        concepts: list[dict[str, Any]] = []
        seen_class: set[str] = set()
        seen_pred: set[str] = set()

        # Query 1: Classes (rdfs:Class, owl:Class, rdfs:subClassOf targets,
        # and any URI used as an rdf:type object)
        # NOTE: QUDT Unit/QuantityKind removed — handled by _qudt_matcher
        # Labels: rdfs:label, skos:prefLabel, skos:altLabel
        # Language filter: keep English-tagged or untagged labels only
        class_query = """
        SELECT DISTINCT ?uri ?label WHERE {
          {
            ?uri a <http://www.w3.org/2000/01/rdf-schema#Class> .
          } UNION {
            ?uri a <http://www.w3.org/2002/07/owl#Class> .
          } UNION {
            ?x <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?uri .
          } UNION {
            ?x a ?uri .
          } UNION {
            ?uri a <urn:nawi-water-ontology#Class> .
          } UNION {
            ?uri <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?x .
          } UNION {
            ?x <http://qudt.org/schema/qudt/hasEnumerationKind> ?uri .
          } UNION {
            ?x <http://data.ashrae.org/standard223#ofSubstance> ?uri .
          } UNION {
            ?x <http://data.ashrae.org/standard223#hasMedium> ?uri .
          }
          OPTIONAL {
            {
              ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#altLabel> ?label .
            }
            FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
          }
          FILTER(isIRI(?uri))
        }
        """
        try:
            res = self.graph_store.sparql_query(class_query, use_union=True)
            # Aggregate all labels per URI
            uri_labels: dict[str, list[str]] = {}
            uri_first_label: dict[str, str | None] = {}
            for row in res.get("rows", []):
                uri = str(row[0]) if row[0] else None
                label = str(row[1]).strip('"') if row[1] else None
                if not uri:
                    continue
                if uri not in uri_labels:
                    uri_labels[uri] = []
                    uri_first_label[uri] = label
                if label and label not in uri_labels[uri]:
                    uri_labels[uri].append(label)

            for uri, labels in uri_labels.items():
                if uri in seen_class:
                    continue
                seen_class.add(uri)
                surfaces = []
                for lbl in labels:
                    lbl_lower = lbl.lower()
                    if lbl_lower not in surfaces:
                        surfaces.append(lbl_lower)
                # Always add tokenized local name as a surface
                tokens = _split_local_name(uri)
                if tokens:
                    joined = " ".join(tokens)
                    if joined not in surfaces:
                        surfaces.append(joined)
                display_label = uri_first_label[uri] or (joined if tokens else uri)
                concepts.append({
                    "uri": uri,
                    "kind": "class",
                    "label": display_label,
                    "surfaces": surfaces,
                })
        except Exception:
            logger.warning("Failed to extract class concepts", exc_info=True)

        # Query 2: Predicates (declared properties + any IRI used as a predicate)
        # Labels: rdfs:label, skos:prefLabel, skos:altLabel
        # Language filter: keep English-tagged or untagged labels only
        pred_query = """
        SELECT DISTINCT ?uri ?label WHERE {
          {
            ?uri a <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
          } UNION {
            ?uri a <http://www.w3.org/2002/07/owl#ObjectProperty> .
          } UNION {
            ?uri a <http://www.w3.org/2002/07/owl#DatatypeProperty> .
          } UNION {
            ?s ?uri ?o .
          }
          OPTIONAL {
            {
              ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#altLabel> ?label .
            }
            FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
          }
          FILTER(isIRI(?uri))
        }
        """
        try:
            res = self.graph_store.sparql_query(pred_query, use_union=True)
            # Aggregate all labels per URI
            uri_labels: dict[str, list[str]] = {}
            uri_first_label: dict[str, str | None] = {}
            for row in res.get("rows", []):
                uri = str(row[0]) if row[0] else None
                label = str(row[1]).strip('"') if row[1] else None
                if not uri:
                    continue
                if uri not in uri_labels:
                    uri_labels[uri] = []
                    uri_first_label[uri] = label
                if label and label not in uri_labels[uri]:
                    uri_labels[uri].append(label)

            for uri, labels in uri_labels.items():
                if uri in seen_pred:
                    continue
                seen_pred.add(uri)
                surfaces = []
                for lbl in labels:
                    lbl_lower = lbl.lower()
                    if lbl_lower not in surfaces:
                        surfaces.append(lbl_lower)
                tokens = _split_local_name(uri)
                if tokens:
                    joined = " ".join(tokens)
                    if joined not in surfaces:
                        surfaces.append(joined)
                display_label = uri_first_label[uri] or (joined if tokens else uri)
                concepts.append({
                    "uri": uri,
                    "kind": "predicate",
                    "label": display_label,
                    "surfaces": surfaces,
                })
        except Exception:
            logger.warning("Failed to extract predicate concepts", exc_info=True)

        return concepts

    def _update_embedding_status(self, index: str, **kwargs: Any) -> None:
        """Update the embedding status for a given index (thread-safe)."""
        with self._embedding_status_lock:
            self._embedding_status[index].update(kwargs)

    def _startup_graph_index(self) -> None:
        """Build graph embedding index on startup. Uses cache if available."""
        self._update_embedding_status("graph", state="building")
        t0 = perf_counter()
        try:
            concepts = self._extract_concepts_for_embedding()
            if concepts:
                self._graph_matcher.build_index(concepts)
                n_surfaces = sum(len(c.get("surfaces", [])) for c in concepts)
                elapsed = perf_counter() - t0
                self._update_embedding_status(
                    "graph", state="ready", concepts=len(concepts),
                    surfaces=n_surfaces, last_built=datetime.now().isoformat(),
                    duration_s=round(elapsed, 2), error=None,
                )
                logger.info("Graph embedding index: %d concepts", len(concepts))
            else:
                self._update_embedding_status("graph", state="ready", concepts=0, surfaces=0)
                logger.info("No concepts found in graph; graph embedding index is empty")
        except Exception as exc:
            self._update_embedding_status("graph", state="error", error=str(exc))
            logger.warning("Failed to build graph embedding index", exc_info=True)

    def _startup_qudt_task(self) -> None:
        """Background task: extract QUDT, diff, build/update QUDT embedding index."""
        self._update_embedding_status("qudt", state="building")
        t0 = perf_counter()
        try:
            if self._qudt_store.has_cache() and not self._qudt_matcher.is_ready:
                # Try loading cached concepts + cached embeddings first
                cached = self._qudt_store.get_all_concepts()
                if cached:
                    self._qudt_matcher.build_index(cached)
                    if self._qudt_matcher.is_ready:
                        logger.info("QUDT embedding index loaded from cache (%d concepts)", len(cached))

            all_concepts, removed_uris, changed = self._qudt_store.extract_and_diff()

            if not all_concepts:
                logger.warning("QUDT extraction returned 0 concepts")
                self._update_embedding_status("qudt", state="ready", concepts=0, surfaces=0)
                return

            if not self._qudt_matcher.is_ready:
                # First build
                logger.info("Building QUDT embedding index from scratch (%d concepts)...", len(all_concepts))
                self._qudt_matcher.build_index(all_concepts)
            elif changed:
                # Incremental update — use public API instead of private _meta
                indexed_uris = self._qudt_matcher.get_indexed_uris()
                added = [c for c in all_concepts if c["uri"] not in indexed_uris]
                logger.info("Updating QUDT embedding index: +%d added, -%d removed", len(added), len(removed_uris))
                self._qudt_matcher.update_index(added, removed_uris, all_concepts=all_concepts)
            else:
                logger.info("QUDT data unchanged, embedding index up to date")

            n_surfaces = sum(len(c.get("surfaces", [])) for c in all_concepts)
            elapsed = perf_counter() - t0
            self._update_embedding_status(
                "qudt", state="ready", concepts=len(all_concepts),
                surfaces=n_surfaces, last_built=datetime.now().isoformat(),
                duration_s=round(elapsed, 2), error=None,
            )

        except Exception as exc:
            self._update_embedding_status("qudt", state="error", error=str(exc))
            logger.warning("Failed QUDT startup task", exc_info=True)

    def _rebuild_graph_index_background(self) -> None:
        """Rebuild graph embedding index in background after insert_graph."""
        self._update_embedding_status("graph", state="building")
        t0 = perf_counter()
        try:
            concepts = self._extract_concepts_for_embedding()
            if concepts:
                self._graph_matcher.build_index(concepts)
                n_surfaces = sum(len(c.get("surfaces", [])) for c in concepts)
                elapsed = perf_counter() - t0
                self._update_embedding_status(
                    "graph", state="ready", concepts=len(concepts),
                    surfaces=n_surfaces, last_built=datetime.now().isoformat(),
                    duration_s=round(elapsed, 2), error=None,
                )
                logger.info("Graph embedding index rebuilt: %d concepts", len(concepts))
        except Exception as exc:
            self._update_embedding_status("graph", state="error", error=str(exc))
            logger.warning("Failed to rebuild graph embedding index", exc_info=True)

    def resolve_text(
        self,
        text: str,
        kind: str | None = None,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> list[dict[str, Any]]:
        """Resolve natural language text to ontology URIs via embedding similarity.

        Routing:
          kind="class" or "predicate"     -> _graph_matcher only
          kind="unit" or "quantity_kind"   -> _qudt_matcher only
          kind=None                        -> both, merged by score
        """
        def _to_dicts(results):
            return [
                {
                    "uri": r.uri,
                    "kind": r.kind,
                    "label": r.label,
                    "score": r.score,
                    "matched_surface": r.matched_surface,
                }
                for r in results
            ]

        if kind in ("class", "predicate"):
            return _to_dicts(
                self._graph_matcher.query(text=text, kind=kind, top_k=top_k, min_score=min_score)
            )
        elif kind in ("unit", "quantity_kind"):
            return _to_dicts(
                self._qudt_matcher.query(text=text, kind=kind, top_k=top_k, min_score=min_score)
            )
        else:
            # Query both matchers, merge results sorted by score
            graph_results = self._graph_matcher.query(text=text, kind=kind, top_k=top_k, min_score=min_score)
            qudt_results = self._qudt_matcher.query(text=text, kind=kind, top_k=top_k, min_score=min_score)
            merged = sorted(
                graph_results + qudt_results,
                key=lambda r: r.score,
                reverse=True,
            )
            # Deduplicate by URI
            seen: set[str] = set()
            deduped = []
            for r in merged:
                if r.uri not in seen:
                    seen.add(r.uri)
                    deduped.append(r)
                    if len(deduped) >= top_k:
                        break
            return _to_dicts(deduped)

    ###########################################
    #################### API ###############
    ###########################################


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace = True, wait_for_embedding: bool = False) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: An `xml.sax.xmlreader.InputSource`, file-like object,
            `pathlib.Path` like object, or string. In the case of a string the string
            is the location of the source.
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
            wait_for_embedding: If True, blocks until the embedding index rebuild is
                complete and logs progress. If False (default), rebuilds in the background.
        """

        try:
            self.graph_store.insert_graph(rdf_graph, format=format, replace=replace)
            logging.info("acquirium: inserted graph into store, now ingesting data")
            self._connect_mqtt_streams_from_graph()
            self._scan_pg_references_from_graph()

            if wait_for_embedding:
                logger.info("acquirium: rebuilding embedding index (synchronous)...")
                self._rebuild_graph_index_background()
                logger.info("acquirium: embedding index rebuild complete")
            else:
                self._executor.submit(self._rebuild_graph_index_background)

        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise
    
    def timeseries_batch(
        self,
        uri: str,
        start: str | None = None,
        end: str | None = None,
        limit: int | None = None,
        order: str = "asc",
        batch_size: int = 50_000,
    ) :
        """
        Retrieve time series data for a given point URI within an optional time range.

        If the URI is a registered PGReference, data is fetched directly from
        the external Postgres database.  Otherwise the internal TimescaleDB is used.

        Returns:
            An iterator that yields batches of time series data as Arrow RecordBatches.
        """
        if self.pg_registry.is_pg_reference(uri):
            return self.pg_registry.timeseries(
                point_uri=uri,
                start=start,
                end=end,
                limit=limit,
                order=order,
                batch_size=batch_size,
            )
        return self.timescale.timeseries(
            point_uri=uri,
            start=start,
            end=end,
            limit=limit,
            order=order,
            batch_size=batch_size,
        )

    def timeseries_info_batch(self, uris: list[str]) -> dict:
        """Fetch stats for multiple URIs, dispatching to PGRegistry or TimescaleDB."""
        from acquirium.internals.models import TimeseriesInfo
        pg_uris = [u for u in uris if self.pg_registry.is_pg_reference(u)]
        ts_uris = [u for u in uris if not self.pg_registry.is_pg_reference(u)]
        result: dict[str, TimeseriesInfo] = {}
        if pg_uris:
            result.update(self.pg_registry.timeseries_info_batch(pg_uris))
        if ts_uris:
            result.update(self.timescale.timeseries_info_batch(ts_uris))
        return result

    def insert_timeseries(
        self,
        *,
        ref_uri: str,
        rows: list[tuple[datetime, Any]],
        point_uri: str | None = None,
        replace: bool = False,
    ) -> int:
        if replace:
            n = self.timescale.replace_rows(ref_uri, rows)
        else:
            n = self.timescale.upsert_rows(ref_uri, rows)
        if point_uri:
            self.timescale.ensure_stream_handle(point_uri, ref_uri)
        return n

    def _app_type_uri(self, app_type: str) -> URIRef:
        norm = (app_type or "").strip().lower()
        if norm in {"soft_sensor", "softsensor"}:
            return SOFT_SENSOR
        if norm == "threshold":
            return THRESHOLD
        if norm == "alarm":
            return ALARM
        if norm == "report":
            return REPORT
        if "://" in app_type or app_type.startswith("urn:"):
            return URIRef(app_type)
        return URIRef(str(ACQUIRIUM_NS[app_type]))

    def _app_storage_dir(self, app_id: str) -> Path:
        path = self.app_storage_root / app_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _ensure_package_inits(self, target_dir: Path, root: Path) -> None:
        current = target_dir
        while current != root and root in current.parents:
            init_file = current / "__init__.py"
            if not init_file.exists():
                init_file.write_text("")
            current = current.parent

    def _module_to_entry_file(self, module: str | None) -> str | None:
        if not module:
            return None
        return f"{module.replace('.', '/')}.py"

    def _add_literal_or_uri(self, graph: Graph, subj: URIRef, pred: URIRef, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str) and ("://" in value or value.startswith("urn:")):
            graph.add((subj, pred, URIRef(value)))
        else:
            graph.add((subj, pred, Literal(value)))

    def register_app_spec(self, spec: AppSpec) -> None:
        app_uri = URIRef(app_uri_for(spec.name))
        graph = Graph()

        graph.add((app_uri, RDF.type, APP))
        graph.add((app_uri, RDFS.label, Literal(spec.name)))
        if spec.app_type:
            graph.add((app_uri, RDF.type, self._app_type_uri(spec.app_type)))

        if spec.version:
            graph.add((app_uri, HAS_VERSION, Literal(spec.version)))
        if spec.module:
            graph.add((app_uri, HAS_MODULE, Literal(spec.module)))
        if spec.app_class:
            graph.add((app_uri, HAS_APP_CLASS, Literal(spec.app_class)))
        if spec.docker_image:
            graph.add((app_uri, HAS_IMAGE, Literal(spec.docker_image)))
        if spec.entrypoint:
            graph.add((app_uri, HAS_ENTRYPOINT, Literal(spec.entrypoint)))
        if spec.command:
            graph.add((app_uri, HAS_COMMAND, Literal(spec.command)))
        if spec.queries:
            graph.add((app_uri, APP_QUERY, Literal(json.dumps(spec.queries, sort_keys=True, ensure_ascii=True))))

        for dep in spec.depends_on:
            graph.add((app_uri, DEPENDS_ON, URIRef(dep)))

        for out in spec.outputs:
            point_uri = URIRef(out.point_uri)
            ref_uri = URIRef(out.ref_uri or make_stream_ref_uri(out.point_uri))

            graph.add((app_uri, PRODUCES, point_uri))
            graph.add((point_uri, RDF.type, VIRTUAL_POINT))
            graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
            graph.add((ref_uri, RDF.type, STREAM))
            if out.kind in {"event", "trigger"}:
                graph.add((ref_uri, RDF.type, EVENT_STREAM))
            else:
                graph.add((ref_uri, RDF.type, TIMESERIES_STREAM))

            graph.add((ref_uri, STORAGE_BACKEND, Literal(out.storage_backend or "timescale")))

            self._add_literal_or_uri(graph, point_uri, HAS_QUANTITY_KIND, out.quantity_kind)
            self._add_literal_or_uri(graph, point_uri, HAS_UNIT, out.unit)
            self._add_literal_or_uri(graph, point_uri, DATA_SOURCE, out.data_source)
            for dep in spec.depends_on:
                graph.add((point_uri, IS_CALCULATED_FROM, URIRef(dep)))

        app_dir = self._app_storage_dir(spec.name)
        entry_file = spec.entry_file or self._module_to_entry_file(spec.module) or "app.py"
        entry_file = entry_file.replace("\\", "/")
        if entry_file.startswith("/") or ".." in entry_file.split("/"):
            entry_file = "app.py"
        if spec.source_code:
            target = app_dir / entry_file
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(spec.source_code)
            self._ensure_package_inits(target.parent, app_dir)

        meta = {"entry_file": entry_file}
        (app_dir / "app.json").write_text(json.dumps(meta, ensure_ascii=True, sort_keys=True))

        self.graph_store.insert_graph(graph, format="turtle", replace=False)

    def _lookup_app_runtime(self, app_id: str) -> dict[str, str | None]:
        app_uri = app_uri_for(app_id)
        q = f"""
        SELECT ?image ?module ?cls ?entry ?cmd
        WHERE {{
          BIND(<{app_uri}> AS ?app)
          OPTIONAL {{ ?app <{HAS_IMAGE}> ?image . }}
          OPTIONAL {{ ?app <{HAS_MODULE}> ?module . }}
          OPTIONAL {{ ?app <{HAS_APP_CLASS}> ?cls . }}
          OPTIONAL {{ ?app <{HAS_ENTRYPOINT}> ?entry . }}
          OPTIONAL {{ ?app <{HAS_COMMAND}> ?cmd . }}
        }}
        """
        res = self.graph_store.sparql_query(q, use_union=True)
        rows = res.get("rows", [])
        if not rows:
            raise ValueError(f"App not found: {app_id}")

        cols = res.get("columns", [])
        idx = {name: i for i, name in enumerate(cols)}

        def pick(name: str) -> str | None:
            i = idx.get(name)
            if i is None:
                return None
            for row in rows:
                if i < len(row) and row[i] is not None:
                    return str(row[i])
            return None

        return {
            "image": pick("image"),
            "module": pick("module"),
            "app_class": pick("cls"),
            "entrypoint": pick("entry"),
            "command": pick("cmd"),
        }

    def _run_app_once(self, req: AppRunRequest, *, keep_alive: bool = False, interval: float | None = None) -> str:
        if self._docker is None:
            raise ValueError("Docker is not available - cannot run apps")

        runtime = self._lookup_app_runtime(req.app_id)
        logger.info("Running app %s with runtime config: %s", req.app_id, runtime)

        image = runtime.get("image") or os.getenv("ACQUIRIUM_DEFAULT_APP_IMAGE")
        if not image:
            raise ValueError(f"App {req.app_id} has no docker image configured")

        # Paths inside the worker container (backed by named volume)
        container_data_root = os.getenv("ACQUIRIUM_APP_DATA_ROOT", "/app/.acquirium")
        container_app_root = f"{container_data_root}/apps/{req.app_id}"

        # entry file discovery remains server side; keep it if you need it
        app_dir = self._app_storage_dir(req.app_id)
        entry_file = None
        meta_path = app_dir / "app.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                entry_file = meta.get("entry_file")
            except Exception:
                entry_file = None

        env = {
            "ACQUIRIUM_APP_ID": req.app_id,
            "ACQUIRIUM_APP_MODULE": runtime.get("module") or "",
            "ACQUIRIUM_APP_CLASS": runtime.get("app_class") or "",
            "ACQUIRIUM_RUN_START": req.start.isoformat() if req.start else "",
            "ACQUIRIUM_RUN_END": req.end.isoformat() if req.end else "",
            "ACQUIRIUM_APP_PARAMS": json.dumps(req.params or {}, ensure_ascii=True),
            "ACQUIRIUM_SERVER_URL": os.getenv("ACQUIRIUM_APP_SERVER_URL", "acquirium"),
            "ACQUIRIUM_SERVER_PORT": os.getenv("ACQUIRIUM_APP_SERVER_PORT", "8000"),
            "ACQUIRIUM_USE_SSL": os.getenv("ACQUIRIUM_APP_USE_SSL", "false"),
            "ACQUIRIUM_APP_ROOT": container_app_root,
            "PYTHONPATH": f"/app/src:{container_app_root}",
        }
        if keep_alive:
            env["ACQUIRIUM_KEEP_ALIVE"] = "true"
            env["ACQUIRIUM_KEEP_ALIVE_INTERVAL"] = str(interval if interval is not None else req.interval)
        if entry_file:
            env["ACQUIRIUM_APP_FILE"] = f"{container_app_root}/{entry_file}"

        # Filter out empty environment values
        env = {k: v for k, v in env.items() if v}

        network = os.getenv("ACQUIRIUM_APP_NETWORK")
        volume_name = os.getenv("ACQUIRIUM_APP_VOLUME", "acquirium_acquirium_data")

        # Build volume mount specification
        volumes = {
            volume_name: {"bind": container_data_root, "mode": "ro"}
        }

        # Build the command to run inside the container
        run_cmd = runtime.get("command") or "python -m acquirium.Apps.worker"
        shell_cmd = f"/app/.venv/bin/{run_cmd}" if run_cmd.startswith("python ") else f"/app/.venv/bin/python -m acquirium.Apps.worker"

        # Optional custom entrypoint
        entrypoint = runtime.get("entrypoint")

        # Generate container name (sanitize app_id for Docker naming rules)
        safe_app_id = re.sub(r'[^a-zA-Z0-9_.-]', '_', req.app_id)
        container_name = f"acquirium_app_{safe_app_id}"

        logger.info(
            "Starting container: name=%s, image=%s, network=%s, volume=%s, env_keys=%s",
            container_name, image, network, volume_name, list(env.keys())
        )

        try:
            container = self._docker.containers.run(
                image=image,
                name=container_name,
                command=["sh", "-lc", shell_cmd],
                entrypoint=entrypoint if entrypoint else None,
                environment=env,
                volumes=volumes,
                network=network if network else None,
                extra_hosts={"host.docker.internal": "host-gateway"},  # Linux compatibility
                detach=True,
                auto_remove=True,
            )
        except DockerException as e:
            logger.error("Failed to start container for app %s: %s", req.app_id, e)
            raise ValueError(f"Failed to run app {req.app_id}: {e}") from e

        cid = container.id
        logger.info("Started docker container for app %s: %s", req.app_id, cid[:12])
        return cid

    def run_app(self, req: AppRunRequest) -> str:
        if not req.keep_alive:
            return self._run_app_once(req)

        cid = self._run_app_once(req, keep_alive=True, interval=req.interval)
        with self._app_runs_lock:
            self._app_runs[cid] = {"app_id": req.app_id, "cid": cid}
        return cid

    def _stop_container(self, cid: str) -> None:
        if self._docker is None:
            logger.warning("Docker not available, cannot stop container %s", cid)
            return
        try:
            container = self._docker.containers.get(cid)
            container.stop(timeout=10)
            logger.info("Stopped container %s", cid[:12])
        except ContainerNotFound:
            logger.debug("Container %s already stopped or removed", cid[:12])
        except DockerException:
            logger.exception("Failed to stop container %s", cid[:12])

    def stop_app(self, *, run_id: str | None = None, app_id: str | None = None) -> dict[str, Any]:
        if not run_id and not app_id:
            raise ValueError("stop_app requires run_id or app_id")

        to_stop: list[str] = []
        with self._app_runs_lock:
            if run_id:
                # Allow stopping by run_id even if not tracked (for cleanup)
                to_stop.append(run_id)
            else:
                for rid, info in self._app_runs.items():
                    if app_id == "*" or info.get("app_id") == app_id:
                        to_stop.append(rid)

        stopped: list[str] = []
        for rid in to_stop:
            with self._app_runs_lock:
                record = self._app_runs.pop(rid, None)
            cid = record.get("cid") if record else rid
            if cid:
                self._stop_container(cid)
            stopped.append(rid)

        return {"stopped": len(stopped), "run_ids": stopped}

    def list_app_runs(self, *, app_id: str | None = None) -> list[dict[str, Any]]:
        with self._app_runs_lock:
            runs = list(self._app_runs.values())
        if app_id:
            runs = [r for r in runs if r.get("app_id") == app_id]
        return [{"run_id": r.get("cid"), "app_id": r.get("app_id")} for r in runs]

    
    def ingest_reference_bytes(
            self,
            *,
            data_uri: str,
            ref_uri: str,
            ref_type: str,
            content: bytes,
            time_column_no: int = 0,
            value_column_no: int = 1,
            filename: str = "upload",
        ) -> int:
        import polars as pl
        from io import BytesIO
        import time

        # Optional: cache using sha256 of bytes to avoid re-ingesting same ref
        digest = hashlib.sha256(content).hexdigest()
        cache_key = ref_uri
        with self._ingest_cache_lock:
            cache = self._load_ingest_cache()
            prev: dict = cache.get(cache_key, {})
            if prev.get("sha256") == digest and prev.get("status") == "done":
                return int(prev.get("rows_ingested", 0) or 0)
            cache[cache_key] = {
                "sha256": digest,
                "status": "scheduled",
                "filename": filename,
            }
            self._save_ingest_cache(cache)

        try:
            bio = BytesIO(content)

            if ref_type == str(PARQUET_REF):
                df = pl.read_parquet(bio, columns=[time_column_no, value_column_no])
            elif ref_type == str(CSV_REF):
                df = pl.read_csv(bio, columns=[time_column_no, value_column_no])
            else:
                raise ValueError(f"Unsupported reference type: {ref_type}")

            # Rename selected columns to ts/value regardless of original names
            if df.width != 2:
                raise ValueError(f"Expected 2 columns after selection, got {df.width}")

            df = df.rename({df.columns[0]: "ts", df.columns[1]: "value"})

            df = df.with_columns(pl.lit(ref_uri).alias("point_uri"))
            df = df.select(["point_uri", "ts", "value"])

            if df.schema.get("ts") == pl.Utf8:
                # Try with UTC timezone first (handles tz-aware strings like
                # "2026-01-27T23:30:16.668982+00:00"), fall back to naive parse.
                try:
                    df = df.with_columns(
                        pl.col("ts").str.to_datetime(time_zone="UTC")
                    )
                except Exception:
                    df = df.with_columns(pl.col("ts").str.to_datetime())

            if df.schema.get("value") != pl.Utf8:
                df = df.with_columns(pl.col("value").cast(pl.Utf8))

            result = self.timescale.bulk_insert_polars(df)

            with self._ingest_cache_lock:
                cache = self._load_ingest_cache()
                entry = cache.get(cache_key, {})
                entry["status"] = "done"
                entry["ingested_at"] = time.time()
                entry["rows_ingested"] = result
                entry["filename"] = filename
                cache[cache_key] = entry
                self._save_ingest_cache(cache)

            return int(result)

        except Exception as exc:
            with self._ingest_cache_lock:
                cache = self._load_ingest_cache()
                entry = cache.get(cache_key, {})
                entry["status"] = "error"
                entry["error"] = str(exc)
                entry["filename"] = filename
                cache[cache_key] = entry
                self._save_ingest_cache(cache)
            raise

    def insert_log(self, log_message: LogEntry):
        """
        Insert a log entry into timescale store.

        Add external reference to the graph. 
        This is for associating log metadata with points.
        If we want to associate metadata with the logs of a point, we can do so here.
        """
        self.timescale.insert_log(log_message)
        logger.info("Inserted log entry for point %s at %s to database", log_message.point_uri, log_message.timestamp)
        G = Graph()
        log_uri = URIRef(f"{str(log_message.point_uri)}_log")
        G.add((URIRef(log_message.point_uri), HAS_LOG, log_uri))
        G.add((log_uri, RDF.type, LOGBOOK))
        self.graph_store.insert_graph(G, format="turtle", replace=False)
        logger.info("Inserted log entry for point %s at %s to graph", log_message.point_uri, log_message.timestamp)


    def query_logs(
        self,
        point_uri: str,
        log_time_interval: TimeIntervalModel | None = None,
        obs_time_interval: TimeIntervalModel | None = None
    ) -> list[LogEntry]:
        """
        Query log entries for a given point URI within an optional time interval.

        Args:
            point_uri: The URI of the time series point.
            log_time_interval: Optional TimeIntervalModel object specifying start and end times for log entries.
            obs_time_interval: Optional TimeIntervalModel object specifying start and end times for observation entries.

        Returns:
            A list of LogEntry objects.
        """
        return self.timescale.query_logs(
            point_uri=point_uri,
            log_time_interval=log_time_interval,
            obs_time_interval=obs_time_interval
        )

    def delete_logs(self, point_uri: str) -> None:
        """
        Delete all log entries for a given point URI.

        Args:
            point_uri: The URI of the time series point.
        """
        if not self.timescale.delete_logs(point_uri):
            logger.warning("Failed to delete log entries for point %s from database", point_uri)
            return False
        logger.info("Deleted all log entries for point %s from database", point_uri)
        # Optionally, remove log references from the graph
        q = f"""
        DELETE WHERE {{
          <{point_uri}> <{HAS_LOG}> ?log .
          ?log a <{LOGBOOK}> .
        }}
        """
        self.graph_store.sparql_update(q)
        logger.info("Deleted all log references for point %s from graph", point_uri)
        return True

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

    def embedding_status(self) -> dict[str, Any]:
        """Return the current state of each embedding index."""
        with self._embedding_status_lock:
            return {k: dict(v) for k, v in self._embedding_status.items()}

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
            self.stop_app(app_id="*")
        except Exception:
            pass
        try:
            self._executor.shutdown(wait=False, cancel_futures=False)
        except Exception:
            pass
        try:
            self.mqtt_ingest.stop()
        except Exception:
            pass
        try:
            if self._docker is not None:
                self._docker.close()
        except Exception:
            pass
        self.timescale.close()
        self.graph_store.close()
