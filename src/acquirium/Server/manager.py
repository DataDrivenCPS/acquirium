from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import os
import logging
from time import perf_counter
from rdflib import Graph, URIRef, Literal, RDF, RDFS, SKOS

from acquirium.Storage import (
    OxigraphGraphStore,
    TimeseriesStore,
    create_timeseries_store,
)
from acquirium.Storage.values import normalize_value_kind
from acquirium.internals.qudt_units import QUDTUnitConverter
from acquirium.internals.models import LogEntry, Order, TimeIntervalModel, AppSpec, AppRunRequest, compute_ref_uri
from acquirium.internals.internals_namespaces import *
from acquirium.internals.app_utils import app_uri_for

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import pyarrow as pa
import shutil
import docker
from docker.errors import DockerException, NotFound as ContainerNotFound
from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher, _split_local_name
from acquirium.TextMatch.qudt_store import QUDTStore
from acquirium.TextMatch.resolver import ConceptResolver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("acquirium.manager")
logger.setLevel(logging.INFO)

DEFAULT_DATA_DIR = Path(".acquirium")
DEFAULT_DB_NAME = "acquirium"
RECREATE_WARNING = (
    "SERVER STARTED WITH recreate=True. Existing Acquirium data will be erased now. "
    "Restarting this server with recreate=True will erase data again."
)


def _aggregate_uri_label_rows(
    rows: list,
    seen: set[str],
    kind: str,
    concepts: list[dict[str, Any]],
) -> None:
    """Aggregate SPARQL (uri, label) rows into *concepts*, skipping already-seen URIs."""
    uri_labels: dict[str, list[str]] = {}
    uri_first_label: dict[str, str | None] = {}
    for row in rows:
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
        if uri in seen:
            continue
        seen.add(uri)
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
        display_label = uri_first_label[uri] or (" ".join(tokens) if tokens else uri)
        concepts.append({
            "uri": uri,
            "kind": kind,
            "label": display_label,
            "surfaces": surfaces,
            "related": [],
        })


def _wipe_dir_contents(base: Path) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for p in base.iterdir():
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink()



@dataclass
class Manager:
    timescale: TimeseriesStore
    graph_store: OxigraphGraphStore
    qudt_converter: QUDTUnitConverter | None = None
    backend: str = "timescale"

    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        pg_dsn: str | None = None,
        duckdb_path: str | Path | None = None,
        timeseries_backend: str = "timescale",
        graph_path: str | Path | None = None,
        ontoenv_root: str | Path | None = None,
        graph_name: str | None = None,
        ontology_dependencies: list[str] | None = None,
        qudt_graph: Graph | None = None,
        qudt_converter: QUDTUnitConverter | None = None,
        recreate: bool = False,
    ):
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(name)s %(message)s",
            )
        start = perf_counter()

        base = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if recreate:
            logger.warning(RECREATE_WARNING)
            if base.exists():
                _wipe_dir_contents(base)
                print(f"Deleted data directory contents: {base}")
        base.mkdir(parents=True, exist_ok=True)
        graph_path = Path(graph_path) if graph_path is not None else base / ".oxigraph"
        ontoenv_root = Path(ontoenv_root) if ontoenv_root is not None else base

        _backend = timeseries_backend.lower()
        if _backend == "duckdb":
            _effective_dsn = None
            _effective_duckdb_path = duckdb_path or (base / "timeseries.duckdb")
            timescale: TimeseriesStore = create_timeseries_store(
                "duckdb", duckdb_path=_effective_duckdb_path, recreate=recreate
            )
        else:
            _effective_dsn = pg_dsn or os.getenv("PG_DSN")
            if not _effective_dsn:
                raise ValueError("PG_DSN required for timescale backend. Set pg_dsn or PG_DSN env var.")
            timescale = create_timeseries_store(
                "timescale", pg_dsn=_effective_dsn, recreate=recreate
            )

        converter = qudt_converter
        if converter is None and qudt_graph is not None:
            converter = QUDTUnitConverter(qudt_graph)
        if converter is None:
            # Auto-detect local QUDT unit ontology
            _qudt_local = Path("ontologies/qudt_unit.ttl")
            if _qudt_local.exists():
                try:
                    converter = QUDTUnitConverter(str(_qudt_local))
                    logging.info("acquirium: auto-loaded QUDTUnitConverter from %s", _qudt_local)
                except Exception as exc:
                    logging.warning("acquirium: failed to load QUDT converter from %s: %s", _qudt_local, exc)

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


        self.timescale = timescale
        self.graph_store = graph
        self.qudt_converter = converter
        self.backend = _backend

        self.data_dir = base
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="acquirium-ingest")
        self.app_storage_root = Path(
            os.getenv("ACQUIRIUM_APP_STORAGE_ROOT", str(self.data_dir / "apps"))
        )
        self.app_storage_root.mkdir(parents=True, exist_ok=True)
        self._app_runs: dict[str, dict[str, Any]] = {}
        self._app_runs_lock = threading.Lock()
        self._graph_change_listeners: list[Callable[[], None]] = []
        self._graph_change_listeners_lock = threading.Lock()
        self._graph_version: int = 0
        self._graph_version_lock = threading.Lock()

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

        # Single normalization façade, sharing the lazily-built converter.
        self._concept_resolver = ConceptResolver(
            graph_matcher=self._graph_matcher,
            qudt_matcher=self._qudt_matcher,
            converter_provider=self._ensure_qudt_converter,
        )

        # Kept for backward compat — points to graph matcher
        self.embedding_matcher = self._graph_matcher

        # Embedding index status tracking
        self._embedding_status_lock = threading.Lock()
        self._embedding_status: dict[str, dict[str, Any]] = {
            "graph": {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
            "qudt":  {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
        }

        # Embedding corpus is the static ontoenv vocabularies; build once.
        self._build_embedding_indexes()

    
    @classmethod
    def from_env(cls) -> Manager:
        _backend = os.getenv("ACQUIRIUM_TIMESERIES_BACKEND", "duckdb").lower()
        _data_dir = os.getenv("ACQUIRIUM_DATA_DIR")
        _ontology_deps_raw = os.getenv("ACQUIRIUM_ONTOLOGY_DEPENDENCIES")
        return cls(
            data_dir=_data_dir,
            pg_dsn=os.getenv("PG_DSN"),
            duckdb_path=os.getenv("ACQUIRIUM_DUCKDB_PATH"),
            timeseries_backend=_backend,
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            graph_name=os.getenv("ACQUIRIUM_GRAPH_NAME"),
            ontology_dependencies=_ontology_deps_raw.split(",") if _ontology_deps_raw else None,
            recreate=os.getenv("ACQUIRIUM_RECREATE", "false").lower() == "true",
        )

    def _scan_pg_references_from_graph(self) -> int:
        """Scan graph for external Postgres timeseries references.

        External Postgres historians use a ``ref:hasExternalReference`` node
        whose ``ref:storedAt`` is a literal DSN. Acquirium-managed streams are
        handled separately by ``_sync_stream_refs_from_graph`` and are
        identified by ``acq:sourceId``/``acq:refName`` on the same reference
        node.
        """
        q = f"""
        SELECT ?data ?ref ?dsn ?table ?query ?tcol ?vcol ?pfilter
        WHERE {{
          ?data <{HAS_EXTERNAL_REFERENCE}> ?ref .
          ?ref <{STORED_AT}> ?dsn .
          FILTER(isLiteral(?dsn))
          FILTER(STRSTARTS(STR(?dsn), "postgresql://") || STRSTARTS(STR(?dsn), "postgres://"))
          OPTIONAL {{ ?ref <{TIMESERIES_TABLE}> ?table . }}
          OPTIONAL {{ ?ref <{TIMESERIES_QUERY}> ?query . }}
          OPTIONAL {{ ?ref <{TIMESERIES_TIME_COLUMN}> ?tcol . }}
          OPTIONAL {{ ?ref <{TIMESERIES_VALUE_COLUMN}> ?vcol . }}
          OPTIONAL {{ ?ref <{TIMESERIES_POINT_FILTER}> ?pfilter . }}
        }}
        """
        res = self.graph_store.sparql_query(q, use_union=True)
        rows = res.get("rows", [])

        count = 0
        for row in rows:
            (data_uri, ref_uri, dsn, table, custom_query, tcol, vcol, pfilter) = row
            try:
                info = PGReferenceInfo(
                    dsn=self._sparql_value(dsn) or "",
                    table=self._sparql_value(table),
                    custom_query=self._sparql_value(custom_query),
                    time_col=self._sparql_value(tcol) or "time",
                    value_col=self._sparql_value(vcol) or "value",
                    point_filter=self._sparql_value(pfilter),
                )
                self.pg_registry.register(str(ref_uri), info)
                count += 1
            except Exception:
                logger.warning("Failed to register external Postgres reference %s", ref_uri, exc_info=True)

        if count:
            logger.info("Registered %d external Postgres reference(s) from graph", count)
        return count


    def _sync_stream_refs_from_graph(self) -> int:
        """Sync the streams reference table from Acquirium-managed timeseries refs.

        Matches every Acquirium-managed reference node:
            ?ref_node  acq:sourceId  ?source_id ;
                       acq:refName   ?ref_name .
        If a semantic point links to the reference with ``ref:hasExternalReference``,
        ``point_uri`` is stored too. Standalone source-local references are
        recorded with a null ``point_uri``.
        The reference-node URI is the canonical stream identity and should be
        equal to ``compute_ref_uri(source_id, ref_name)``. We upsert into the
        streams table using the graph's actual reference URI as the storage key.
        References without sourceId/refName are skipped; drivers are responsible
        for ingesting external data into managed streams.
        """
        q = f"""
        SELECT ?point ?ref_node ?source_id ?ref_name ?value_kind
        WHERE {{
          ?ref_node <{ACQUIRIUM_SOURCE_ID}> ?source_id .
          ?ref_node <{ACQUIRIUM_REF_NAME}> ?ref_name .
          OPTIONAL {{ ?ref_node <{ACQUIRIUM_VALUE_KIND}> ?value_kind . }}
          OPTIONAL {{ ?point <{HAS_EXTERNAL_REFERENCE}> ?ref_node . }}
        }}
        """
        # use_union=False: these acquirium-internal predicates only ever live
        # in the main graph, so skipping the closure rebuild on the hot
        # insert path is correct and avoids the per-insert closure refresh.
        res = self.graph_store.sparql_query(q, use_union=False)
        count = 0
        for point_uri, ref_node, source_id, ref_name, value_kind in res.get("rows", []):
            try:
                sid = str(source_id).strip('"')
                rn  = str(ref_name).strip('"')
                expected = compute_ref_uri(sid, rn)
                actual = URIRef(str(ref_node))
                if actual != expected:
                    raise ValueError(
                        f"Managed reference URI mismatch for point {point_uri}: "
                        f"graph has {actual}, expected {expected} from "
                        f"source_id={sid!r}, ref_name={rn!r}"
                    )
                point = str(point_uri) if point_uri is not None else None
                self.timescale.ensure_stream_ref(
                    point,
                    sid,
                    rn,
                    ref_uri=actual,
                    value_kind=normalize_value_kind(
                        str(value_kind).strip('"') if value_kind is not None else None
                    ),
                )
                count += 1
            except Exception:
                logger.warning("Failed to ensure stream ref %s / %s → %s", point_uri, source_id, ref_name, exc_info=True)
                raise
        if count:
            logger.info("Synced %d stream ref(s) from graph", count)
        return count

    # ----- Embedding index methods -----

    def _extract_concepts_for_embedding(self, graph: "Graph") -> list[dict[str, Any]]:
        """Extract class / predicate / substance concepts from *graph*.

        *graph* is the merged water + s223 vocabulary (read out of ontoenv,
        imports not followed). Unit / quantity_kind concepts come separately
        from the QUDT graphs via :class:`QUDTStore`.
        """
        concepts: list[dict[str, Any]] = []

        label_block_basic = f"""
          OPTIONAL {{
            {{ ?uri <{RDFS.label}> ?label . }}
            UNION {{ ?uri <{SKOS.prefLabel}> ?label . }}
            UNION {{ ?uri <{SKOS.altLabel}> ?label . }}
            FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
          }}
        """

        class_where = f"""
          {{ ?uri a <{RDFS.Class}> . }}
          UNION {{ ?uri a <{OWL_CLASS}> . }}
          UNION {{ ?x <{RDFS.subClassOf}> ?uri . }}
          UNION {{ ?x a ?uri . }}
          UNION {{ ?uri a <{WATR.Class}> . }}
          UNION {{ ?uri <{RDFS.subClassOf}> ?x . }}
          UNION {{ ?x <{HAS_ENUMERATION_KIND}> ?uri . }}
          UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
          UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
        """
        pred_where = f"""
          {{ ?uri a <{RDF_PROP}> . }}
          UNION {{ ?uri a <{OWL_OBJ_PROP}> . }}
          UNION {{ ?uri a <{OWL_DATA_PROP}> . }}
          UNION {{ ?s ?uri ?o . }}
        """
        # Constrained medium/substance space: the s223 substance enumeration
        # and the NAWI water medium taxonomy, plus whatever the loaded model
        # actually uses as a medium/substance (self-grounding so it's correct
        # regardless of the imported s223 closure).
        substance_where = f"""
          {{ ?uri (<{RDFS.subClassOf}>)* <{S223['EnumerationKind-Substance']}> . }}
          UNION {{ ?uri (<{RDFS.subClassOf}>)* <{WATR['Medium-Constituent']}> . }}
          UNION {{ ?x <{S223.ofMedium}> ?uri . }}
          UNION {{ ?x <{HAS_MEDIUM}> ?uri . }}
          UNION {{ ?x <{OF_SUBSTANCE}> ?uri . }}
        """

        extractions: list[tuple[str, str, str]] = [
            ("class", class_where, label_block_basic),
            ("predicate", pred_where, label_block_basic),
            ("substance", substance_where, label_block_basic),
        ]

        for kind, where, label_block in extractions:
            query = f"""
            SELECT DISTINCT ?uri ?label WHERE {{
              {where}
              {label_block}
              FILTER(isIRI(?uri))
            }}
            """
            seen: set[str] = set()
            try:
                rows = list(graph.query(query))
                _aggregate_uri_label_rows(rows, seen, kind, concepts)
            except Exception:
                logger.warning("Failed to extract %s concepts", kind, exc_info=True)

        return concepts

    def _update_embedding_status(self, index: str, **kwargs: Any) -> None:
        """Update the embedding status for a given index (thread-safe)."""
        with self._embedding_status_lock:
            self._embedding_status[index].update(kwargs)

    def _mark_index_ready(
        self, index: str, concepts: list[dict[str, Any]], t0: float
    ) -> None:
        """Record an index as ready with concept/surface counts and timing."""
        self._update_embedding_status(
            index, state="ready", concepts=len(concepts),
            surfaces=sum(len(c.get("surfaces", [])) for c in concepts),
            last_built=datetime.now().isoformat(),
            duration_s=round(perf_counter() - t0, 2), error=None,
        )

    def _build_embedding_indexes(self) -> None:
        """Build both embedding indexes once from the static ontoenv graphs.

        graph matcher <- water + s223 vocabularies merged (class / predicate
        / substance); qudt matcher <- the QUDT unit + quantity_kind
        vocabularies. Graphs are read out of ontoenv by IRI (owl:imports
        not followed); no inserted data is embedded.
        """
        iris = self.graph_store.ontology_iris()

        self._update_embedding_status("graph", state="building")
        t0 = perf_counter()
        try:
            merged = Graph()
            for key in ("water", "s223"):
                if key in iris:
                    for triple in self.graph_store.named_graph(iris[key]):
                        merged.add(triple)
            concepts = self._extract_concepts_for_embedding(merged)
            if concepts:
                self._graph_matcher.build_index(concepts)
            self._mark_index_ready("graph", concepts, t0)
            logger.info(
                "Graph embedding index: %d concepts (water+s223)", len(concepts)
            )
        except Exception as exc:
            self._update_embedding_status("graph", state="error", error=str(exc))
            logger.warning("Failed to build graph embedding index", exc_info=True)

        self._update_embedding_status("qudt", state="building")
        t0 = perf_counter()
        try:
            qc: list[dict[str, Any]] = []
            if "unit" in iris:
                qc += QUDTStore.extract_concepts(
                    self.graph_store.named_graph(iris["unit"]), str(QUDT.Unit)
                )
            if "quantity_kind" in iris:
                qc += QUDTStore.extract_concepts(
                    self.graph_store.named_graph(iris["quantity_kind"]),
                    str(QUDT.QuantityKind),
                )
            if qc:
                self._qudt_matcher.build_index(qc)
            self._mark_index_ready("qudt", qc, t0)
            logger.info("QUDT embedding index: %d concepts", len(qc))
        except Exception as exc:
            self._update_embedding_status("qudt", state="error", error=str(exc))
            logger.warning("Failed to build QUDT embedding index", exc_info=True)

    def resolve_text(
        self,
        text: str,
        kind: str | None = None,
        top_k: int = 5,
        min_score: float = 0.5,
        context: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Resolve natural language text to ontology URIs.

        Thin delegation to :class:`ConceptResolver`, which owns the full
        resolution policy (data-graph + QUDT matchers, deterministic unit
        converter tier, context rerank). Conversion is a separate concern and
        still goes through ``QUDTUnitConverter`` directly
        (:meth:`resolve_unit_info` / :meth:`get_conversion_factors`).

        Example::

            # asset-type label from an equipment register row
            resolve_text("sedimentation tank", kind="class", top_k=2)
            # -> [{"uri": "urn:nawi-water-ontology#SedimentationTank",
            #      "kind": "class", "score": 1.0,
            #      "match_stage": "exact", ...}, ...]
        """
        results = self._concept_resolver.resolve(
            text, kind=kind, top_k=top_k, min_score=min_score, context=context
        )
        return [asdict(r) for r in results]

    def resolve_record(
        self,
        fields: dict[str, tuple[str, str | None]],
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> dict[str, list[dict[str, Any]]]:
        """Jointly resolve a record's fields.

        Thin delegation to :meth:`ConceptResolver.resolve_record`: related
        fields (e.g. unit and its quantity kind) reinforce each other so a
        confident sibling disambiguates an ambiguous one.

        ``fields`` keys are caller-chosen labels echoed back unchanged in
        the result; the resolver never reads them. Resolution is driven by
        the ``(text, kind)`` tuple. The example keys below are styled as a
        historian export's column headers only to look like a real source.

        Example::

            resolve_record({"FIT-101.EU":  ("gal/min", "unit"),
                            "FIT-101.QTY": ("flow rate", "quantity_kind")})
            # -> {"FIT-101.EU":  [{"uri": ".../unit/GAL_US-PER-MIN", ...}, ...],
            #     "FIT-101.QTY": [{"uri": ".../quantitykind/VolumeFlowRate",
            #                      ...}, ...]}
        """
        resolved = self._concept_resolver.resolve_record(
            fields, top_k=top_k, min_score=min_score
        )
        return {name: [asdict(r) for r in rs] for name, rs in resolved.items()}

    ###########################################
    #################### API ###############
    ###########################################

    def add_graph_change_listener(self, callback: Callable[[], None]) -> None:
        """Register a callback to be invoked after any graph mutation.

        Listeners run synchronously on the thread that mutated the graph, so
        they should be cheap (typically: hand off the actual work to a
        background executor).
        """
        with self._graph_change_listeners_lock:
            if callback not in self._graph_change_listeners:
                self._graph_change_listeners.append(callback)

    def remove_graph_change_listener(self, callback: Callable[[], None]) -> None:
        with self._graph_change_listeners_lock:
            if callback in self._graph_change_listeners:
                self._graph_change_listeners.remove(callback)

    def _notify_graph_change(self) -> None:
        with self._graph_version_lock:
            self._graph_version += 1
        with self._graph_change_listeners_lock:
            listeners = list(self._graph_change_listeners)
        for cb in listeners:
            try:
                cb()
            except Exception:
                logger.warning("Graph change listener %r failed", cb, exc_info=True)

    def graph_version(self) -> int:
        """Monotonically-increasing version bumped on every graph mutation.

        Workers can poll this to detect when their cached query becomes stale.
        """
        with self._graph_version_lock:
            return self._graph_version


    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace = True) -> None:
        """
        Insert RDF graph into the graph store's main graph.

        The embedding index is refreshed synchronously before returning, so
        once this call completes the just-inserted concepts are resolvable.
        The refresh is incremental (only new concepts are embedded) unless
        ``replace=True``, which triggers a full rebuild.

        Args:
            :param rdf_graph: An `xml.sax.xmlreader.InputSource`, file-like object,
            `pathlib.Path` like object, or string. In the case of a string the string
            is the location of the source.
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
        """

        if isinstance(rdf_graph, Path):
            rdf_graph = rdf_graph.read_text()
        elif isinstance(rdf_graph, str):
            p = Path(rdf_graph)
            try:
                if p.is_file():
                    rdf_graph = p.read_text()
            except OSError:
                pass

        try:
            self.graph_store.insert_graph(rdf_graph, format=format, replace=replace)
            logging.info("acquirium: inserted graph into store")
            self._sync_stream_refs_from_graph()
            # Embedding corpus is the static ontoenv vocabularies, not
            # inserted data — no per-insert reindex. refresh_union (inside
            # graph_store.insert_graph) keeps the data/SPARQL union current.
            self._notify_graph_change()

        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise
    
    def timeseries_batch(
        self,
        uri: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: Order = "asc",
        batch_size: int = 50_000,
        value_mode: str = "default",
    ) :
        """
        Retrieve time series data for a given point URI within an optional time range.

        Returns:
            An iterator that yields batches of time series data as Arrow RecordBatches.
        """
        storage_key = self.timescale.resolve_storage_key(uri)
        return self.timescale.timeseries(
            ref_uri=storage_key,
            start=start,
            end=end,
            limit=limit,
            order=order,
            batch_size=batch_size,
            value_mode=value_mode,
        )

    def timeseries_info_batch(self, uris: list[str]) -> dict:
        """Fetch stats for multiple stream or point URIs."""
        if not uris:
            return {}
        uri_to_key = self.timescale.resolve_storage_keys(uris)
        key_to_uri = {v: k for k, v in uri_to_key.items()}
        raw = self.timescale.timeseries_info_batch(list(key_to_uri.keys()))
        return {key_to_uri[k]: v for k, v in raw.items()}

    @staticmethod
    def _sparql_value(value: Any) -> str | None:
        if value is None:
            return None
        return str(value).strip('"')

    def register_datasource(self, source_id: str) -> str:
        """Register a named datasource in the knowledge graph.

        Writes a graph node typed as acquirium:DataSourceRegistry so the
        datasource is discoverable via SPARQL.  Idempotent — safe to call
        on every startup.  Returns source_id.
        """
        node = URIRef(f"urn:acquirium:datasource:{source_id}")
        g = Graph()
        g.add((node, RDF.type,   ACQUIRIUM_DATASOURCE))
        g.add((node, RDFS.label, Literal(source_id)))
        self.graph_store.insert_graph(g, format="turtle", replace=False)
        return source_id

    def insert_timeseries(
        self,
        *,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        point_uri: str | None = None,
        replace: bool = False,
    ) -> int:
        ref_uri = str(compute_ref_uri(source_id, ref_name))
        value_kind = self._registered_value_kind(ref_uri)
        if replace:
            n = self.timescale.replace_rows(ref_uri, rows, value_kind=value_kind)
        else:
            n = self.timescale.upsert_rows(ref_uri, rows, value_kind=value_kind)
        return n

    def insert_timeseries_batch(
        self,
        source_id: str,
        streams: dict[str, list[tuple[datetime, Any]]],
    ) -> int:
        """Insert multiple source-local streams in one storage operation."""
        import polars as pl

        ref_uris: list[str] = []
        timestamps: list[datetime] = []
        values: list[Any] = []
        value_kinds: list[str] = []
        for ref_name, stream_rows in streams.items():
            ref_uri = str(compute_ref_uri(source_id, ref_name))
            value_kind = self._registered_value_kind(ref_uri)
            for ts, value in stream_rows:
                ref_uris.append(ref_uri)
                timestamps.append(ts)
                values.append(value)
                value_kinds.append(value_kind)
        if not ref_uris:
            return 0

        df = pl.DataFrame(
            {"ref_uri": ref_uris, "ts": timestamps, "value": values, "value_kind": value_kinds},
            schema={
                "ref_uri": pl.Utf8,
                "ts": pl.Datetime("us", "UTC"),
                "value": pl.Object,
                "value_kind": pl.Utf8,
            },
        )
        return self.timescale.bulk_insert_polars(df)

    def insert_timeseries_arrow(self, source_id: str, table: "pa.Table") -> int:
        """Insert a melted (ts, ref_name, value) Arrow table, computing ref_uris vectorized."""
        import polars as pl

        if len(table) == 0:
            return 0
        df = pl.from_arrow(table)
        ref_uri_map: dict[str, str] = {}
        value_kind_map: dict[str, str] = {}
        for name in df["ref_name"].unique().to_list():
            ref_uri = str(compute_ref_uri(source_id, name))
            ref_uri_map[name] = ref_uri
            value_kind_map[name] = self._registered_value_kind(ref_uri)
        df = (
            df.with_columns([
                pl.col("ref_name").replace(ref_uri_map).alias("ref_uri"),
                pl.col("ref_name").replace(value_kind_map).alias("value_kind"),
            ])
            .drop("ref_name")
            .select(["ref_uri", "ts", "value", "value_kind"])
        )
        return self.timescale.bulk_insert_polars(df)

    def _registered_value_kind(self, ref_uri: str) -> str:
        value_kind = self.timescale.stream_value_kind(ref_uri)
        if value_kind is None:
            raise ValueError(f"stream {ref_uri} is not registered")
        return normalize_value_kind(value_kind)

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
            ref_uri = compute_ref_uri(spec.name, out.point_uri)

            graph.add((app_uri, PRODUCES, point_uri))
            graph.add((point_uri, RDF.type, VIRTUAL_POINT))
            graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
            graph.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(spec.name)))
            graph.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(out.point_uri)))
            graph.add((ref_uri, RDF.type, STREAM))
            if out.kind in {"event", "trigger"}:
                graph.add((ref_uri, RDF.type, EVENT_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("text")))
            else:
                graph.add((ref_uri, RDF.type, TIMESERIES_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal("numeric")))

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
        self._notify_graph_change()

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

    def _run_app_once(self, req: AppRunRequest, *, keep_alive: bool = False, interval: float | None = None) -> str | None :
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
        shell_cmd = f"/app/.venv/bin/{run_cmd}" if run_cmd.startswith("python ") else run_cmd

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
        if isinstance(cid,str):
            logger.info("Started docker container for app %s: %s", req.app_id, cid[:12])
        else: 
            logger.warning("Container ID is not a string for app %s: %s", req.app_id, cid)
        return cid

    def run_app(self, req: AppRunRequest) -> str | None:
        if not req.keep_alive:
            return self._run_app_once(req)

        cid = self._run_app_once(req, keep_alive=True, interval=req.interval)
        with self._app_runs_lock:
            if isinstance(cid, str):
                self._app_runs[cid] = {"app_id": req.app_id, "cid": cid}
            else:
                logger.warning("Received non-string container ID for app %s: %s", req.app_id, cid)
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


    def insert_log(self, log_message: LogEntry):
        self.timescale.insert_log(log_message)
        G = Graph()
        log_uri = URIRef(f"{str(log_message.point_uri)}_log")
        G.add((URIRef(log_message.point_uri), HAS_LOG, log_uri))
        G.add((log_uri, RDF.type, LOGBOOK))
        # Write bookkeeping triples but skip _notify_graph_change — log inserts
        # don't affect the ontology/concept space and would otherwise continuously
        # invalidate the embedding cache.
        self.graph_store.insert_graph(G, format="turtle", replace=False)


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

    def delete_logs(self, point_uri: str) -> bool:
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
        self._notify_graph_change()
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

    # -------------------- Unit conversion --------------------

    def _ensure_qudt_converter(self) -> QUDTUnitConverter:
        """Lazily initialize the QUDT converter if not already available."""
        if self.qudt_converter is not None:
            return self.qudt_converter
        _qudt_local = Path("ontologies/qudt_unit.ttl")
        if _qudt_local.exists():
            self.qudt_converter = QUDTUnitConverter(str(_qudt_local))
            logger.info("Lazily loaded QUDTUnitConverter from %s", _qudt_local)
            return self.qudt_converter
        raise ValueError(
            "QUDT converter not available. Place ontologies/qudt_unit.ttl "
            "in the working directory or pass qudt_graph to Manager."
        )

    def resolve_unit_info(self, identifier: str) -> dict[str, Any]:
        """Resolve a unit identifier to its QUDT metadata (deterministic).

        Example::

            # engineering-unit string off a flow meter tag
            resolve_unit_info("gal/min")
            # -> {"uri": "http://qudt.org/vocab/unit/GAL_US-PER-MIN",
            #     "label": "US Gallon per Minute", "symbol": "gal/min",
            #     "quantity_kind": ".../quantitykind/VolumeFlowRate",
            #     "multiplier": 6.30901964e-05, "offset": 0.0}
        """
        converter = self._ensure_qudt_converter()
        unit_def = converter.resolve_unit(identifier)
        return {
            "uri": str(unit_def.uri),
            "label": unit_def.label,
            "symbol": unit_def.symbol,
            "quantity_kind": str(unit_def.quantity_kind) if unit_def.quantity_kind else None,
            "multiplier": unit_def.multiplier,
            "offset": unit_def.offset,
        }

    def get_conversion_factors(self, from_unit: str, to_unit: str) -> dict[str, Any]:
        """Return pre-computed conversion factors between two units.

        The client can apply: result = ((value + src_offset) * src_mult / tgt_mult) - tgt_offset
        """
        converter = self._ensure_qudt_converter()
        src = converter.resolve_unit(from_unit)
        tgt = converter.resolve_unit(to_unit)
        compatible = converter._check_compatible(src, tgt)
        return {
            "from_uri": str(src.uri),
            "to_uri": str(tgt.uri),
            "from_multiplier": src.multiplier,
            "from_offset": src.offset,
            "to_multiplier": tgt.multiplier,
            "to_offset": tgt.offset,
            "compatible": compatible,
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
            if self._docker is not None:
                self._docker.close()
        except Exception:
            pass
        self.timescale.close()
        self.graph_store.close()
