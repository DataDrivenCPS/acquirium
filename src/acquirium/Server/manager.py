from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import os
import logging
from threading import Lock
import pyoxigraph as ox
from time import perf_counter
from rdflib import Graph, URIRef, Literal, RDF, RDFS, SKOS
from rdflib.namespace import NamespaceManager
from acquirium.Storage import (
    OxigraphGraphStore,
    TimeseriesStore,
    create_timeseries_store,
)
from acquirium.Storage.publication.types import PublicationReceipt, PublicationRequest, PublicationStore
from acquirium.Storage.values import normalize_value_kind
from acquirium.internals.qudt_units import QUDTUnitConverter
from acquirium.Server.config import OntologySource
from acquirium.internals.models import LogEntry, Order, TimeIntervalModel, compute_ref_uri
from acquirium.internals.internals_namespaces import *

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
import shutil
from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher, _split_local_name
from acquirium.TextMatch.qudt_store import QUDTStore
from acquirium.TextMatch.resolver import ConceptResolver

from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.manager")

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
    """Aggregate SPARQL (uri, label) rows into *concepts*, skipping already-seen URIs.

    SPARQL row order is not stable across processes; sort labels and iterate
    URIs in sorted order so the resulting concept dicts hash identically run
    to run (otherwise the embedding cache misses every restart).
    """
    uri_labels: dict[str, set[str]] = {}
    uri_first_label: dict[str, str | None] = {}
    for row in rows:
        uri = str(row[0]) if row[0] else None
        label = str(row[1]).strip('"') if row[1] else None
        if not uri:
            continue
        bucket = uri_labels.setdefault(uri, set())
        if label:
            bucket.add(label)

    for uri in sorted(uri_labels):
        if uri in seen:
            continue
        seen.add(uri)
        labels = sorted(uri_labels[uri])
        surfaces: list[str] = []
        for lbl in labels:
            lbl_lower = lbl.lower()
            if lbl_lower not in surfaces:
                surfaces.append(lbl_lower)
        tokens = _split_local_name(uri)
        if tokens:
            joined = " ".join(tokens)
            if joined not in surfaces:
                surfaces.append(joined)
        display_label = labels[0] if labels else (" ".join(tokens) if tokens else uri)
        concepts.append({
            "uri": uri,
            "kind": kind,
            "label": display_label,
            "surfaces": surfaces,
            "related": [],
        })


def pick_convertible_pair(from_candidates, to_candidates, are_compatible):
    """Best-ranked compatible (from, to) pair from rank-ordered URI lists.

    Minimizes the summed candidate ranks (ties favor the from side);
    returns ``None`` when no pair is compatible.
    """
    best = None
    for i, a in enumerate(from_candidates):
        for j, b in enumerate(to_candidates):
            if best is not None and i + j >= best[0]:
                continue
            if are_compatible(a, b):
                best = (i + j, a, b)
    return (best[1], best[2]) if best else None


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
    publication: PublicationStore
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
        ontology_sources: list["OntologySource"] | None = None,
        qudt_graph: Graph | None = None,
        qudt_converter: QUDTUnitConverter | None = None,
        materialization_executor: object | None = None,
        recreate: bool = False,
    ):
        if not logging.getLogger().handlers:
            from acquirium.internals._log import configure_logging
            configure_logging()
        start = perf_counter()
        logger.debug(
            "Manager.__init__: backend=%s data_dir=%s recreate=%s",
            timeseries_backend, data_dir, recreate,
        )

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
        with timed_debug(logger, "Manager.__init__: timeseries backend setup (%s)", _backend):
            if _backend == "duckdb":
                _effective_dsn = None
                _effective_duckdb_path = duckdb_path or (base / "timeseries.duckdb")
                timescale: TimeseriesStore = create_timeseries_store(
                    "duckdb", duckdb_path=_effective_duckdb_path, recreate=recreate
                )
                from acquirium.Storage.publication.duckdb import PublicationDuckDB
                from acquirium.Storage.materialization.duckdb import MaterializationDuckDB

                publication: PublicationStore = PublicationDuckDB(timescale)
                materialization = MaterializationDuckDB(timescale)
            else:
                _effective_dsn = pg_dsn or os.getenv("PG_DSN")
                if not _effective_dsn:
                    raise ValueError("PG_DSN required for timescale backend. Set pg_dsn or PG_DSN env var.")
                timescale = create_timeseries_store(
                    "timescale", pg_dsn=_effective_dsn, recreate=recreate
                )
                from acquirium.Storage.publication.postgres import PublicationPostgres
                from acquirium.Storage.materialization.postgres import MaterializationPostgres

                publication = PublicationPostgres(_effective_dsn)
                materialization = MaterializationPostgres(_effective_dsn)

        # Caller-supplied converter or graph wins; otherwise the converter
        # is built lazily in _ensure_qudt_converter from the QUDT unit
        # named graph that ontoenv loads at startup — no separate file
        # read or eager initialization needed.
        converter = qudt_converter
        if converter is None and qudt_graph is not None:
            converter = QUDTUnitConverter(qudt_graph)

        with timed_debug(logger, "Manager.__init__: OxigraphGraphStore setup"):
            graph = OxigraphGraphStore(
                store_path=graph_path,
                env_root=ontoenv_root,
                extra_ontology_sources=ontology_sources,
            )

        self.timescale = timescale
        self.graph_store = graph
        self.publication = publication
        self.materialization = materialization
        from acquirium.Materialization.executor import LocalExecutorPool
        from acquirium.Materialization.epoch_reconciler import TopologyEpochReconciler
        # Production injects its fixed Ray pool. The local implementation is a
        # service-free harness for direct library use and unit tests only.
        self.materialization_executor = (
            materialization_executor if materialization_executor is not None else LocalExecutorPool()
        )
        if _backend == "duckdb":
            from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
            epoch_materialization = TopologyEpochDuckDB(
                timescale,
                state_revision_resolver=materialization.active_state_revisions,
                query_resolver=self.resolve_text,
            )
        else:
            from acquirium.Storage.materialization.epoch_postgres import TopologyEpochPostgres
            epoch_materialization = TopologyEpochPostgres(
                _effective_dsn,
                state_revision_resolver=materialization.active_state_revisions,
                query_resolver=self.resolve_text,
            )
        self.epoch_materialization = epoch_materialization
        self.epoch_reconciler = TopologyEpochReconciler(epoch_materialization, graph, self.materialization_executor)
        from acquirium.Materialization.service_runtime import ServiceSupervisor
        from acquirium.Materialization.effect_worker import EffectDispatcher
        from acquirium.Server.effect_worker import deliver_effect
        self.service_supervisor = ServiceSupervisor(materialization, self.service_snapshot)
        self.effect_dispatcher = EffectDispatcher(materialization, deliver_effect)
        # Published graph revision seen by the last stream-ref resync. It gates
        # the expensive rebuild in _registered_value_kind so repeated writes to
        # an unregistered ref do not each trigger a full inferred-graph rebuild.
        self._refs_synced_revision = None
        self.qudt_converter = converter
        self.backend = _backend

        self.data_dir = base
        from acquirium.Storage.artifacts import FilesystemArtifactStore
        self.materialization_artifacts = FilesystemArtifactStore(
            self.data_dir / "materialization-artifacts"
        )
        def _epoch_artifact(revision_id: str) -> bytes:
            revision = materialization.state_revision(revision_id)
            return self.materialization_artifacts.get(revision.artifact.digest)
        self.epoch_reconciler._artifact_loader = _epoch_artifact

        _emb_model = os.getenv("ACQUIRIUM_EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")

        # Persist the downloaded embedding model under the data dir so it
        # survives OS temp-dir purges (fastembed defaults to $TMPDIR). A
        # pre-warmed cache (e.g. baked into the Docker image) can be pointed
        # at with FASTEMBED_CACHE_PATH.
        _model_cache = Path(
            os.getenv("FASTEMBED_CACHE_PATH") or base / "embedding_cache" / "models"
        )

        # Dual matchers: graph (class/predicate) and QUDT (unit/quantity_kind)
        self._graph_matcher = EmbeddingMatcher(
            model_name=_emb_model,
            cache_dir=base / "embedding_cache" / "graph",
            model_cache_dir=_model_cache,
        )
        self._qudt_matcher = EmbeddingMatcher(
            model_name=_emb_model,
            cache_dir=base / "embedding_cache" / "qudt",
            model_cache_dir=_model_cache,
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
        self._embedding_status_lock = Lock()
        self._embedding_status: dict[str, dict[str, Any]] = {
            "graph": {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
            "qudt":  {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
        }

        # Embedding corpus is the static ontoenv vocabularies; build once.
        with timed_debug(logger, "Manager.__init__: _build_embedding_indexes"):
            self._build_embedding_indexes()
        logger.debug("Manager.__init__: complete in %.2fs", perf_counter() - start)

    
    @classmethod
    def from_env(cls, *, materialization_executor: object) -> Manager:
        _backend = os.getenv("ACQUIRIUM_TIMESERIES_BACKEND", "duckdb").lower()
        _data_dir = os.getenv("ACQUIRIUM_DATA_DIR")
        # Ontology sources are read directly from acquirium.toml —
        # ACQUIRIUM_CONFIG points at it. Keeps the environment-variable
        # surface small.
        from acquirium.Server.config import load_ontology_config

        ont_cfg = load_ontology_config()
        return cls(
            data_dir=_data_dir,
            pg_dsn=os.getenv("PG_DSN"),
            duckdb_path=os.getenv("ACQUIRIUM_DUCKDB_PATH"),
            timeseries_backend=_backend,
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            ontology_sources=list(ont_cfg.sources) or None,
            materialization_executor=materialization_executor,
            recreate=os.getenv("ACQUIRIUM_RECREATE", "false").lower() == "true",
        )

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
        # Stream registrations may be completed by ontology rules, so query the
        # fresh inferred deployment graph with shapes rather than only asserted
        # source data. The graph store coalesces concurrent rebuilds.
        with timed_debug(logger, "_sync_stream_refs_from_graph: SPARQL"):
            res = self.graph_store.sparql_query(
                q,
                include_dependencies=True,
                wait_for_fresh=True,
            )
        rows = res.get("rows", [])
        logger.debug("_sync_stream_refs_from_graph: %d candidate rows", len(rows))
        # Validate every row first, then hand the whole batch to the store in one
        # statement. Upserting row by row costs a round trip each, which grows with
        # the graph: ~3.6s for 1000 refs versus ~16ms batched.
        refs: list[tuple[str | None, str, str, URIRef, str]] = []
        for point_uri, ref_node, source_id, ref_name, value_kind in rows:
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
                refs.append((
                    point,
                    sid,
                    rn,
                    actual,
                    normalize_value_kind(
                        str(value_kind).strip('"') if value_kind is not None else None
                    ),
                ))
            except Exception:
                logger.warning("Failed to ensure stream ref %s / %s → %s", point_uri, source_id, ref_name, exc_info=True)
                raise
        count = len(self.timescale.ensure_stream_refs(refs)) if refs else 0
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
          FILTER NOT EXISTS {{ ?uri (<{RDFS.subClassOf}>)* <{WATR.Process}> . }}
          FILTER(!STRSTARTS(STR(?uri), "{WATR}Process"))
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
        # Constrained process space: the NAWI process taxonomy plus whatever
        # the loaded model actually uses as a process (self-grounding, like
        # substances). Its own kind so process filters never rank equipment
        # classes ("reverse osmosis" must hit Process-ReverseOsmosis, not
        # ReverseOsmosisMembrane).
        process_where = f"""
          {{ ?uri (<{RDFS.subClassOf}>)* <{WATR.Process}> . }}
          UNION {{ ?x <{WATR.hasProcess}> ?uri . }}
          UNION {{ ?uri a <{WATR.Class}> . FILTER(STRSTARTS(STR(?uri), "{WATR}Process")) }}
        """

        extractions: list[tuple[str, str, str]] = [
            ("class", class_where, label_block_basic),
            ("predicate", pred_where, label_block_basic),
            ("substance", substance_where, label_block_basic),
            ("process", process_where, label_block_basic),
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
                with timed_debug(logger, "extract %s concepts (SPARQL)", kind):
                    rows = list(graph.query(query))
                logger.debug("extract %s: %d raw rows", kind, len(rows))
                _aggregate_uri_label_rows(rows, seen, kind, concepts)
            except Exception:
                logger.warning("Failed to extract %s concepts", kind, exc_info=True)

        logger.debug("_extract_concepts_for_embedding: %d total concepts", len(concepts))
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
        from acquirium._ontologies import (
            WATER_IRI, S223_IRI, QUDT_UNIT_IRI, QUDT_QK_IRI,
        )

        self._update_embedding_status("graph", state="building")
        t0 = perf_counter()
        try:
            with timed_debug(logger, "graph embedding: merge water+s223 named graphs"):
                merged = Graph()
                for iri in (WATER_IRI, S223_IRI):
                    for triple in self.graph_store.named_graph(iri):
                        merged.add(triple)
            logger.debug("graph embedding: merged %d triples", len(merged))
            concepts = self._extract_concepts_for_embedding(merged)
            if concepts:
                with timed_debug(logger, "graph embedding: build_index n=%d", len(concepts)):
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
            with timed_debug(logger, "qudt embedding: extract Unit concepts"):
                qc += QUDTStore.extract_concepts(
                    self.graph_store.named_graph(QUDT_UNIT_IRI), str(QUDT.Unit)
                )
            with timed_debug(logger, "qudt embedding: extract QuantityKind concepts"):
                qc += QUDTStore.extract_concepts(
                    self.graph_store.named_graph(QUDT_QK_IRI),
                    str(QUDT.QuantityKind),
                )
            logger.debug("qudt embedding: %d total concepts", len(qc))
            if qc:
                with timed_debug(logger, "qudt embedding: build_index n=%d", len(qc)):
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

    def graph_version(self) -> int:
        """Return the store-owned source-data generation."""
        return int(self.graph_store.graph_status()["source_version"])

    def graph_status(self) -> dict[str, int | bool]:
        """Return source and derived-query generation status."""
        return self.graph_store.graph_status()

    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = True,
        *,
        source_id: str,
    ) -> None:
        """
        Insert RDF graph into one explicitly owned deployment data graph.

        The embedding index is refreshed synchronously before returning, so
        once this call completes the just-inserted concepts are resolvable.
        The refresh is incremental (only new concepts are embedded) unless
        ``replace=True``, which triggers a full rebuild.

        Args:
            :param rdf_graph: An `xml.sax.xmlreader.InputSource`, file-like object,
            `pathlib.Path` like object, or string. In the case of a string the string
            is the location of the source.
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the selected graph. If False, appends to it.
            source_id: Data-graph owner. Use ``"plant"`` for the shared plant
                model, or a component's stable source ID.
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
            with timed_debug(logger, "insert_graph format=%s replace=%s", format, replace):
                graph_uri = self.graph_store.source_graph_uri(source_id)
                self.graph_store.insert_graph(
                    rdf_graph,
                    format=format,
                    replace=replace,
                    graph_uri=graph_uri,
                )
            logging.info("acquirium: inserted graph into store")
            with timed_debug(logger, "insert_graph: _sync_stream_refs_from_graph"):
                self._sync_stream_refs_from_graph()
            self._record_materialization_graph_revision()
            # Embedding corpus is the static ontoenv vocabularies, not
            # inserted data — no per-insert reindex. Stream-reference sync
            # below requires a fresh inferred view; concurrent callers share
            # its single-flight rebuild.
        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise

    def _ensure_current_epoch(self) -> str | None:
        """Ensure the desired epoch for the published graph; None before first publish."""
        graph_revision = int(self.graph_store.graph_status()["published_version"])
        if graph_revision < 0:
            return None
        return self.epoch_reconciler.ensure_graph_epoch(
            graph_revision, self.graph_store.published_query_digest()
        )

    def _record_materialization_graph_revision(self) -> None:
        """Durably create the desired epoch for a completed query-graph publication."""
        graph_revision = int(self.graph_store.graph_status()["published_version"])
        if graph_revision < 0:
            return
        self._ensure_current_epoch()
        self.notify_service_changes({}, graph_revision=graph_revision)

    def recover_materialization_state(self) -> None:
        """Re-derive control work from durable graph and publication state."""
        self._record_materialization_graph_revision()
        self.epoch_materialization.plan_data_changes()

    def materialization_safety_scan(self) -> None:
        """Continuously recover publication work missed by an interrupted request."""
        self.epoch_materialization.plan_data_changes()
        self.service_safety_scan()

    def deploy_transformation(self, definition) -> dict[str, Any]:
        """Validate and deploy one immutable transformation definition."""
        from acquirium.Materialization.api import StatefulTransformation, Transformation
        from acquirium.Materialization.worker import load_entrypoint
        target = load_entrypoint(definition.entrypoint, definition.source_digest)
        if (not isinstance(target, type)
                or not issubclass(target, (Transformation, StatefulTransformation))):
            raise ValueError("transformation entrypoints must be Transformation classes")
        definition_id = self.epoch_materialization.register_definition(definition)
        generation = self.epoch_materialization.deploy_definition(
            definition.name, definition_id, self.graph_store
        )
        return {"name": definition.name, "definition_id": definition_id,
                "generation": generation, "epoch_id": self._ensure_current_epoch(),
                "status": "deploying"}

    def remove_transformation(self, name: str) -> dict[str, Any]:
        self.epoch_materialization.remove_deployment(name, self.graph_store)
        return {"name": name, "epoch_id": self._ensure_current_epoch(), "status": "removing"}

    def register_service(self, definition) -> dict[str, Any]:
        """Register an immutable service package without granting stream ownership."""
        if definition.kind != "service":
            raise ValueError("service registration requires a service definition")
        if definition.outputs is not None:
            raise ValueError("services cannot declare materialized stream inputs or outputs")
        from acquirium.Materialization.api import Service
        from acquirium.Materialization.worker import load_entrypoint
        target = load_entrypoint(definition.entrypoint, definition.source_digest)
        if not isinstance(target, type) or not issubclass(target, Service):
            raise ValueError("service entrypoints must be Service classes")
        definition_id = self.materialization.register_definition(definition)
        service = self.materialization.register_service(definition.name, definition_id)
        return {"name": service.name, "definition_id": service.definition_id,
                "status": service.status, "health": service.health}

    def promote_state_revision(self, revision_id: str, *, policy: str = "prospective",
                               effective_from: datetime | None = None):
        revision = self.materialization.promote_state_revision(
            revision_id, policy=policy, effective_from=effective_from
        )
        self._ensure_current_epoch()
        return revision

    def notify_service_changes(self, versions: dict[str, int], *, graph_revision: int | None = None) -> None:
        """Persist a latest-state wake-up for every running service, coalescing bursts.

        Hints are an optimization: the startup safety scan recreates any that
        are missed, so a service deleted mid-loop is skipped rather than
        allowed to fail the publish that triggered the notification.
        """
        from acquirium.Materialization.services import ChangeHint
        current = self.materialization.all_stream_versions()
        versions = {
            key: max(int(current.get(key, value)), int(value))
            for key, value in {**current, **versions}.items()
        }
        now = datetime.now(timezone.utc)
        for service in self.materialization.services(status="running"):
            self._coalesce_hint_ignore_removed(ChangeHint(
                service.name, str(uuid.uuid4()), dict(versions), graph_revision, now,
            ))

    def service_safety_scan(self) -> None:
        """Recover missed process-local notifications from canonical stream heads."""
        from acquirium.Materialization.services import ChangeHint
        graph_revision = int(self.graph_store.graph_status()["published_version"])
        graph_revision = graph_revision if graph_revision >= 0 else None
        versions = self.materialization.all_stream_versions()
        now = datetime.now(timezone.utc)
        for name in self.materialization.services_needing_hint(versions, graph_revision):
            self._coalesce_hint_ignore_removed(ChangeHint(name, str(uuid.uuid4()), versions, graph_revision, now))

    def _coalesce_hint_ignore_removed(self, hint) -> None:
        """Write one coalesced hint; a concurrently removed service is not an error."""
        try:
            self.materialization.coalesce_service_hint(hint)
        except KeyError:
            pass

    def start_service(self, name: str):
        service = self.service_supervisor.start(name)
        self.service_safety_scan()
        return service

    def run_service_once(self) -> bool:
        return self.service_supervisor.run_next()

    def run_effect_once(self, owner: str = "manager") -> bool:
        return self.effect_dispatcher.deliver_once(owner)

    def service_snapshot(self, refs: tuple[str, ...], *, since: "datetime | None" = None):
        """Return authoritative rows per stream, proven consistent by before/after head vectors.

        Reads the latest row per stream by default, or every live row at or
        after ``since`` when a window/history is requested.
        """
        from acquirium.Materialization.services import ServiceSnapshot, snapshot_token
        for _ in range(3):
            before, inputs = self.materialization.service_input_snapshot(refs, since=since)
            after = self.materialization.stream_versions(refs)
            if before == after:
                graph = int(self.graph_store.graph_status()["published_version"])
                return ServiceSnapshot(snapshot_token(after, graph if graph >= 0 else None, since=since), after,
                                       graph if graph >= 0 else None, inputs)
        raise RuntimeError("canonical inputs changed continuously while taking service snapshot")

    def run_materialization_once(self, owner: str = "manager") -> bool:
        """Execute one topology-epoch transition, if any is pending."""
        return self.epoch_reconciler.run_once(owner)

    def collect_materialization_artifacts(self, *, older_than_seconds: float = 86400) -> int:
        """Collect aged local blobs that no durable state revision references."""
        return self.materialization_artifacts.sweep_orphans(
            self.materialization.artifact_digests(), older_than_seconds=older_than_seconds
        )

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
        logger.debug(
            "timeseries_batch uri=%s storage_key=%s start=%s end=%s limit=%s",
            uri, storage_key, start, end, limit,
        )
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
        self.graph_store.insert_graph(
            g,
            format="turtle",
            replace=False,
            graph_uri=self.graph_store.source_graph_uri(source_id),
        )
        return source_id

    def publish(
        self, mutations: "pa.Table", *, publication_id: str | None = None
    ) -> PublicationReceipt:
        """Atomically publish canonical mutations with stable retry identity.

        Every write to canonical timeseries storage -- driver ingest,
        materialization output commits, explicit deletes -- goes through this
        one path so stream versions and change-range manifests stay
        authoritative. Assigns a fresh uuid4 ``publication_id`` when the caller
        doesn't supply a stable one; a caller reusing the same id across a
        retried request gets the idempotent-replay path in
        ``PublicationStore.publish`` for free.
        """
        pub_id = publication_id or str(uuid.uuid4())
        receipt = self.publication.publish(PublicationRequest(pub_id, mutations))
        return self._after_canonical_publish(receipt)

    def _after_canonical_publish(self, receipt: PublicationReceipt) -> PublicationReceipt:
        """Derive materialization work and service hints from a committed publication."""
        self.epoch_materialization.plan_data_changes()
        self.notify_service_changes(dict(receipt.versions))
        return receipt

    @staticmethod
    def _empty_receipt(publication_id: str | None) -> PublicationReceipt:
        return PublicationReceipt(
            publication_id=publication_id or str(uuid.uuid4()),
            payload_hash="", row_count=0, versions={},
        )

    @staticmethod
    def _mutation_table(df: "Any", *, operation: str = "upsert") -> "pa.Table":
        """Split ``value``/``value_kind`` columns and tag every row with
        *operation*, producing a table matching MUTATION_SCHEMA's column set."""
        import polars as pl
        from acquirium.Storage.values import prepare_value_columns

        split = prepare_value_columns(df)
        return (
            split.with_columns(pl.lit(operation).alias("operation"))
            .select(["operation", "ref_uri", "ts", "numeric_value", "text_value"])
            .to_arrow()
        )

    def insert_timeseries(
        self,
        *,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        point_uri: str | None = None,
        replace: bool = False,
        publication_id: str | None = None,
    ) -> PublicationReceipt:
        import polars as pl
        from acquirium.Storage.values import typed_value_series

        ref_uri = str(compute_ref_uri(source_id, ref_name))
        value_kind = self._registered_value_kind(ref_uri)
        logger.debug(
            "insert_timeseries source=%s ref_name=%s rows=%d kind=%s replace=%s",
            source_id, ref_name, len(rows), value_kind, replace,
        )
        n = len(rows)
        if n == 0 and not replace:
            return self._empty_receipt(publication_id)
        df = pl.DataFrame(
            {
                "ref_uri": pl.Series("ref_uri", [ref_uri] * n, dtype=pl.Utf8),
                "ts": pl.Series("ts", [ts for ts, _ in rows], dtype=pl.Datetime("us", "UTC")),
                "value": typed_value_series([v for _, v in rows]),
                "value_kind": pl.Series("value_kind", [value_kind] * n, dtype=pl.Utf8),
            }
        )
        upserts = pl.from_arrow(self._mutation_table(df))

        if not replace:
            return self.publish(upserts.to_arrow(), publication_id=publication_id)

        receipt = self.publication.replace(
            PublicationRequest(
                publication_id or str(uuid.uuid4()),
                upserts.to_arrow(),
            ),
            ref_uri,
        )
        return self._after_canonical_publish(receipt)

    def insert_timeseries_batch(
        self,
        source_id: str,
        streams: dict[str, list[tuple[datetime, Any]]],
        *,
        publication_id: str | None = None,
    ) -> PublicationReceipt:
        """Publish multiple source-local streams as one atomic mutation set."""
        import polars as pl

        ref_uris: list[str] = []
        timestamps: list[datetime] = []
        values: list[Any] = []
        value_kinds: list[str] = []
        with timed_debug(logger, "insert_timeseries_batch source=%s streams=%d", source_id, len(streams)):
            for ref_name, stream_rows in streams.items():
                ref_uri = str(compute_ref_uri(source_id, ref_name))
                value_kind = self._registered_value_kind(ref_uri)
                for ts, value in stream_rows:
                    ref_uris.append(ref_uri)
                    timestamps.append(ts)
                    values.append(value)
                    value_kinds.append(value_kind)
        logger.debug(
            "insert_timeseries_batch source=%s built %d total rows across %d streams",
            source_id, len(ref_uris), len(streams),
        )
        if not ref_uris:
            return self._empty_receipt(publication_id)

        from acquirium.Storage.values import typed_value_series

        df = pl.DataFrame(
            {
                "ref_uri": pl.Series("ref_uri", ref_uris, dtype=pl.Utf8),
                "ts": pl.Series("ts", timestamps, dtype=pl.Datetime("us", "UTC")),
                "value": typed_value_series(values),
                "value_kind": pl.Series("value_kind", value_kinds, dtype=pl.Utf8),
            }
        )
        return self.publish(self._mutation_table(df), publication_id=publication_id)

    def insert_timeseries_arrow(
        self, source_id: str, table: "pa.Table", *, publication_id: str | None = None
    ) -> PublicationReceipt:
        """Publish a melted (ts, ref_name, value) Arrow table as one atomic mutation set."""
        import polars as pl

        logger.debug("insert_timeseries_arrow source=%s arrow_rows=%d", source_id, len(table))
        if len(table) == 0:
            return self._empty_receipt(publication_id)
        df = pl.from_arrow(table)
        stream_count = df["ref_name"].n_unique()
        logger.info(
            "acquirium: insert_timeseries_arrow received %d row(s) across %d stream(s) for source_id=%s",
            len(df),
            stream_count,
            source_id,
        )
        ref_uri_map: dict[str, str] = {}
        value_kind_map: dict[str, str] = {}
        for name in df["ref_name"].unique().to_list():
            ref_uri = str(compute_ref_uri(source_id, name))
            ref_uri_map[name] = ref_uri
            value_kind_map[name] = self._registered_value_kind(ref_uri)
        logger.debug("insert_timeseries_arrow source=%s unique_streams=%d", source_id, len(ref_uri_map))
        df = (
            df.with_columns([
                pl.col("ref_name").replace(ref_uri_map).alias("ref_uri"),
                pl.col("ref_name").replace(value_kind_map).alias("value_kind"),
            ])
            .drop("ref_name")
            .select(["ref_uri", "ts", "value", "value_kind"])
        )
        receipt = self.publish(self._mutation_table(df), publication_id=publication_id)
        logger.info(
            "acquirium: insert_timeseries_arrow wrote %d row(s) for source_id=%s",
            receipt.row_count,
            source_id,
        )
        return receipt

    def delete_timeseries(
        self,
        ref_uri: str,
        *,
        timestamps: list[datetime] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        publication_id: str | None = None,
    ) -> PublicationReceipt:
        """Publish tombstones for explicit timestamps, or every timestamp in
        ``[start, end]`` (resolved by reading current live keys -- deletion
        is itself an explicit mutation set, not a range predicate stored in
        the manifest). Only timestamps are read back, not values."""
        import polars as pl

        if timestamps is None:
            timestamps = self.timescale.timestamps(ref_uri, start=start, end=end)
        if not timestamps:
            return self._empty_receipt(publication_id)
        n = len(timestamps)
        mutations = pl.DataFrame(
            {
                "operation": pl.Series(["delete"] * n, dtype=pl.Utf8),
                "ref_uri": pl.Series([ref_uri] * n, dtype=pl.Utf8),
                "ts": pl.Series(timestamps, dtype=pl.Datetime("us", "UTC")),
                "numeric_value": pl.Series([None] * n, dtype=pl.Float64),
                "text_value": pl.Series([None] * n, dtype=pl.Utf8),
            }
        ).to_arrow()
        return self.publish(mutations, publication_id=publication_id)

    def _registered_value_kind(self, ref_uri: str) -> str:
        value_kind = self.timescale.stream_value_kind(ref_uri)
        # Graph writes and Arrow ingestion may be issued back-to-back by a
        # client. Ensure the derived stream registry has observed the graph
        # write before rejecting the first data batch. The resync rebuilds the
        # inferred graph, so only run it when the graph actually advanced since
        # the last sync; otherwise repeated writes to an unregistered ref would
        # each pay the full rebuild cost.
        if value_kind is None:
            published = int(self.graph_store.graph_status()["published_version"])
            if published != self._refs_synced_revision:
                self._sync_stream_refs_from_graph()
                self._refs_synced_revision = published
            value_kind = self.timescale.stream_value_kind(ref_uri)
        if value_kind is None:
            raise ValueError(f"stream {ref_uri} is not registered")
        return normalize_value_kind(value_kind)

    def insert_log(self, log_message: LogEntry):
        logger.debug("insert_log point_uri=%s ts=%s", log_message.point_uri, log_message.timestamp)
        self.timescale.insert_log(log_message)
        G = Graph()
        log_uri = URIRef(f"{str(log_message.point_uri)}_log")
        G.add((URIRef(log_message.point_uri), HAS_LOG, log_uri))
        G.add((log_uri, RDF.type, LOGBOOK))
        # Log writes still change the store-owned graph version, but no
        # manager-local counter needs to be maintained.
        self.graph_store.insert_graph(
            G,
            format="turtle",
            replace=False,
            graph_uri=self.graph_store.acquirium_graph_uri,
        )


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
        result = self.graph_store.sparql_update(
            q,
            graph_uri=self.graph_store.acquirium_graph_uri,
        )
        logger.info("Deleted all log references for point %s from graph", point_uri)
        return True

    def sparql_update(self, update: str, source_id: str) -> dict[str, Any]:
        """Execute a SPARQL UPDATE against one explicitly owned data graph.

        It lets a component remove exactly the triples it registered without
        granting updates to ontology graphs.
        """
        graph_uri = self.graph_store.source_graph_uri(source_id)
        result = self.graph_store.sparql_update(update, graph_uri=graph_uri)
        self._sync_stream_refs_from_graph()
        self._record_materialization_graph_revision()
        return result

    def validate_graph(self) -> dict[str, str | bool]:
        """Validate all registered deployment data against ontology shapes."""
        return self.graph_store.validate()

    def sparql_dict(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
    ) -> dict[str, Any]:
        """
        Execute a SPARQL query against the graph store and return results in dict format.

        Args:
            query: The SPARQL query string.
            include_dependencies: Whether to include imported
                ontology/shape triples included.
            wait_for_fresh: Wait for the latest graph mutation to be inferred.
                The default returns the last complete published graph while a
                coalesced rebuild runs in the background.

        Returns:
            A dictionary containing the query results.
            {"cols": [...], "rows": [...]}
        """
        logger.debug("sparql_dict dependencies=%s len=%d", include_dependencies, len(query))
        return self.graph_store.sparql_query(
            query,
            include_dependencies=include_dependencies,
            wait_for_fresh=wait_for_fresh,
        )

    def sparql_json(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
    ) -> bytes | None:
        """Return native SPARQL JSON for SELECT queries when available."""
        logger.debug("sparql_json dependencies=%s len=%d", include_dependencies, len(query))
        return self.graph_store.sparql_query_json(
            query,
            include_dependencies=include_dependencies,
            wait_for_fresh=wait_for_fresh,
        )

    def sparql_serialized(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
        results_format: ox.QueryResultsFormat,
        graph_format: ox.RdfFormat,
    ) -> tuple[bytes, str]:
        """Return a SPARQL Protocol response body from the derived query view."""
        return self.graph_store.sparql_query_serialized(
            query,
            include_dependencies=include_dependencies,
            wait_for_fresh=wait_for_fresh,
            results_format=results_format,
            graph_format=graph_format,
        )

    def namespace_manager(self) -> NamespaceManager :
        """
        Get the RDFLib NamespaceManager from the graph store.

        Returns:
            An RDFLib NamespaceManager instance.
        """
        return self.graph_store.namespace_manager()

    def embedding_status(self) -> dict[str, Any]:
        """Return the current state of each embedding index."""
        with self._embedding_status_lock:
            return {k: dict(v) for k, v in self._embedding_status.items()}

    # -------------------- Unit conversion --------------------

    def _ensure_qudt_converter(self) -> QUDTUnitConverter:
        """Lazily initialize the QUDT converter from the in-store QUDT graph.

        Pulling the QUDT unit graph through ontoenv (rather than re-reading
        the bundled TTL) means a user-supplied override at the QUDT unit
        IRI in ``[ontologies] sources`` is honored automatically: the
        converter sees whatever graph ontoenv currently has registered
        at that IRI, bundled or replaced.
        """
        if self.qudt_converter is not None:
            return self.qudt_converter
        from acquirium._ontologies import QUDT_UNIT_IRI

        qudt_graph = self.graph_store.named_graph(QUDT_UNIT_IRI)
        self.qudt_converter = QUDTUnitConverter(qudt_graph)
        logger.info(
            "Lazily loaded QUDTUnitConverter from ontoenv graph %s (%d triples)",
            QUDT_UNIT_IRI, len(qudt_graph),
        )
        return self.qudt_converter

    def resolve_conversion_info(
        self,
        from_unit: str,
        to_unit: str,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> dict[str, Any]:
        """Resolve a from/to unit pair to a *convertible* match plus factors.

        Each side may be a URI (pinned) or free text — text goes through the
        full resolver cascade, and the best-ranked candidate pair that is
        actually compatible for conversion wins. This closes the gap between
        the lenient text resolver (which may top-rank a non-convertible
        near-match) and the strict converter (which rejects anything but
        exact symbols/labels).

        Example::

            resolve_conversion_info("mg/l", "grams per liter")
            # -> {"from": {...MilliGM-PER-L...}, "to": {...GM-PER-L...},
            #     "factors": {"from_multiplier": ..., "to_uri": ..., ...}}
        """
        converter = self._ensure_qudt_converter()

        def candidates(identifier: str, side: str) -> list[str]:
            s = str(identifier)
            if s.startswith(("http://", "https://", "urn:")):
                return [s]
            uris = [m["uri"] for m in self.resolve_text(
                s, kind="unit", top_k=top_k, min_score=min_score)]
            if not uris:
                raise ValueError(f"could not resolve {side} unit {identifier!r}")
            return uris

        from_cands = candidates(from_unit, "from")
        to_cands = candidates(to_unit, "to")

        def compat(a: str, b: str) -> bool:
            try:
                return converter.are_compatible(a, b)
            except Exception:
                return False

        pair = pick_convertible_pair(from_cands, to_cands, compat)
        if pair is None:
            raise ValueError(
                f"no convertible pair among the matches for {from_unit!r} -> {to_unit!r} "
                f"(from candidates: {from_cands}, to candidates: {to_cands})"
            )
        return {
            "from": self.resolve_unit_info(pair[0]),
            "to": self.resolve_unit_info(pair[1]),
            "factors": self.get_conversion_factors(pair[0], pair[1]),
        }

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
        """Release every owned resource; one component failing must not leak the rest.

        Each step is isolated so a component that raises during shutdown (for
        example a Ray executor killed after its runtime is gone) cannot skip
        the stores that still need closing.
        """
        logger.debug("Manager.close: shutting down")
        steps = [
            ("materialization executor", getattr(self, "materialization_executor", None)),
            ("service supervisor", getattr(self, "service_supervisor", None)),
            ("publication store", self.publication),
            ("epoch materialization store", getattr(self, "epoch_materialization", None)),
            ("materialization store", self.materialization),
            ("timeseries store", self.timescale),
            ("graph store", self.graph_store),
        ]
        for label, component in steps:
            close = getattr(component, "close", None)
            if not callable(close):
                continue
            try:
                close()
            except Exception:
                logger.exception("Manager.close: failed to close %s", label)
        logger.debug("Manager.close: done")
