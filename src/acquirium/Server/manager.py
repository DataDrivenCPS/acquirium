from __future__ import annotations

import uuid
from functools import wraps
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import os
import logging
from threading import Lock
import pyoxigraph as ox
from time import perf_counter
from rdflib import Graph, URIRef, Literal, RDF, RDFS
from rdflib.namespace import NamespaceManager
from acquirium.Storage.graph_store import OxigraphGraphStore
from acquirium.core import Core
from acquirium.apps import Apps
from acquirium.Storage.graph_store import select_values
from acquirium.Storage.values import normalize_value_kind, normalize_value_mode
from acquirium.internals.qudt_units import QUDTUnitConverter
from acquirium.Server.config import OntologySource
from acquirium.internals.models import compute_ref_uri
from acquirium.internals.internals_namespaces import *

from typing import Any
import shutil
from acquirium.TextMatch.embedding_matcher import DEFAULT_MODEL, EmbeddingMatcher
from acquirium.TextMatch.graph_concepts import GraphConcepts
from acquirium.TextMatch.qudt_store import QUDTStore
from acquirium.TextMatch.resolver import ConceptResolver
from acquirium.TextMatch.unit_key import UnitKeyIndex

from acquirium.internals._log import timed_debug

logger = logging.getLogger("acquirium.manager")

DEFAULT_DATA_DIR = Path(".acquirium")
DEFAULT_DB_NAME = "acquirium"
RECREATE_WARNING = (
    "SERVER STARTED WITH recreate=True. Existing Acquirium data will be erased now. "
    "Restarting this server with recreate=True will erase data again."
)


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



def semantic_operation(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self.apps.lock:
            return method(self, *args, **kwargs)
    return locked


class Manager:
    qudt_converter: QUDTUnitConverter | None = None

    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        engine_path: str | Path | None = None,
        engine_workers: int = 2,
        python_pool_size: int | None = None,
        python_max_worker_jobs: int = 10_000,
        python_max_worker_age: float = 1800,
        epoch_requests: int = 128,
        max_pending_requests: int = 10_000,
        graph_path: str | Path | None = None,
        ontoenv_root: str | Path | None = None,
        ontology_sources: list["OntologySource"] | None = None,
        qudt_graph: Graph | None = None,
        qudt_converter: QUDTUnitConverter | None = None,
        recreate: bool = False,
        exact_only: bool = False,
    ):
        if not logging.getLogger().handlers:
            from acquirium.internals._log import configure_logging
            configure_logging()

        base = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if recreate:
            logger.warning(RECREATE_WARNING)
            if base.exists():
                _wipe_dir_contents(base)
                print(f"Deleted data directory contents: {base}")
        base.mkdir(parents=True, exist_ok=True)
        graph_path = Path(graph_path) if graph_path is not None else base / ".oxigraph"
        ontoenv_root = Path(ontoenv_root) if ontoenv_root is not None else base

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

        self.graph_store = graph
        self.qudt_converter = converter
        self._refs_revision = -1
        self._stream_refs = {}
        self._point_refs = {}
        try:
            self.core = Core(
                engine_path or base / "escapement.duckdb",
                workers=engine_workers,
                epoch_requests=epoch_requests,
                max_pending_requests=max_pending_requests,
                python_pool_size=python_pool_size,
                python_max_worker_jobs=python_max_worker_jobs,
                python_max_worker_age=python_max_worker_age,
            )
            self.apps = Apps(self.core, graph, base, converter=self._ensure_qudt_converter)
        except BaseException:
            if hasattr(self, "core"):
                self.core.close(drain=False)
            graph.close()
            raise

        self.data_dir = base

        # exact_only builds the same indexes over the same concepts but
        # never embeds them: resolution keeps working on exact names, units
        # and symbols with no model to download, and nothing is written
        # under embedding_cache/. See EmbeddingMatcher.
        self.exact_only = exact_only

        _emb_model = os.getenv("ACQUIRIUM_EMBEDDING_MODEL", DEFAULT_MODEL)

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
            exact_only=exact_only,
            name="graph",
        )
        self._qudt_matcher = EmbeddingMatcher(
            model_name=_emb_model,
            cache_dir=base / "embedding_cache" / "qudt",
            model_cache_dir=_model_cache,
            exact_only=exact_only,
            name="qudt",
        )

        # Units by typed expression; built with the QUDT index, no model needed.
        self._unit_key_index: UnitKeyIndex | None = None

        # Single normalization façade, sharing the lazily-built converter.
        self._concept_resolver = ConceptResolver(
            graph_matcher=self._graph_matcher,
            qudt_matcher=self._qudt_matcher,
            converter_provider=self._ensure_qudt_converter,
            unit_keys_provider=lambda: self._unit_key_index,
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
        try:
            with timed_debug(logger, "Manager.__init__: _build_embedding_indexes"):
                self._build_embedding_indexes()
        except BaseException:
            self.close()
            raise

    
    @classmethod
    def from_env(cls) -> Manager:
        _data_dir = os.getenv("ACQUIRIUM_DATA_DIR")
        # Ontology sources are read directly from acquirium.toml —
        # ACQUIRIUM_CONFIG points at it. Keeps the environment-variable
        # surface small.
        from acquirium.Server.config import load_ontology_config

        ont_cfg = load_ontology_config()
        return cls(
            data_dir=_data_dir,
            engine_path=os.getenv("ACQUIRIUM_ENGINE_PATH"),
            engine_workers=int(os.getenv("ACQUIRIUM_ENGINE_WORKERS", "2")),
            python_pool_size=(
                int(os.environ["ACQUIRIUM_PYTHON_POOL_SIZE"])
                if "ACQUIRIUM_PYTHON_POOL_SIZE" in os.environ else None
            ),
            python_max_worker_jobs=int(
                os.getenv("ACQUIRIUM_PYTHON_MAX_WORKER_JOBS", "10000")
            ),
            python_max_worker_age=float(
                os.getenv("ACQUIRIUM_PYTHON_MAX_WORKER_AGE", "1800")
            ),
            epoch_requests=int(os.getenv("ACQUIRIUM_EPOCH_REQUESTS", "128")),
            max_pending_requests=int(os.getenv("ACQUIRIUM_MAX_PENDING_REQUESTS", "10000")),
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            ontology_sources=list(ont_cfg.sources) or None,
            recreate=os.getenv("ACQUIRIUM_RECREATE", "false").lower() == "true",
            exact_only=os.getenv("ACQUIRIUM_EXACT_ONLY", "false").lower() == "true",
        )

    def _sync_stream_refs_from_graph(self) -> int:
        """Rebuild the reference lookup from a fresh inferred RDF view."""
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
                sid = str(source_id)
                rn  = str(ref_name)
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
                        str(value_kind) if value_kind is not None else "numeric"
                    ),
                ))
            except Exception:
                logger.warning("Failed to ensure stream ref %s / %s → %s", point_uri, source_id, ref_name, exc_info=True)
                raise
        self._stream_refs = {str(ref): {"source_id": sid, "ref_name": name, "value_kind": kind}
                             for point, sid, name, ref, kind in refs}
        point_refs = {}
        for point, sid, name, ref, kind in refs:
            if point:
                point_refs.setdefault(point, set()).add(str(ref))
        self._point_refs = point_refs
        self._refs_revision = self.graph_store.graph_status()["source_version"]
        return len(self._stream_refs)

    # ----- Embedding index methods -----

    def _extract_concepts_for_embedding(self, iris: list[str]) -> list[dict[str, Any]]:
        """Extract class / predicate / substance / process / role concepts.

        The queries run over an in-memory Oxigraph copy of the union of the
        ontology graphs named by *iris* (water + s223; imports not followed).
        :class:`GraphConcepts` holds the queries and builds the concept dicts.
        Unit / quantity_kind concepts come separately via :class:`QUDTStore`.
        """
        concepts: list[dict[str, Any]] = []
        with timed_debug(logger, "graph embedding: copy %s", iris):
            vocabulary = self.graph_store.copy_graphs(iris)

        for kind in GraphConcepts.KINDS:
            try:
                with timed_debug(logger, "extract %s concepts (SPARQL)", kind):
                    rows = select_values(vocabulary, GraphConcepts.concept_query(kind), iris)
                logger.debug("extract %s: %d raw rows", kind, len(rows))
                concepts += GraphConcepts.extract_concepts(rows, kind)
            except Exception:
                logger.warning("Failed to extract %s concepts", kind, exc_info=True)

        GraphConcepts.add_initialisms(concepts)
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

        graph matcher <- water + s223 vocabularies (class / predicate /
        substance / process / role); qudt matcher <- the QUDT unit + quantity_kind
        vocabularies. Both are queried in Oxigraph by graph IRI (owl:imports
        not followed); no inserted data is embedded.

        Runs the same way in exact-only mode — same concepts, same counts —
        the matchers just index the surfaces without embedding them.
        """
        from acquirium._ontologies import (
            WATER_IRI, S223_IRI, QUDT_UNIT_IRI, QUDT_QK_IRI,
        )

        self._update_embedding_status("graph", state="building")
        t0 = perf_counter()
        try:
            concepts = self._extract_concepts_for_embedding([WATER_IRI, S223_IRI])
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
            for iri, rdf_type in (
                (QUDT_UNIT_IRI, str(QUDT.Unit)),
                (QUDT_QK_IRI, str(QUDT.QuantityKind)),
            ):
                with timed_debug(logger, "qudt embedding: extract %s concepts", rdf_type):
                    rows = self.graph_store.select_in_graphs(
                        QUDTStore.concept_query(rdf_type), [iri]
                    )
                    qc += QUDTStore.extract_concepts(rows, rdf_type)
            logger.debug("qudt embedding: %d total concepts", len(qc))
            self._unit_key_index = UnitKeyIndex([c for c in qc if c["kind"] == "unit"])
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

    @semantic_operation
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
            self._refresh_apps()
            # Embedding corpus is the static ontoenv vocabularies, not
            # inserted data — no per-insert reindex. Stream-reference sync
            # below requires a fresh inferred view; concurrent callers share
            # its single-flight rebuild.
        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise

    def _refresh_apps(self) -> None:
        self.apps.refresh()
        self._sync_stream_refs_from_graph()

    def recover_apps(self) -> None:
        self.apps.recover()
        self._sync_stream_refs_from_graph()

    def deploy_app(self, app):
        result = self.apps.deploy(app)
        self._sync_stream_refs_from_graph()
        return result

    def remove_app(self, name):
        result = self.apps.remove(name)
        self._sync_stream_refs_from_graph()
        return result

    def resolve_storage_keys(self, uris):
        result = {}
        for uri in uris:
            refs = self._point_refs.get(uri, {uri})
            if len(refs) != 1:
                raise ValueError(f"point {uri!r} has multiple references; select an explicit ref_uri")
            result[uri] = next(iter(refs))
        return result

    def timeseries_batch(self, uri, start=None, end=None, limit=None, order="asc",
                         batch_size=10_000, value_mode="default", epoch=None):
        stream = self.resolve_storage_keys([uri])[uri]
        mode = normalize_value_mode(value_mode)
        if mode == "default":
            # A stream reads as its declared kind; derived streams are numeric.
            mode = self._stream_refs.get(stream, {}).get("value_kind", "numeric")
        return self.core.batches(stream, start=start, end=end, limit=limit, order=order,
                                 batch_size=batch_size, epoch=epoch, mode=mode)

    def timeseries_info_batch(self, uris):
        keys = self.resolve_storage_keys(uris)
        info = self.core.info(set(keys.values()))
        return {uri: info[key] for uri, key in keys.items()}

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

    def insert_timeseries(self, *, source_id, ref_name, rows, request_id=None):
        return self.insert_timeseries_batch(source_id, {ref_name: rows}, request_id=request_id)

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None):
        import polars as pl
        from acquirium.core import time_ns
        refs, times, values = [], [], []
        for name, rows in streams.items():
            ref = str(compute_ref_uri(source_id, name))
            for timestamp, value in rows:
                refs.append(ref)
                times.append(time_ns(timestamp))
                values.append(value)
        frame = pl.DataFrame({"ref_uri": pl.Series(refs, dtype=pl.String),
                              "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                              "value": pl.Series(values, dtype=pl.Object)})
        return self._ingest(source_id, frame, request_id)

    def insert_timeseries_arrow(self, source_id, table, *, request_id=None):
        import polars as pl
        frame = pl.from_arrow(table)
        names = frame["ref_name"].unique().to_list()
        if any(not isinstance(name, str) or not name for name in names):
            raise ValueError("ref_name must be a nonempty string")
        refs = {name: str(compute_ref_uri(source_id, name)) for name in names}
        frame = frame.with_columns(pl.col("ref_name").replace_strict(refs).alias("ref_uri"))
        return self._ingest(source_id, frame.drop("ref_name"), request_id)

    def _ingest(self, source_id, frame, request_id):
        """Submit ``ref_uri``/``ts``/``value`` rows, each value stored as its
        stream's declared kind."""
        import polars as pl
        kinds = self._validate_refs(source_id, set(frame["ref_uri"].unique().to_list()))
        frame = frame.with_columns(
            pl.col("ref_uri").replace_strict(kinds, return_dtype=pl.String).alias("value_kind")
        )
        return self.core.ingest(source_id, request_id or str(uuid.uuid4()), frame)

    @semantic_operation
    def _validate_refs(self, source, references):
        """Each registered reference's value kind; unregistered or foreign
        references fail."""
        # Refresh from the graph, never from a second SQL registry.
        if self._refs_revision != self.graph_store.graph_status()["source_version"]:
            self._sync_stream_refs_from_graph()
        kinds = {}
        for reference in references:
            declaration = self._stream_refs.get(reference)
            if declaration is None:
                raise ValueError(f"stream {reference!r} is not registered")
            if declaration["source_id"] != source:
                raise ValueError("stream source mismatch")
            kinds[reference] = declaration["value_kind"]
        return kinds

    def delete_timeseries(self, source_id, ref_name, timestamps, *, request_id=None):
        import polars as pl
        from acquirium.core import time_ns
        reference = str(compute_ref_uri(source_id, ref_name))
        self._validate_refs(source_id, {reference})
        frame = pl.DataFrame({"ref_uri": [reference] * len(timestamps),
                              "ts": pl.Series([time_ns(value) for value in timestamps], dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                              "value": pl.Series([None] * len(timestamps), dtype=pl.Float64),
                              "operation": ["delete"] * len(timestamps)})
        return self.core.ingest(source_id, request_id or str(uuid.uuid4()), frame)

    @semantic_operation
    def sparql_update(self, update: str, source_id: str) -> dict[str, Any]:
        """Execute a SPARQL UPDATE against one explicitly owned data graph.

        It lets a component remove exactly the triples it registered without
        granting updates to ontology graphs.
        """
        graph_uri = self.graph_store.source_graph_uri(source_id)
        result = self.graph_store.sparql_update(update, graph_uri=graph_uri)
        self._sync_stream_refs_from_graph()
        self._refresh_apps()
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
        """Return the current state of each concept index.

        The top-level ``semantic`` key says whether the indexes carry
        embeddings; when ``False`` (exact-only mode) they still build and
        report ``"ready"``, but answer exact matches only.
        """
        with self._embedding_status_lock:
            status: dict[str, Any] = {
                k: dict(v) for k, v in self._embedding_status.items()
            }
        status["semantic"] = not self.exact_only
        return status

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
        example a worker pool killed after its runtime is gone) cannot skip
        the stores that still need closing.
        """
        logger.debug("Manager.close: shutting down")
        steps = [
            ("Escapement", self.core),
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
