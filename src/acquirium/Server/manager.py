from __future__ import annotations

import uuid
from contextlib import nullcontext
from functools import partial, wraps
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import os
import logging
from threading import Lock, RLock
import pyoxigraph as ox
from time import perf_counter
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import NamespaceManager
from acquirium.Storage.graph_store import OxigraphGraphStore
import sys

from acquirium.core import Core, IngestResult
from acquirium.apps import MODULE_DIRECTORY, Apps, output_value_kind
from acquirium.Storage.graph_store import select_values
from acquirium.Storage.graph_registry import METADATA_SOURCE_ID
from acquirium.Storage.ledger import is_postgres, open_ledger
from acquirium.Storage.logbook import Logbook
from acquirium.Storage.values import (
    UNDECLARED_VALUE_KIND, normalize_value_kind, normalize_value_mode,
    stored_value_kind, streams_with_numeric_values, typed_value_series,
)
from acquirium.Experiments import ExperimentStore
from acquirium.ExperimentSamples import EscapementSamples
from acquirium.internals.qudt_units import QUDTUnitConverter
from acquirium.Server.config import OntologySource
from acquirium.internals.models import LogEntry, TimeIntervalModel, compute_stream_uri
from acquirium.internals.internals_namespaces import *

from typing import Any
from urllib.parse import parse_qs, urlsplit
import shutil
from acquirium.TextMatch.embedding_matcher import DEFAULT_MODEL, EmbeddingMatcher
from acquirium.TextMatch.graph_concepts import GraphConcepts
from acquirium.TextMatch.qudt_store import QUDTStore
from acquirium.TextMatch.resolver import ConceptResolver
from acquirium.TextMatch.unit_key import UnitKeyIndex

from acquirium.internals._log import StartupProgress, timed_debug

logger = logging.getLogger("acquirium.manager")

DEFAULT_DATA_DIR = Path(".acquirium")
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
        engine_schema: str | None = None,
        experiments_path: str | Path | None = None,
        engine_workers: int = 2,
        epoch_requests: int = 128,
        max_pending_requests: int = 100_000,
        history_epochs: int | None = None,
        drain_timeout: float | None = 10,
        graph_path: str | Path | None = None,
        ontoenv_root: str | Path | None = None,
        ontology_sources: list["OntologySource"] | None = None,
        qudt_graph: Graph | None = None,
        qudt_converter: QUDTUnitConverter | None = None,
        recreate: bool = False,
        exact_only: bool = False,
        startup: StartupProgress | None = None,
    ):
        if not logging.getLogger().handlers:
            from acquirium.internals._log import configure_logging
            configure_logging()
        phase = startup.phase if startup is not None else partial(timed_debug, logger)

        base = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if not recreate:
            for name in ("timeseries.duckdb", "timeseries.duckdb.wal"):
                if (legacy := base / name).exists():
                    raise ValueError(
                        f"Legacy Acquirium storage at {legacy} cannot be opened by Escapement. "
                        "Choose a fresh data directory and keep the original store for export."
                    )
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

        with phase("Opening the graph store and loading ontologies"):
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
            with phase("Opening the readings store and starting Escapement"):
                artifacts = base / MODULE_DIRECTORY
                artifacts.mkdir(exist_ok=True)
                target = engine_path or base / "escapement-epoch.duckdb"
                if is_postgres(target) and engine_schema is None:
                    options = parse_qs(urlsplit(str(target)).query).get("options", ())
                    if not any("search_path" in option for option in options):
                        engine_schema = "acquirium_epoch"
                self.core = Core(
                    target,
                    schema=engine_schema,
                    workers=engine_workers,
                    epoch_requests=epoch_requests,
                    max_pending=max_pending_requests,
                    history_epochs=history_epochs,
                    module_paths=[*sys.path, str(artifacts.resolve())],
                )
            # App deployment and experiment metadata delivery share one lock:
            # delivery may reconcile apps, while deployment may edit metadata.
            semantic_lock = RLock()
            self.apps = Apps(
                self.core, graph, base, converter=self._ensure_qudt_converter,
                lock=semantic_lock,
            )
            with phase("Opening the experiment and logbook ledger"):
                # Experiment records stay in SQL, beside Escapement: the same
                # PostgreSQL database when Escapement uses one, else DuckDB.
                if experiments_path is None:
                    experiments_path = engine_path if engine_path is not None and is_postgres(engine_path) \
                        else base / "experiments.duckdb"
                self.ledger = open_ledger(experiments_path, samples=EscapementSamples(self.core), recreate=recreate)
                ledger_backend = "postgres" if is_postgres(experiments_path) else "duckdb"
                self.logbook = Logbook(self.ledger, backend=ledger_backend)
                self._logbook_lock = Lock()
                self.experiments = ExperimentStore(
                    self.ledger, graph_store=graph, resolve=self._resolve_experiment_metadata,
                    metadata_lock=semantic_lock, on_metadata_published=self._refresh_experiment_apps,
                )
        except BaseException:
            if hasattr(self, "ledger"):
                self.ledger.close()
            if hasattr(self, "core"):
                self.core.close(drain=False)
            graph.close()
            raise

        self.data_dir = base
        self.drain_timeout = drain_timeout

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

        # Embedding index status tracking
        self._embedding_status_lock = Lock()
        self._embedding_status: dict[str, dict[str, Any]] = {
            "graph": {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
            "qudt":  {"state": "idle", "concepts": 0, "surfaces": 0, "error": None, "last_built": None, "duration_s": None},
        }

        # Embedding corpus is the static ontoenv vocabularies; build once.
        try:
            mode = "exact-only" if exact_only else "with embeddings"
            with phase(f"Preparing text-matching indexes ({mode})"):
                self._build_embedding_indexes()
        except BaseException:
            self.close()
            raise

    
    @classmethod
    def from_env(cls, *, startup: StartupProgress | None = None) -> Manager:
        _data_dir = os.getenv("ACQUIRIUM_DATA_DIR")
        # Ontology sources are read directly from acquirium.toml —
        # ACQUIRIUM_CONFIG points at it. Keeps the environment-variable
        # surface small.
        from acquirium.Server.config import load_active_config, load_ontology_config

        loaded = load_active_config()
        server = loaded.data.get("server", {})
        legacy = (
            server.get("duckdb_path") or server.get("pg_dsn")
            or str(server.get("timeseries_backend", "")).lower() == "timescale"
            or os.getenv("ACQUIRIUM_DUCKDB_PATH")
            or os.getenv("ACQUIRIUM_TIMESERIES_BACKEND", "").lower() == "timescale"
        )
        if legacy:
            raise ValueError(
                "Legacy DuckDB or TimescaleDB storage settings are still configured. "
                "Remove those settings and choose a fresh data directory and storage locations."
            )
        ont_cfg = load_ontology_config(loaded)
        return cls(
            data_dir=_data_dir,
            engine_path=os.getenv("ACQUIRIUM_ENGINE_PATH"),
            engine_schema=os.getenv("ACQUIRIUM_ENGINE_SCHEMA"),
            experiments_path=os.getenv("ACQUIRIUM_EXPERIMENTS_PATH"),
            engine_workers=int(os.getenv("ACQUIRIUM_ENGINE_WORKERS", "2")),
            epoch_requests=int(os.getenv("ACQUIRIUM_EPOCH_REQUESTS", "128")),
            max_pending_requests=int(os.getenv("ACQUIRIUM_MAX_PENDING_REQUESTS", "100000")),
            history_epochs=(int(os.environ["ACQUIRIUM_HISTORY_EPOCHS"])
                            if "ACQUIRIUM_HISTORY_EPOCHS" in os.environ else None),
            drain_timeout=float(os.getenv("ACQUIRIUM_DRAIN_TIMEOUT", "10")),
            graph_path=os.getenv("ACQUIRIUM_GRAPH_PATH"),
            ontoenv_root=os.getenv("ACQUIRIUM_ONTOENV_ROOT"),
            ontology_sources=list(ont_cfg.sources) or None,
            recreate=os.getenv("ACQUIRIUM_RECREATE", "false").lower() == "true",
            exact_only=os.getenv("ACQUIRIUM_EXACT_ONLY", "false").lower() == "true",
            startup=startup,
        )

    def _sync_stream_refs_from_graph(self) -> int:
        """Rebuild the reference lookup from a fresh inferred RDF view."""
        q = f"""
        SELECT ?point ?ref_node ?source_id ?stream_name ?value_kind
        WHERE {{
          ?ref_node <{ACQUIRIUM_SOURCE_ID}> ?source_id .
          ?ref_node <{ACQUIRIUM_REF_NAME}> ?stream_name .
          OPTIONAL {{ ?ref_node <{ACQUIRIUM_VALUE_KIND}> ?declared_kind . }}
          OPTIONAL {{ ?ref_node <{INFERRED_VALUE_KIND}> ?inferred_kind . }}
          BIND(COALESCE(?declared_kind, ?inferred_kind) AS ?value_kind)
          OPTIONAL {{ ?point <{HAS_EXTERNAL_REFERENCE}> ?ref_node . }}
        }}
        """
        # Read the revision first: a change landing during the query leaves
        # it behind, so the next check syncs again.
        revision = self.graph_store.graph_status()["source_version"]
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
        # Built aside and swapped in whole, so readers never see a partial map.
        stream_refs: dict[str, dict[str, str]] = {}
        point_refs: dict[str, set[str]] = {}
        for property_uri, ref_node, source_id, stream_name, value_kind in rows:
            sid, name, ref = str(source_id), str(stream_name), str(ref_node)
            expected = str(compute_stream_uri(sid, name))
            if ref != expected:
                raise ValueError(
                    f"Managed reference URI mismatch for point {property_uri}: "
                    f"graph has {ref}, expected {expected} from "
                    f"source_id={sid!r}, stream_name={name!r}"
                )
            try:
                kind = stored_value_kind(value_kind)
            except ValueError as error:
                raise ValueError(f"stream {ref}: {error}") from error
            stream_refs[ref] = {"source_id": sid, "stream_name": name, "value_kind": kind}
            if property_uri is not None:
                point_refs.setdefault(str(property_uri), set()).add(ref)
        self._stream_refs, self._point_refs = stream_refs, point_refs
        self._refs_revision = revision
        return len(stream_refs)

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

    def _resolve_experiment_metadata(self, text: str, kind: str) -> str | None:
        hits = self.resolve_text(text, kind=kind, top_k=1, min_score=0.4)
        return hits[0]["uri"] if hits else None

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
            # -> [{"uri": "https://watermetadata.org/ontology/watr#SedimentationTank",
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
        return int(self.graph_status()["source_version"])

    def graph_status(self) -> dict[str, int | bool]:
        """Return source and derived-query generation status.

        Queued experiment writes are published first, so the version covers
        every committed write. Clients cache attribute discovery by version;
        a version that missed queued runs would keep their new keys unknown.
        """
        self.experiments.publish_queued_metadata()
        return self.graph_store.graph_status()

    @semantic_operation
    def insert_graph(
        self,
        rdf_graph: str,
        format: str = "turtle",
        replace: bool = True,
        *,
        source_id: str,
    ) -> dict[str, str]:
        """
        Insert RDF graph into one explicitly owned deployment data graph.

        The embedding indexes cover the bundled ontologies only, so an insert
        does not rebuild them.

        The RDF commits before app reconciliation. Return any reconciliation
        error separately so callers can repair the graph against the published plan.

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
                with self._editing_source(source_id):
                    graph_uri = self.graph_store.source_graph_uri(source_id)
                    self.graph_store.insert_graph(
                        rdf_graph,
                        format=format,
                        replace=replace,
                        graph_uri=graph_uri,
                    )
            logging.info("acquirium: inserted graph into store")
            # Embedding corpus is the static ontoenv vocabularies, not
            # inserted data — no per-insert reindex. Syncing here reports a
            # bad stream registration to the caller that made it.
            with timed_debug(logger, "insert_graph: _sync_stream_refs_from_graph"):
                self._sync_stream_refs_from_graph()
        except Exception as e:
            logging.error("acquirium: failed to insert graph: %s", e)
            raise
        return self.apps.reconcile()

    def _editing_source(self, source_id: str):
        """Hold the experiment metadata edit while writing a source graph it
        owns; any other source needs nothing."""
        owned = source_id in {METADATA_SOURCE_ID, "experiments"} or source_id.startswith("experiment/")
        return self.experiments.metadata.edit() if owned else nullcontext()

    # App changes write derived stream metadata to the graph, which the next
    # _current_refs() call picks up.
    def recover_apps(self) -> None:
        self.apps.recover()

    def _refresh_experiment_apps(self) -> None:
        with self.apps.lock:
            # Delivery without deployed apps must not submit an empty plan:
            # it would consume capacity reserved for experiment samples.
            if self.apps.state["apps"]:
                self.apps.refresh()

    def deploy_app(self, app):
        result = self.apps.deploy(app)
        # Deployment projects new output metadata. Publish the matching query
        # view before a client can query those outputs using the result.
        self._current_refs()
        return result

    def remove_app(self, name):
        result = self.apps.remove(name)
        self._current_refs()
        return result

    def _current_refs(self) -> None:
        """Resync the reference maps if the graph changed since the last sync.

        No apps lock: a deploy can hold it for seconds, and ingest and reads
        must not wait on that. The maps are replaced whole, never mutated, so
        a reader sees either the old or the new registrations.
        """
        if self._refs_revision != self.graph_store.graph_status()["source_version"]:
            self._sync_stream_refs_from_graph()

    def resolve_storage_keys(self, uris, *, view="current", epoch=None):
        """Resolve points and stream references against one published plan."""
        from acquirium.replacements import resolve

        self._current_refs()
        result = {}
        for uri in uris:
            refs = self._point_refs.get(uri, {uri})
            if len(refs) != 1:
                raise ValueError(f"point {uri!r} has multiple references; select an explicit stream_uri")
            result[uri] = next(iter(refs))
        plan = self.apps.plan_at(epoch)
        resolved = resolve(plan, result.values(), view=view)
        return {uri: resolved[ref] for uri, ref in result.items()}

    def timeseries_batch(self, uri, start=None, end=None, limit=None, order="asc",
                         batch_size=10_000, value_mode="default", epoch=None, after=None,
                         view="current"):
        """Read one view at one epoch; ``after`` waits for a published request."""
        if after is not None:
            self.core.wait_for(after)
            if epoch is not None and epoch < self.core.epochs_of([after])[0]:
                raise ValueError("epoch precedes the after receipt")
        if epoch is None:
            epoch = self.core.engine.snapshot().epoch
        stream = self.resolve_storage_keys([uri], view=view, epoch=epoch)[uri]
        mode = normalize_value_mode(value_mode)
        if mode == "default":
            mode = normalize_value_kind(self._reading_value_kind(stream, epoch))
        return self.core.batches(stream, start=start, end=end, limit=limit, order=order,
                                 batch_size=batch_size, epoch=epoch, mode=mode)

    def _reading_value_kind(self, stream: str, epoch: int) -> str:
        """Recover a removed output's kind from the plan at the read epoch."""
        from acquirium.replacements import resolve

        if stream in self._stream_refs:
            return self._stream_refs[stream]["value_kind"]
        plan = self.apps.plan_at(epoch)
        kind = output_value_kind(plan, stream)
        if kind is not None:
            return kind
        # Removed replacement stages can inherit the kind from the raw stream.
        raw = resolve(plan, [stream], view="raw")[stream]
        if raw != stream:
            if raw in self._stream_refs:
                return self._stream_refs[raw]["value_kind"]
            return output_value_kind(plan, raw) or "numeric"
        return "numeric"

    def stream_views(self, uris, *, epoch=None):
        """Pin an epoch and resolve each stream's views at that snapshot."""
        from acquirium.replacements import resolve, units

        if epoch is None:
            snapshot, _ = self.engine_snapshot()
        else:
            snapshot = self.core.engine.snapshot(epoch)
        plan = snapshot.plan()
        heads = resolve(plan, uris, view="current")
        bases = resolve(plan, uris, view="raw")
        stage_units = units(plan)
        streams = {
            uri: {"current": heads[uri], "raw": bases[uri], "unit": stage_units.get(heads[uri])}
            for uri in uris
        }
        return {"epoch": snapshot.epoch, "streams": streams}

    def engine_snapshot(self, *, timeout: float = 30.0):
        """The latest snapshot, once its plan reflects the metadata graph as
        of this call, and whether it does.

        Readers that resolve streams in the graph and then pin an epoch take
        it here. They wait only while a graph write is ahead of the published
        plan, and at most ``timeout`` seconds for an install under way."""
        current = self.apps.catch_up(timeout)
        if not current:
            logger.warning(
                "engine snapshot: the published plan does not reflect the latest "
                "metadata yet; reads at it may miss or include app outputs"
            )
        return self.core.engine.snapshot(), current

    def timeseries_info_batch(self, uris, *, view="current", epoch=None):
        """Resolve and count streams at the requested epoch or the latest snapshot."""
        snapshot = self.core.engine.snapshot(epoch)
        keys = self.resolve_storage_keys(uris, view=view, epoch=snapshot.epoch)
        info = self.core.info(set(keys.values()), snapshot=snapshot)
        return {uri: info[key] for uri, key in keys.items()}

    def timeseries_exists_batch(self, uris: list[str], *, view="current", epoch=None) -> list[str]:
        """The stream or point URIs among ``uris`` that have any row."""
        info = self.timeseries_info_batch(uris, view=view, epoch=epoch)
        return sorted(uri for uri, stats in info.items() if stats["row_count"])

    def insert_timeseries(self, *, source_id, stream_name, rows, request_id=None) -> IngestResult:
        return self.insert_timeseries_batch(source_id, {stream_name: rows}, request_id=request_id)

    def insert_timeseries_batch(self, source_id, streams, *, request_id=None, replace=()) -> IngestResult:
        """Upsert ``{stream_name: rows}`` in one request; each stream named in
        ``replace`` holds exactly its supplied rows afterwards."""
        import polars as pl
        from acquirium.core import time_ns
        refs, times, values = [], [], []
        for name, rows in streams.items():
            ref = str(compute_stream_uri(source_id, name))
            for timestamp, value in rows:
                refs.append(ref)
                times.append(time_ns(timestamp))
                values.append(value)
        frame = pl.DataFrame({"stream_uri": pl.Series(refs, dtype=pl.String),
                              "ts": pl.Series(times, dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                              "value": typed_value_series(values)})
        replaced = {str(compute_stream_uri(source_id, name)) for name in replace}
        return self._ingest(source_id, frame, request_id, replace=replaced)

    def insert_timeseries_frame(self, source_id, frame, *, request_id=None, stored=False) -> IngestResult:
        """Resolve Arrow stream names to references and preserve the value column's dtype."""
        import polars as pl

        names = frame["stream_name"].unique().to_list()
        if any(not isinstance(name, str) or not name for name in names):
            raise ValueError("stream_name must be a nonempty string")
        refs = {name: str(compute_stream_uri(source_id, name)) for name in names}
        value_columns = ("number", "text") if stored else ("value",)
        if not {"ts", *value_columns}.issubset(frame.columns):
            raise ValueError(f"Arrow ingest requires ts and {', '.join(value_columns)} columns")
        frame = frame.select(pl.col("stream_name").replace_strict(refs).alias("stream_uri"), "ts", *value_columns)
        if stored:
            self._validate_refs(source_id, set(refs.values()))
            return self.core.ingest_stored(source_id, request_id or str(uuid.uuid4()), frame)
        return self._ingest(source_id, frame, request_id)

    def _ingest(self, source_id, frame, request_id, *, replace=()) -> IngestResult:
        """Submit ``stream_uri``/``ts``/``value`` rows, each value stored as its
        stream's declared kind."""
        import polars as pl
        refs = set(frame["stream_uri"].unique().to_list()) | set(replace)
        kinds = self._validate_refs(source_id, refs)
        undeclared = [uri for uri, kind in kinds.items() if kind == UNDECLARED_VALUE_KIND]
        if undeclared:
            observed = frame.filter(pl.col("stream_uri").is_in(undeclared))
            inferred = streams_with_numeric_values(observed)
            if inferred:
                # A first numeric observation changes registration metadata.
                # Serialize that transition with other graph edits.
                with self.apps.lock:
                    # Registrations may have changed while acquiring the lock.
                    kinds = self._validate_refs(source_id, refs)
                    inferred = {uri for uri in inferred if kinds[uri] == UNDECLARED_VALUE_KIND}
                    kinds.update(dict.fromkeys(inferred, "numeric"))
                    result = self._submit_readings(source_id, frame, request_id, kinds, replace)
                    if inferred:
                        # Record the inferred kind only after admission succeeds.
                        graph = Graph()
                        for uri in inferred:
                            graph.add((URIRef(uri), INFERRED_VALUE_KIND, Literal("numeric")))
                        self.graph_store.insert_graph(
                            graph, graph_uri=self.graph_store.source_graph_uri(source_id),
                        )
                    return result
        return self._submit_readings(source_id, frame, request_id, kinds, replace)

    def _submit_readings(self, source_id, frame, request_id, kinds, replace) -> IngestResult:
        """Attach the registered kinds so the engine adapter can normalize readings."""
        import polars as pl

        frame = frame.with_columns(
            pl.col("stream_uri").replace_strict(kinds, return_dtype=pl.String).alias("value_kind")
        )
        return self.core.ingest(source_id, request_id or str(uuid.uuid4()), frame, replace=replace)

    def _validate_refs(self, source, references):
        """Each registered reference's value kind; unregistered or foreign
        references fail."""
        self._current_refs()
        kinds = {}
        for reference in references:
            declaration = self._stream_refs.get(reference)
            if declaration is None:
                raise ValueError(f"stream {reference!r} is not registered")
            if declaration["source_id"] != source:
                raise ValueError("stream source mismatch")
            kinds[reference] = declaration["value_kind"]
        return kinds

    def delete_timeseries(self, source_id, stream_name, timestamps, *, request_id=None) -> IngestResult:
        import polars as pl
        from acquirium.core import time_ns
        reference = str(compute_stream_uri(source_id, stream_name))
        self._validate_refs(source_id, {reference})
        frame = pl.DataFrame({"stream_uri": [reference] * len(timestamps),
                              "ts": pl.Series([time_ns(value) for value in timestamps], dtype=pl.Int64).cast(pl.Datetime("ns", "UTC")),
                              "value": pl.Series([None] * len(timestamps), dtype=pl.Float64),
                              "operation": ["delete"] * len(timestamps)})
        return self.core.ingest(source_id, request_id or str(uuid.uuid4()), frame)

    def insert_log(self, entry: LogEntry) -> None:
        property_uri = URIRef(entry.property_uri)
        log_uri = URIRef(f"{property_uri}_log")
        graph = Graph()
        graph.add((property_uri, HAS_LOG, log_uri))
        graph.add((log_uri, RDF.type, LOGBOOK))
        with self._logbook_lock:
            self.logbook.insert(entry)
            self.graph_store.insert_graph(graph, graph_uri=self.graph_store.acquirium_graph_uri)

    def query_logs(self, property_uri: str, log_time_interval: TimeIntervalModel | None = None,
                   obs_time_interval: TimeIntervalModel | None = None) -> list[LogEntry]:
        return self.logbook.query(property_uri, log_time_interval, obs_time_interval)

    def delete_logs(self, property_uri: str) -> bool:
        uri = URIRef(property_uri).n3()
        with self._logbook_lock:
            self.logbook.delete(property_uri)
            self.graph_store.sparql_update(
                f"DELETE WHERE {{ {uri} <{HAS_LOG}> ?log . ?log a <{LOGBOOK}> . }}",
                graph_uri=self.graph_store.acquirium_graph_uri,
            )
        return True

    @semantic_operation
    def sparql_update(self, update: str, source_id: str) -> dict[str, Any]:
        """Execute a SPARQL UPDATE against one explicitly owned data graph.

        It lets a component remove exactly the triples it registered without
        granting updates to ontology graphs.
        """
        with self._editing_source(source_id):
            graph_uri = self.graph_store.source_graph_uri(source_id)
            result = self.graph_store.sparql_update(update, graph_uri=graph_uri)
        self._sync_stream_refs_from_graph()
        return {**result, **self.apps.reconcile()}

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
        self.experiments.publish_queued_metadata()
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
        self.experiments.publish_queued_metadata()
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
        self.experiments.publish_queued_metadata()
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
        compatible = converter.check_compatible(src, tgt)
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
            ("experiment ledger", self.ledger.close),
            ("Escapement", self._close_engine),
            ("graph store", self.graph_store.close),
        ]
        for label, close in steps:
            try:
                close()
            except Exception:
                logger.exception("Manager.close: failed to close %s", label)
        logger.debug("Manager.close: done")

    def _close_engine(self) -> None:
        if not self.core.close(timeout=self.drain_timeout):
            logger.warning(
                "Manager.close: Escapement did not drain within %ss; leaving %d "
                "pending request(s) for the next start",
                self.drain_timeout, self.core.pending_at_close,
            )
