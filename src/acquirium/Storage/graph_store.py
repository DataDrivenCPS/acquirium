from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from threading import Condition, RLock, Thread
from typing import Iterator

from ontoenv import OntoEnv
import pyoxigraph as ox
import shifty
from pyoxigraph import NamedNode, RdfFormat
from rdflib import Dataset, Graph, Literal, RDF, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import XSD, OWL, NamespaceManager
from oxrdflib.store import from_ox


## ALL NAMESPACES AND INTERNAL PREDICATES HERE ##
from acquirium.internals.internals_namespaces import *

from acquirium.internals._log import timed_debug
from acquirium._ontologies import (
    BUNDLED_FILES,
    load_bundled_graph,
    rename_ontology_iri,
)
from acquirium.Server.config import OntologySource
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI, GraphRegistry

_logger = logging.getLogger("acquirium.graph_store")


def _literal_dt(value: datetime) -> Literal:
    '''
    Convert a datetime to an XSD.dateTime Literal in UTC.
    '''
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return Literal(value.isoformat(), datatype=XSD.dateTime)


def _maybe_literal_dt(value: Literal | None) -> datetime | None:
    '''
    convert an XSD.dateTime Literal to a datetime object in UTC.
    Return None if value is None or cannot be parsed.
    '''
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _external_uri(subject: URIRef) -> str:
    '''
    Convert an internal URIRef to an external string representation.
    If the URIRef is from a point, strip the prefix.
    '''
    uri_str = str(subject)
    if uri_str.startswith(ACQUIRIUM_POINT_NS):
        return uri_str[len(ACQUIRIUM_POINT_NS) :]
    return uri_str


def _graph_affects_closure(graph: Graph) -> bool:
    """Return True if *graph* can change owl:imports-driven closure."""
    return any(graph.triples((None, OWL.imports, None))) or any(
        graph.triples((None, RDF.type, OWL.Ontology))
    )


# Kept at the old cache IRI so a warm start overwrites the former materialized
# ``inferred + dependencies`` cache instead of leaving it in the query dataset.
_DEPENDENCY_QUERY_GRAPH = URIRef(str(ACQUIRIUM_NS.ImportsUnionGraph))
_INFERRED_DATA_GRAPH = URIRef(str(ACQUIRIUM_NS.InferredDataGraph))

class _OntoenvOxigraphStore:
    """ontoenv graph-store protocol over the shared Oxigraph dataset.

    Each ontology ontoenv holds is one Oxigraph named graph keyed by its
    IRI, so ontoenv's graphs and the instance data live in one store and
    ontoenv's own closure tooling runs against Oxigraph. ``on_change`` is
    fired on any add/remove so the owner can invalidate its cached
    data-graph closure.

    ``excluded_iris`` names the graphs acquirium owns in that shared
    dataset. ontoenv syncs its catalog from ``graph_ids()``, so anything
    left in there gets catalogued as an ontology — the main data graph and
    rdflib's default graph would otherwise show up as (empty, and with
    their non-absolute IRIs resolved against the cwd) bogus ontologies.
    """

    def __init__(
        self,
        dataset: Dataset,
        on_change: Callable[[], None],
        *,
        excluded_iris: Iterable[URIRef] = (),
    ) -> None:
        self._ds = dataset
        self._on_change = on_change
        self._excluded = {str(iri) for iri in excluded_iris} | {
            str(DATASET_DEFAULT_GRAPH_ID)
        }

    def add_graph(self, iri: str, graph: Graph, overwrite: bool = False) -> None:
        ctx = self._ds.graph(URIRef(iri))
        if len(ctx) and not overwrite:
            _logger.debug("add_graph skip (already populated): %s (%d triples)", iri, len(ctx))
            return
        ctx.remove((None, None, None))
        # rdflib's per-triple ctx.add() crosses the Rust FFI once per triple
        # — ~76s for the ~535k-triple bundled-ontology load on a cold start.
        # Serialise to N-Triples (rdflib's fastest writer) once and bulk-load
        # through pyoxigraph, which writes straight to SST. ~10× faster.
        with timed_debug(_logger, "add_graph serialize %s (%d triples)", iri, len(graph)):
            nt = graph.serialize(format="nt", encoding="utf-8")
        with timed_debug(_logger, "add_graph bulk_load %s (%d bytes)", iri, len(nt)):
            self._ds.store._inner.bulk_load(
                input=nt, format=RdfFormat.N_TRIPLES, to_graph=NamedNode(iri),
            )
        self._on_change()

    def get_graph(self, iri: str) -> Graph:
        out = Graph()
        for triple in self._ds.graph(URIRef(iri)):
            out.add(triple)
        return out

    def remove_graph(self, iri: str) -> None:
        graph = self._ds.graph(URIRef(iri))
        if len(graph) == 0:
            return
        graph.remove((None, None, None))
        self._on_change()

    def _ontology_graphs(self) -> list[Graph]:
        return [g for g in self._ds.graphs() if str(g.identifier) not in self._excluded]

    def graph_ids(self) -> list[str]:
        return [str(g.identifier) for g in self._ontology_graphs()]

    def size(self) -> dict[str, int]:
        graphs = self._ontology_graphs()
        return {
            "num_graphs": len(graphs),
            "num_triples": sum(len(g) for g in graphs),
        }


class OxigraphGraphStore:
    """Oxigraph-backed source store with an in-memory imports-closure cache.

    There are two distinct representations in play:

    - The persisted source dataset is the system of record. Driver/API writes
      and ontoenv-managed ontology graphs all land there.
    - The dependency cache is an in-memory rdflib graph containing only the
      triples introduced by owl:imports closure expansion. It exists for
      export-style "data plus dependencies" views, not for normal SPARQL
      reads.
    """

    def __init__(
        self,
        *,
        store_path: str | Path,
        env_root: str | Path,
        main_graph_uri: URIRef = DEFAULT_MAIN_GRAPH,
        extra_ontology_sources: list[OntologySource] | None = None,
    ):
        # Serializes the public surface below. Oxigraph's own core is
        # thread-safe, but the derived state around it is not: the version
        # counters are plain ints, and refreshing a cache empties a graph
        # before reloading it, so an unguarded reader can observe the gap.
        # Reentrant because read paths (sparql_query, export_graph) refresh
        # those caches themselves. The condition uses this same lock to make
        # derived-cache rebuilding single-flight: one thread builds while
        # fresh readers wait for its complete publication.
        self._lock = RLock()
        self._rebuild_condition = Condition(self._lock)
        self._query_rebuild_in_progress = False
        self._query_rebuild_error: Exception | None = None
        # A batch delays only rebuild scheduling; each source write remains
        # durable immediately.  This deliberately is not a transaction.
        self._write_batch_depth = 0
        self._write_batch_rebuild_pending = False
        self.store_path = Path(store_path)
        self.env_root = Path(env_root)
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.env_root.mkdir(parents=True, exist_ok=True)
        self.source_store_path = self.store_path / "source"
        self.query_store_path = self.store_path / "query"
        self.source_store_path.mkdir(parents=True, exist_ok=True)
        self.query_store_path.mkdir(parents=True, exist_ok=True)
        _logger.debug(
            "OxigraphGraphStore.__init__: store=%s env_root=%s", self.store_path, self.env_root
        )

        with timed_debug(_logger, "OxigraphGraphStore._open_dataset source=%s", self.source_store_path):
            self.source_dataset, self.source_store_path = self._open_dataset(self.source_store_path)
        with timed_debug(_logger, "OxigraphGraphStore._open_dataset query=%s", self.query_store_path):
            self.query_dataset, self.query_store_path = self._open_dataset(self.query_store_path)

        self.main_graph_uri = main_graph_uri
        self.graph_registry = GraphRegistry(
            self.store_path / "graph_registry.json",
            plant_graph_uri=str(self.main_graph_uri),
        )
        self.acquirium_graph_uri = URIRef(ACQUIRIUM_GRAPH_URI)
        self._source_version = self._load_source_version()
        self._closure_version = 0
        self._dependency_graph_cache: Graph | None = None
        self._dependency_graph_closure_version = -1
        self.dependency_query_graph_uri = _DEPENDENCY_QUERY_GRAPH
        self.inferred_data_graph_uri = _INFERRED_DATA_GRAPH
        self._query_cache_source_version = -1
        self._query_cache_closure_version = -1
        self._dependency_query_graph_closure_version = -1

        # ontoenv shares this Oxigraph store via the graph-store protocol,
        # over the authoritative source dataset. SPARQL reads query that
        # dataset directly; only export-time dependency closure is cached.
        self._ontoenv_store = _OntoenvOxigraphStore(
            self.source_dataset,
            self._mark_ontology_graph_changed,
            excluded_iris=(
                self.main_graph_uri,
                self.acquirium_graph_uri,
                self.dependency_query_graph_uri,
                self.inferred_data_graph_uri,
            ),
        )
        with timed_debug(_logger, "OntoEnv build env_root=%s", self.env_root):
            self.env, is_warm_start = self._build_ontoenv()
        # On a cold start, the persisted store has no bundled ontologies
        # yet — parse each TTL, rewrite its declared owl:Ontology IRI to
        # the package's canonical IRI, and register it. On warm start
        # the bundled graphs are already in the store; skip re-adding.
        if not is_warm_start:
            for fname, canonical in BUNDLED_FILES:
                try:
                    g = load_bundled_graph(fname, canonical)
                    self.env.add(g, fetch_imports=False)
                    _logger.info("ontoenv: registered bundled %s at %s", fname, canonical)
                except Exception as exc:
                    _logger.error("ontoenv: failed to load bundled %s: %s", fname, exc)
        # User sources from acquirium.toml. An entry without `rename_to`
        # keeps its declared IRI; with `rename_to`, we rewrite the
        # declared IRI to the canonical key and pass overwrite=True so
        # the user's content replaces whatever graph was previously
        # registered there (typically a bundled one).
        for src in extra_ontology_sources or []:
            try:
                self._add_user_source(src)
            except Exception as exc:
                _logger.warning(
                    "ontoenv: failed to register user ontology source %s: %s",
                    src.source, exc,
                )
        self._commit_dataset(self.source_dataset)
        _logger.debug(
            "OxigraphGraphStore.__init__: ready (main_graph=%s, source_version=%d)",
            self.main_graph_uri, self._source_version,
        )

    # -------------------- ontoenv named-graph access --------------------
    def named_graph(self, iri: str) -> Graph:
        """One ontology's own graph from ontoenv (owl:imports NOT followed).

        This is ontoenv's read-only store-backed view: cheap to take, but
        mutating it raises ``ValueError``. Callers that need to write should
        copy the triples out (or use ``env.copy_graph``).
        """
        with self._lock:
            return self.env.get_graph(iri)

# -------------------- source + dependency cache coordination --------------------
    def _source_state_path(self) -> Path:
        return self.source_store_path / "acquirium_source_state.json"

    def _has_persisted_source_graphs(self) -> bool:
        """Return True when the attached Oxigraph store already contains graphs."""
        return any(True for _ in self.source_dataset.graphs())

    def _new_ontoenv(self) -> OntoEnv:
        """Open the environment at ``env_root`` over the shared graph store.

        ``connect`` covers both starts: it creates the catalog under
        ``.ontoenv/`` when there isn't one yet and reopens it when there
        is, reconciling either way against what the graph store currently
        holds. The narrower entry points (``create``, ``open``, ``adopt``)
        each rule out one of those cases, so none of them fit here.
        """
        return OntoEnv.connect(
            str(self.env_root),
            graph_store=self._ontoenv_store,
        )

    def _build_ontoenv(self) -> tuple[OntoEnv, bool]:
        """Build (or restore) the ontoenv environment.

        Returns ``(env, is_warm_start)``. Opening the environment is the
        same call either way, so what this really decides is the flag: a
        warm start means the persisted Oxigraph store already contains
        bundled ontology graphs from a previous run and the caller can skip
        re-adding them. On a cold start — or if the warm-start open raises,
        in which case we retry and treat the result as empty — populating
        the environment is the caller's job.
        """
        can_warm_start = (
            self._source_state_path().exists() and self._has_persisted_source_graphs()
        )
        if can_warm_start:
            try:
                _logger.debug("ontoenv: reopening over the populated store")
                return self._new_ontoenv(), True
            except Exception as exc:
                _logger.warning(
                    "ontoenv: warm start failed; building empty env: %s", exc
                )
        _logger.debug("ontoenv: cold start, building empty env")
        return self._new_ontoenv(), False

    def _add_user_source(self, src: OntologySource) -> None:
        """Register one ``[ontologies] sources`` entry with ontoenv.

        - No ``rename_to``: hand the source to ``env.add`` as-is, letting
          ontoenv key it under its own declared ``owl:Ontology`` IRI.
        - With ``rename_to``: parse the source into an rdflib graph,
          rewrite the declared ontology IRI to the canonical value, and
          add with ``overwrite=True`` so it replaces any pre-existing
          graph at that IRI (typically a bundled one).
        """
        if src.rename_to is None:
            self.env.add(src.source, fetch_imports=False)
            _logger.info("ontoenv: registered %s (no rename)", src.source)
            return
        g = Graph()
        g.parse(src.source)
        declared = next(iter(g.subjects(RDF.type, OWL.Ontology)), None)
        target = URIRef(src.rename_to)
        if declared is not None and str(declared) != str(target):
            rename_ontology_iri(g, URIRef(str(declared)), target)
        elif declared is None:
            # No owl:Ontology in the source — synthesize one so ontoenv
            # registers the graph at the canonical IRI.
            g.add((target, RDF.type, OWL.Ontology))
        self.env.add(g, overwrite=True, fetch_imports=False)
        _logger.info(
            "ontoenv: registered %s at canonical IRI %s (rename applied)",
            src.source, src.rename_to,
        )

    def _load_source_version(self) -> int:
        path = self._source_state_path()
        if not path.exists():
            return 0
        try:
            raw = json.loads(path.read_text())
        except (OSError, ValueError):
            return 0
        value = raw.get("version") if isinstance(raw, dict) else 0
        return int(value) if isinstance(value, int) else 0

    def _write_source_version(self) -> None:
        self._source_state_path().write_text(json.dumps({"version": self._source_version}))

    def _mark_source_changed(self) -> None:
        self._source_version += 1
        self._write_source_version()

    def _mark_ontology_graph_changed(self) -> None:
        self._mark_source_changed()
        self._mark_closure_changed()

    def _invalidate_dependency_cache(self) -> None:
        self._dependency_graph_closure_version = -1

    def _mark_closure_changed(self) -> None:
        # Closure invalidation is narrower than source invalidation: ordinary
        # instance-data writes should not force dependency recomputation.
        self._closure_version += 1
        self._invalidate_dependency_cache()

    def _ensure_dependency_cache_current(self) -> Graph:
        if self._dependency_graph_closure_version != self._closure_version:
            return self._refresh_dependency_cache()
        cached = self._dependency_graph_cache or Graph()
        _logger.debug(
            "_ensure_dependency_cache_current: cache hit (closure_v=%d, %d dep triples)",
            self._closure_version, len(cached),
        )
        return cached

    def _refresh_dependency_cache(self) -> Graph:
        with timed_debug(_logger, "_refresh_dependency_cache (closure_v=%d)", self._closure_version):
            # Imports may be declared by the plant or by a source-owned graph.
            # Use the complete deployment-data union as the working graph, then
            # retain only triples introduced by import resolution as shapes.
            data_graph = self._source_data_graph()
            closure = Graph()
            for triple in data_graph:
                closure.add(triple)
            # OntoEnv mutates the working graph in place by loading imported
            # ontologies, so keep only the dependency delta in the cache.
            self.env.import_dependencies(closure)
            deps = Graph()
            for triple in closure:
                if triple not in data_graph:
                    deps.add(triple)
            self._dependency_graph_cache = deps
            self._dependency_graph_closure_version = self._closure_version
        _logger.debug("_refresh_dependency_cache: %d dep triples", len(deps))
        return deps

    def _source_graph_with_dependencies(self) -> Graph:
        """Materialize the export view: source graph plus imported triples."""
        merged = Graph()
        for triple in self._source_data_graph():
            merged.add(triple)
        for triple in self._ensure_dependency_cache_current():
            merged.add(triple)
        return merged

    def _dependency_query_graph(self) -> Graph:
        """The query-dataset graph containing only import dependencies."""
        return self.query_dataset.graph(self.dependency_query_graph_uri)

    def _inferred_data_graph(self) -> Graph:
        return self.query_dataset.graph(self.inferred_data_graph_uri)

    def _query_cache_is_current(self) -> bool:
        return (
            self._query_cache_source_version == self._source_version
            and self._query_cache_closure_version == self._closure_version
        )

    @staticmethod
    def _copy_graph(graph: Graph) -> Graph:
        """Return an inference input snapshot detached from mutable store state."""
        snapshot = Graph()
        for triple in graph:
            snapshot.add(triple)
        return snapshot

    def _snapshot_query_inputs(self) -> tuple[int, int, Graph, Graph]:
        """Capture one consistent data/shapes generation while holding ``_lock``."""
        with timed_debug(_logger, "derived snapshot data"):
            data = self._source_data_graph()
        with timed_debug(_logger, "derived snapshot shapes"):
            shapes = self._copy_graph(self._ensure_dependency_cache_current())
        return self._source_version, self._closure_version, data, shapes

    def _build_query_views(self, data: Graph, shapes: Graph) -> Graph:
        """Build inferred deployment data outside the store lock."""
        with timed_debug(
            _logger,
            "shifty.infer data=%d triples shapes=%d triples",
            len(data),
            len(shapes),
        ):
            inferred = shifty.infer(data, shapes).graph()

        return inferred

    def _replace_query_graph(self, graph: Graph, graph_uri: URIRef, *, label: str) -> None:
        """Replace one disposable query graph while publication is locked."""
        target = self.query_dataset.graph(graph_uri)
        with timed_debug(_logger, "derived publish %s clear", label):
            target.remove((None, None, None))
        if not len(graph):
            return
        with timed_debug(_logger, "derived publish %s serialize", label):
            nt = graph.serialize(format="nt", encoding="utf-8")
        with timed_debug(_logger, "derived publish %s load", label):
            self.query_dataset.store._inner.bulk_load(
                input=nt,
                format=RdfFormat.N_TRIPLES,
                to_graph=NamedNode(str(graph_uri)),
            )

    def _publish_query_views(self, inferred: Graph, shapes: Graph) -> None:
        """Publish inferred data and, when needed, the dependency graph.

        The dependency graph is independent of ordinary data writes. Keeping
        it separate avoids copying and serializing the full ontology/shape
        closure into a second merged graph after every inference rebuild.
        Queries that include dependencies use Oxigraph's native union of the
        two named graphs.
        """

        self._replace_query_graph(inferred, self.inferred_data_graph_uri, label="inferred")
        if self._dependency_query_graph_closure_version != self._closure_version:
            self._replace_query_graph(
                shapes,
                self.dependency_query_graph_uri,
                label="dependencies",
            )
            self._dependency_query_graph_closure_version = self._closure_version
        with timed_debug(_logger, "derived publish commit"):
            self._commit_dataset(self.query_dataset)
        self._query_cache_source_version = self._source_version
        self._query_cache_closure_version = self._closure_version

    def _start_query_rebuild_locked(self) -> None:
        """Start the sole background rebuild owner. Caller holds ``_lock``."""
        if self._query_rebuild_in_progress or self._query_cache_is_current():
            return
        self._query_rebuild_in_progress = True
        self._query_rebuild_error = None
        Thread(target=self._rebuild_query_views, name="acquirium-derived-graph", daemon=True).start()

    @contextmanager
    def write_batch(self) -> Iterator[None]:
        """Coalesce derived-cache scheduling for a group of source writes.

        Writes are still committed independently, so an exception does not
        roll them back.  The benefit is that the outermost scope starts at
        most one rebuild after its writes finish.  The method is intentionally
        server-side: an HTTP client cannot use a process-local context manager
        to span separate requests.
        """
        with self._lock:
            self._write_batch_depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._write_batch_depth -= 1
                if self._write_batch_depth < 0:
                    raise RuntimeError("graph write batch depth underflow")
                if self._write_batch_depth == 0 and self._write_batch_rebuild_pending:
                    self._write_batch_rebuild_pending = False
                    self._start_query_rebuild_locked()

    def _rebuild_query_views(self) -> None:
        """Build the latest generation; writes during a build are coalesced."""
        error: Exception | None = None
        try:
            while True:
                with self._lock:
                    source_version, closure_version, data, shapes = self._snapshot_query_inputs()
                inferred = self._build_query_views(data, shapes)
                with self._rebuild_condition:
                    if source_version == self._source_version and closure_version == self._closure_version:
                        self._publish_query_views(inferred, shapes)
                        return
                    _logger.debug("derived cache changed during rebuild; coalescing follow-up")
        except Exception as exc:
            error = exc
            _logger.exception("derived cache rebuild failed")
        finally:
            with self._rebuild_condition:
                self._query_rebuild_error = error
                self._query_rebuild_in_progress = False
                self._rebuild_condition.notify_all()

    def _ensure_query_cache_current(self, *, wait_for_fresh: bool) -> None:
        """Ensure a complete cache exists, optionally waiting for the latest one.

        Writes schedule one background rebuild. Eventual callers get the last
        published cache immediately; strict callers wait for the current source
        generation. All callers wait until the first cache is published.
        """
        with self._rebuild_condition:
            if self._query_cache_is_current():
                return
            self._start_query_rebuild_locked()
            if self._query_cache_source_version >= 0 and not wait_for_fresh:
                return
            while self._query_rebuild_in_progress:
                self._rebuild_condition.wait()
            if self._query_cache_is_current():
                return
            if self._query_rebuild_error is not None:
                raise RuntimeError("derived cache rebuild failed") from self._query_rebuild_error
            raise RuntimeError("derived cache remained stale after rebuild")

    def _apply_graph_write(self, incoming: Graph, *, target: Graph, replace: bool) -> Graph:
        """Apply a parsed graph write to one registered source graph."""
        if replace:
            target.remove((None, None, None))
        for triple in incoming:
            target.add(triple)
        # Propagate prefix bindings declared in the incoming Turtle/RDF
        # (rdflib's parser populates incoming.namespace_manager from
        # `@prefix` directives) so they survive into the stores that back
        # the public /namespace/list endpoint.
        for prefix, ns_uri in incoming.namespaces():
            try:
                target.bind(prefix, ns_uri, override=False)
                self.query_dataset.namespace_manager.bind(
                    prefix, ns_uri, override=False
                )
            except Exception:
                _logger.debug(
                    "namespace bind failed for %s=%s", prefix, ns_uri, exc_info=True
                )
        return target

    def _registered_data_graph(self, graph_uri: URIRef | None) -> Graph:
        """Return a writable registered graph; default to the plant graph.

        The registry is the allow-list that separates deployment data from
        ontoenv-managed ontology graphs in the shared source dataset.
        """
        target_uri = graph_uri or self.main_graph_uri
        if target_uri not in {URIRef(record.uri) for record in self.graph_registry.data_graphs()}:
            raise ValueError(f"graph is not a registered data input: {target_uri}")
        return self.source_dataset.graph(target_uri)

    def _finalize_source_write(self, *, affects_closure: bool) -> None:
        """Persist a write, mark derived state stale, and schedule its rebuild."""
        self._commit_dataset(self.source_dataset)
        self._mark_source_changed()
        if affects_closure:
            self._mark_closure_changed()
        if self._write_batch_depth:
            self._write_batch_rebuild_pending = True
        else:
            self._start_query_rebuild_locked()

    def graph_status(self) -> dict[str, int | bool]:
        """Return the source generation and the state of its derived cache."""
        with self._lock:
            return {
                "source_version": self._source_version,
                "published_version": self._query_cache_source_version,
                "is_current": self._query_cache_is_current(),
                "rebuild_in_progress": self._query_rebuild_in_progress,
            }

    def refresh_union(self, snapshot_path: str | Path | None = None) -> dict[str, int]:
        """Refresh inferred query views from all data graphs and shape graphs."""
        # Do not hold the store lock while inference runs: the single-flight
        # coordinator owns publication, while writes may continue during the
        # expensive build.
        self._ensure_query_cache_current(wait_for_fresh=True)
        with self._lock:
            source_data = self._source_data_graph()
            dependencies = self._ensure_dependency_cache_current()
            if snapshot_path:
                snapshot_path = Path(snapshot_path)
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                merged = Graph()
                for triple in source_data:
                    merged.add(triple)
                for triple in dependencies:
                    merged.add(triple)
                merged.serialize(destination=str(snapshot_path), format="turtle")
            return {
                "main_triples": len(source_data),
                "union_triples": len(source_data) + len(dependencies),
            }

    # -------------------- SPARQL surface --------------------
    def _query_default_graphs(
        self, include_dependencies: bool, *, wait_for_fresh: bool
    ) -> list[NamedNode]:
        """Select current named graphs for a query's default graph."""
        self._ensure_query_cache_current(wait_for_fresh=wait_for_fresh)
        if include_dependencies:
            return [
                NamedNode(str(self.inferred_data_graph_uri)),
                NamedNode(str(self.dependency_query_graph_uri)),
            ]
        return [NamedNode(str(self.inferred_data_graph_uri))]

    def sparql_query(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
    ) -> dict:
        _logger.debug("sparql_query dependencies=%s query=%s", include_dependencies, query)
        # Take Oxigraph's repeatable-read snapshot while publication is locked.
        # Publication clears then bulk-loads its named graph, so starting a
        # query outside this short critical section could select that empty
        # intermediate state. Result iteration remains outside the lock.
        default_graphs = self._query_default_graphs(
            include_dependencies, wait_for_fresh=wait_for_fresh
        )
        with timed_debug(_logger, "sparql_query dependencies=%s", include_dependencies):
            dataset = self.query_dataset
            with timed_debug(_logger,"sparql_query--oxi query time:"):
                with self._lock:
                    result = dataset.store._inner.query(
                        query,
                        use_default_graph_as_union=False,
                        default_graph=default_graphs,
                    )
            with timed_debug(_logger,"sparql_query--output processing:"):
                if isinstance(result, ox.QueryBoolean):
                    out = {"columns": [], "rows": [[bool(result)]]}
                elif isinstance(result, ox.QuerySolutions):
                    cols = [str(v.value) for v in result.variables]
                    rows = [[from_ox(cell) for cell in row] for row in result]
                    out = {"columns": cols, "rows": rows}
                elif isinstance(result, ox.QueryTriples):
                    triples = Graph()
                    triples += (from_ox(t) for t in result)
                    out = {"columns": ["triple"], "rows": [[triple] for triple in triples]}
                else:
                    raise ValueError(f"Unexpected query result: {result!r}")
        _logger.debug("sparql_query: %d rows", len(out["rows"]))
        return out

    def sparql_query_json(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
    ) -> bytes | None:
        """Serialize SELECT/ASK results in PyOxyGraph without RDFLib conversion.

        Graph result forms return ``None`` because SPARQL results JSON does not
        represent RDF triples.
        """
        default_graphs = self._query_default_graphs(
            include_dependencies, wait_for_fresh=wait_for_fresh
        )
        with timed_debug(_logger, "sparql_query_json dependencies=%s", include_dependencies):
            with self._lock:
                result = self.query_dataset.store._inner.query(
                    query,
                    use_default_graph_as_union=False,
                    default_graph=default_graphs,
                )
            if not isinstance(result, (ox.QuerySolutions, ox.QueryBoolean)):
                return None
            serialized = result.serialize(format=ox.QueryResultsFormat.JSON)
            return bytes(serialized) if serialized is not None else None

    def sparql_query_serialized(
        self,
        query: str,
        include_dependencies: bool = True,
        *,
        wait_for_fresh: bool = False,
        results_format: ox.QueryResultsFormat = ox.QueryResultsFormat.JSON,
        graph_format: ox.RdfFormat = ox.RdfFormat.TURTLE,
    ) -> tuple[bytes, str]:
        """Run a query and return a standards-format SPARQL protocol body.

        The derived inferred graph is the default dataset. ``include_dependencies`` adds
        the resolved ontology/shape closure, and ``wait_for_fresh`` selects
        Acquirium's strict-versus-published cache behavior before the query's
        normal Oxigraph repeatable-read snapshot is taken.
        """
        default_graphs = self._query_default_graphs(
            include_dependencies, wait_for_fresh=wait_for_fresh
        )
        with self._lock:
            result = self.query_dataset.store._inner.query(
                query,
                use_default_graph_as_union=False,
                default_graph=default_graphs,
            )
        if isinstance(result, (ox.QuerySolutions, ox.QueryBoolean)):
            serialized = result.serialize(format=results_format)
            return bytes(serialized), results_format.media_type
        if isinstance(result, ox.QueryTriples):
            serialized = result.serialize(format=graph_format)
            return bytes(serialized), graph_format.media_type
        raise ValueError(f"Unexpected query result: {result!r}")

    def sparql_update(self, update: str, *, graph_uri: URIRef | None = None) -> dict:
        _logger.debug("sparql_update: %s", update.replace("\n", " ")[:200])
        with self._lock, timed_debug(_logger, "sparql_update"):
            self._registered_data_graph(graph_uri).update(update)
            self._finalize_source_write(affects_closure=True)
        return {"message": "update applied", "changed": True}

    def validate(self) -> dict[str, str | bool]:
        """Validate the full data union against the ontology shape closure.

        Shifty runs SHACL-AF inference before validation by default, so this
        uses the same effective data and shape inputs as the inferred cache.
        """
        with self._lock, timed_debug(_logger, "shifty.validate"):
            data = self._source_data_graph()
            shapes = self._ensure_dependency_cache_current()
            conforms, report_graph, results_text = shifty.validate(
                data,
                shapes,
                graph_mode="union",
            )
            return {
                "conforms": bool(conforms),
                "report": report_graph.serialize(format="turtle"),
                "results_text": results_text,
            }

    def export_graph(self, *, include_dependencies: bool = True, format: str = "turtle") -> str:
        """Serialize all deployment data, optionally with ontology dependencies.

        ``include_dependencies=False`` never means the legacy plant graph alone: it
        includes every registered source-owned graph and Acquirium's managed
        deployment data. ``True`` adds the resolved ontology/shape closure.
        """
        fmt = (format or "turtle").lower()
        with self._lock:
            graph = self._source_graph_with_dependencies() if include_dependencies else self._source_data_graph()
            return graph.serialize(format=fmt)

    def export_dependency_graph(self, *, format: str = "trig") -> str:
        """Serialize only the imported triples (closure minus instance data)."""
        fmt = (format or "trig").lower()
        with self._lock:
            return self._ensure_dependency_cache_current().serialize(format=fmt)

    def insert_graph(self, content: str | bytes | Graph, *, format: str = "turtle", replace: bool = False, graph_uri: URIRef | None = None) -> dict[str, int | bool]:
        """Parse incoming graph data and merge (or replace) into one data graph.
        format: turtle | n3 | xml | trix

        ``graph_uri`` must be one of the graph registry's data inputs. Omitting
        it preserves the legacy behavior of writing the plant graph.
        """

        fmt = (format or "turtle").lower()
        incoming = Graph()
        if isinstance(content, Graph):
            incoming = content
        else:
            with timed_debug(_logger, "insert_graph parse fmt=%s", fmt):
                incoming.parse(data=content, format=fmt)
        _logger.debug("insert_graph: %d incoming triples (replace=%s)", len(incoming), replace)

        # Parsing stays outside the lock: `incoming` is thread-local and this is
        # the expensive part of the call.
        affects_closure = replace or _graph_affects_closure(incoming)
        with self._lock:
            with timed_debug(_logger, "insert_graph merge into data graph (replace=%s, affects_closure=%s)", replace, affects_closure):
                target = self._registered_data_graph(graph_uri)
                self._apply_graph_write(
                    incoming, target=target, replace=replace,
                )
                self._finalize_source_write(affects_closure=affects_closure)
        return {
            "replaced": replace,
            "changed": True,
        }

    def namespace_manager(self) -> NamespaceManager :
        return self.query_dataset.namespace_manager
    
    # -------------------- helpers --------------------
    def source_graph_uri(self, source_id: str) -> URIRef:
        """Get the registered data graph for one driver or metadata source."""
        with self._lock:
            return URIRef(self.graph_registry.source_graph(source_id).uri)

    def _source_data_graph(self) -> Graph:
        """Materialize the union of registered deployment data graphs only."""
        merged = Graph()
        for record in self.graph_registry.data_graphs():
            for triple in self.source_dataset.graph(URIRef(record.uri)):
                merged.add(triple)
        return merged

    @staticmethod
    def _commit_dataset(dataset: Dataset) -> None:
        try:
            dataset.commit()
        except Exception:
            # Oxigraph's commit is a no-op but keep for forward compatibility.
            pass

    def close(self) -> None:
        _logger.debug("OxigraphGraphStore.close")
        # A rebuild owns snapshots only while it is running, but its final
        # publication writes the query dataset. Do not close that dataset out
        # from under the sole background rebuild owner.
        with self._rebuild_condition:
            while self._query_rebuild_in_progress:
                self._rebuild_condition.wait()
        with self._lock:
            # ontoenv holds an exclusive lock on .ontoenv/store.lock for the
            # life of the environment, so it has to be released before the
            # next open — otherwise reopening the same env_root (tests, or a
            # restart within one process) fails to acquire it.
            try:
                self.env.close()
            except Exception:
                pass
            for dataset in (self.source_dataset, self.query_dataset):
                try:
                    dataset.close()
                except Exception:
                    pass

    # -------------------- internal: store bootstrap --------------------
    @staticmethod
    def _open_dataset(path: Path) -> tuple[Dataset, Path]:
        """Open an Oxigraph-backed Dataset at *path*.

        RocksDB's LOCK file is what stops two processes from opening the same
        store and corrupting it, so a lock error is fatal: removing the lock or
        diverting to a scratch store would turn a startup failure into silent
        corruption or silent data loss.
        """
        dataset = Dataset(store="Oxigraph", default_union=False)
        try:
            dataset.open(str(path))
        except OSError as exc:
            raise RuntimeError(
                f"Cannot open Oxigraph store at {path}: {exc}. "
                "Another Acquirium process is most likely already using this store "
                "(the DuckDB and Oxigraph backends allow a single process only). "
                "Stop it, or point this server at a different data directory."
            ) from exc
        return dataset, path
