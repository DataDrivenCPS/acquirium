from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

from ontoenv import OntoEnv
import pyoxigraph as ox
from pyoxigraph import NamedNode, RdfFormat
from rdflib import Dataset, Graph, Literal, RDF, URIRef
from rdflib.namespace import XSD, OWL, NamespaceManager
from oxrdflib.store import from_ox


## ALL NAMESPACES AND INTERNAL PREDICATES HERE ##
from acquirium.internals.internals_namespaces import *

from acquirium.internals.models import Point, PointCreateRequest
from acquirium.internals._log import timed_debug
from acquirium._ontologies import (
    BUNDLED_FILES,
    load_bundled_graph,
    rename_ontology_iri,
)
from acquirium.Server.config import OntologySource

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


_IMPORTS_UNION_GRAPH = URIRef(str(ACQUIRIUM_NS.ImportsUnionGraph))


class _OntoenvOxigraphStore:
    """ontoenv graph-store protocol over the shared Oxigraph dataset.

    Each ontology ontoenv holds is one Oxigraph named graph keyed by its
    IRI, so ontoenv's graphs and the instance data live in one store and
    ontoenv's own closure tooling runs against Oxigraph. ``on_change`` is
    fired on any add/remove so the owner can invalidate its cached
    data-graph closure.
    """

    def __init__(self, dataset: Dataset, on_change: Callable[[], None]) -> None:
        self._ds = dataset
        self._on_change = on_change

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

    def graph_ids(self) -> list[str]:
        return [str(g.identifier) for g in self._ds.graphs()]

    def size(self) -> dict[str, int]:
        graphs = list(self._ds.graphs())
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
        # those caches themselves.
        self._lock = RLock()
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
        self._source_version = self._load_source_version()
        self._closure_version = 0
        self._dependency_graph_cache: Graph | None = None
        self._dependency_graph_closure_version = -1
        self.imports_union_graph_uri = _IMPORTS_UNION_GRAPH
        self._imports_union_graph_source_version = -1
        self._imports_union_graph_closure_version = -1

        # ontoenv shares this Oxigraph store via the graph-store protocol,
        # over the authoritative source dataset. SPARQL reads query that
        # dataset directly; only export-time dependency closure is cached.
        self._ontoenv_store = _OntoenvOxigraphStore(
            self.source_dataset,
            self._mark_ontology_graph_changed,
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
        """One ontology's own graph from ontoenv (owl:imports NOT followed)."""
        with self._lock:
            return self.env.get_graph(iri)

# -------------------- source + dependency cache coordination --------------------
    def _source_state_path(self) -> Path:
        return self.source_store_path / "acquirium_source_state.json"

    def _has_persisted_source_graphs(self) -> bool:
        """Return True when the attached Oxigraph store already contains graphs."""
        return any(True for _ in self.source_dataset.graphs())

    def _new_ontoenv(self, *, init_from_store: bool) -> OntoEnv:
        return OntoEnv(
            path=str(self.env_root),
            graph_store=self._ontoenv_store,
            init_from_store=init_from_store,
        )

    def _build_ontoenv(self) -> tuple[OntoEnv, bool]:
        """Build (or restore) the ontoenv environment.

        Returns ``(env, is_warm_start)``. A warm start means the persisted
        Oxigraph store already contains bundled ontology graphs from a
        previous run, so the caller can skip re-adding them. On cold start
        (or if init_from_store fails) we build an empty environment and
        the caller is responsible for populating it.
        """
        can_warm_start = (
            self._source_state_path().exists() and self._has_persisted_source_graphs()
        )
        if can_warm_start:
            try:
                _logger.debug("ontoenv: rebuilding from store")
                return self._new_ontoenv(init_from_store=True), True
            except Exception as exc:
                _logger.warning(
                    "ontoenv: init_from_store failed; building empty env: %s", exc
                )
        _logger.debug("ontoenv: cold start, building empty env")
        return self._new_ontoenv(init_from_store=False), False

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
            main_graph = self._source_main_graph()
            closure = Graph()
            for triple in main_graph:
                closure.add(triple)
            # OntoEnv mutates the working graph in place by loading imported
            # ontologies, so keep only the dependency delta in the cache.
            self.env.import_dependencies(closure)
            deps = Graph()
            for triple in closure:
                if triple not in main_graph:
                    deps.add(triple)
            self._dependency_graph_cache = deps
            self._dependency_graph_closure_version = self._closure_version
        _logger.debug("_refresh_dependency_cache: %d dep triples", len(deps))
        return deps

    def _source_graph_with_dependencies(self) -> Graph:
        """Materialize the export view: source graph plus imported triples."""
        merged = Graph()
        for triple in self._source_main_graph():
            merged.add(triple)
        for triple in self._ensure_dependency_cache_current():
            merged.add(triple)
        return merged

    def _imports_union_graph(self) -> Graph:
        return self.query_dataset.graph(self.imports_union_graph_uri)

    def _ensure_imports_union_graph_current(self) -> Graph:
        if (
            self._imports_union_graph_source_version != self._source_version
            or self._imports_union_graph_closure_version != self._closure_version
        ):
            return self._refresh_imports_union_graph()
        return self._imports_union_graph()

    def _refresh_imports_union_graph(self) -> Graph:
        """Materialize data graph + imports closure into one query graph."""
        merged = self._source_graph_with_dependencies()
        graph = self._imports_union_graph()
        graph.remove((None, None, None))
        if len(merged):
            nt = merged.serialize(format="nt", encoding="utf-8")
            self.query_dataset.store._inner.bulk_load(
                input=nt,
                format=RdfFormat.N_TRIPLES,
                to_graph=NamedNode(str(self.imports_union_graph_uri)),
            )
        self._commit_dataset(self.query_dataset)
        self._imports_union_graph_source_version = self._source_version
        self._imports_union_graph_closure_version = self._closure_version
        return graph

    def _apply_main_graph_write(self, incoming: Graph, *, replace: bool) -> Graph:
        """Apply a parsed graph write to the main source graph and return it."""
        main = self._source_main_graph()
        if replace:
            main.remove((None, None, None))
        for triple in incoming:
            main.add(triple)
        # Propagate prefix bindings declared in the incoming Turtle/RDF
        # (rdflib's parser populates incoming.namespace_manager from
        # `@prefix` directives) so they survive into the stores that back
        # the public /namespace/list endpoint.
        for prefix, ns_uri in incoming.namespaces():
            try:
                main.bind(prefix, ns_uri, override=False)
                self.query_dataset.namespace_manager.bind(
                    prefix, ns_uri, override=False
                )
            except Exception:
                _logger.debug(
                    "namespace bind failed for %s=%s", prefix, ns_uri, exc_info=True
                )
        return main

    def _finalize_source_write(self, *, affects_closure: bool) -> None:
        """Persist a source-graph write and refresh only the state it invalidates."""
        self._commit_dataset(self.source_dataset)
        self._mark_source_changed()
        if affects_closure:
            self._mark_closure_changed()
            self._refresh_dependency_cache()

    def refresh_union(self, snapshot_path: str | Path | None = None) -> dict[str, int]:
        """Ensure the dependency closure cache reflects the current source store."""
        with self._lock:
            merged = self._source_graph_with_dependencies()
            if snapshot_path:
                snapshot_path = Path(snapshot_path)
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                merged.serialize(destination=str(snapshot_path), format="turtle")
            return {
                "main_triples": len(self._source_main_graph()),
                "union_triples": len(merged),
            }

    # -------------------- SPARQL surface --------------------
    def sparql_query(self, query: str, use_union: bool = False) -> dict:
        _logger.debug("sparql_query union=%s query=%s", use_union, query)
        # Held across the row materialization too: pyoxigraph's query() returns
        # a lazy iterator, so the rows are still being pulled from the store
        # while `result` is consumed below.
        with self._lock, timed_debug(_logger, "sparql_query union=%s", use_union):
            if use_union:
                with timed_debug(_logger,"sparql_query--ensure imports union graph current"):
                    dataset = self.query_dataset
                    graph_uri = self._ensure_imports_union_graph_current().identifier
            else:
                dataset = self.source_dataset
                graph_uri = self.main_graph_uri
            with timed_debug(_logger,"sparql_query--oxi query time:"):
                result = dataset.store._inner.query(
                    query,
                    use_default_graph_as_union=False,
                    default_graph=ox.NamedNode(str(graph_uri)),
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

    def sparql_update(self, update: str) -> dict:
        _logger.debug("sparql_update: %s", update.replace("\n", " ")[:200])
        with self._lock, timed_debug(_logger, "sparql_update"):
            main = self._source_main_graph()
            main.update(update)
            self._finalize_source_write(affects_closure=True)
        return {"message": "update applied", "changed": True}

    def export_graph(self, *, include_union: bool = True, format: str = "turtle") -> str:
        """Serialize for download: the data-graph closure, or just the data."""
        fmt = (format or "turtle").lower()
        with self._lock:
            graph = self._source_graph_with_dependencies() if include_union else self._source_main_graph()
            return graph.serialize(format=fmt)

    def export_dependency_graph(self, *, format: str = "trig") -> str:
        """Serialize only the imported triples (closure minus instance data)."""
        fmt = (format or "trig").lower()
        with self._lock:
            return self._ensure_dependency_cache_current().serialize(format=fmt)

    def insert_graph(self, content: str | bytes | Graph, *, format: str = "turtle", replace: bool = False) -> dict[str, int | bool]:
        """Parse incoming graph data and merge (or replace) into the main graph.
        format: turtle | n3 | xml | trix
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
            with timed_debug(_logger, "insert_graph merge into main (replace=%s, affects_closure=%s)", replace, affects_closure):
                main = self._apply_main_graph_write(incoming, replace=replace)
                self._finalize_source_write(affects_closure=affects_closure)
            # Counted under the same lock as the write, so the reported totals
            # cannot include a concurrent writer's triples.
            main_triples = len(main)
            union_triples = main_triples + len(self._ensure_dependency_cache_current())
        return {
            "main_triples": main_triples,
            "union_triples": union_triples,
            "replaced": replace,
            "changed": True,
        }

    def namespace_manager(self) -> NamespaceManager :
        return self.query_dataset.namespace_manager
    
    # -------------------- helpers --------------------
    def _materialize_point(self, subject: URIRef) -> Point:
        with self._lock:
            main_graph = self._source_main_graph()
            types = [str(o) for o in main_graph.objects(subject, RDF.type)]
            unit_literal = next(main_graph.objects(subject, QUDT.hasUnit), None)
            last_literal = next(main_graph.objects(subject, LAST_REPORTED), None)
        return Point(
            uri=_external_uri(subject),
            types=types,
            unit=str(unit_literal) if unit_literal else None,
            last_reported=_maybe_literal_dt(last_literal),
        )

    def _source_main_graph(self) -> Graph:
        return self.source_dataset.graph(self.main_graph_uri)

    @staticmethod
    def _commit_dataset(dataset: Dataset) -> None:
        try:
            dataset.commit()
        except Exception:
            # Oxigraph's commit is a no-op but keep for forward compatibility.
            pass

    def close(self) -> None:
        _logger.debug("OxigraphGraphStore.close")
        with self._lock:
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
