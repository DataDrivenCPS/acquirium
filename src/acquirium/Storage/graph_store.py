from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from ontoenv import OntoEnv
from pyoxigraph import NamedNode, RdfFormat
from rdflib import Dataset, Graph, Literal, RDF, URIRef
from rdflib.namespace import XSD
from rdflib.namespace import OWL


## ALL NAMESPACES AND INTERNAL PREDICATES HERE ##
from acquirium.internals.internals_namespaces import *

from acquirium.internals.models import Point, PointCreateRequest
from acquirium.internals.qudt_units import QUDTUnitConverter

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


class _OntoenvOxigraphStore:
    """ontoenv graph-store protocol over the shared Oxigraph dataset.

    Each ontology ontoenv discovers is one Oxigraph named graph keyed by
    its IRI, so ontoenv's graphs and the instance data live in one store
    and ontoenv's own closure tooling runs against Oxigraph. ``on_change``
    is fired on any add/remove so the owner can invalidate its cached
    data-graph closure.
    """

    def __init__(self, dataset: Dataset, on_change: Callable[[], None]) -> None:
        self._ds = dataset
        self._on_change = on_change

    def add_graph(self, iri: str, graph: Graph, overwrite: bool = False) -> None:
        ctx = self._ds.graph(URIRef(iri))
        if len(ctx) and not overwrite:
            return
        ctx.remove((None, None, None))
        # rdflib's per-triple ctx.add() crosses the Rust FFI once per triple
        # — ~76s for the 535k-triple ontoenv crawl. Serialise to N-Triples
        # (rdflib's fastest writer) once and bulk-load through pyoxigraph,
        # which writes straight to SST. ~10× faster on cold startup.
        nt = graph.serialize(format="nt", encoding="utf-8")
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
        self._ds.graph(URIRef(iri)).remove((None, None, None))
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
    """Persistent graph store backed by oxrdflib and managed by ontoenv.

    - Instance data lives in a dedicated named graph (main graph).
    - ontoenv uses the same Oxigraph store (via the graph-store protocol),
      so every discovered ontology is a named graph in one place.
    - "The union" exposed by export/SPARQL is the *data graph's* import
      closure (instance data + its owl:imports), computed by ontoenv and
      cached; the cache is invalidated whenever any graph changes.
    """

    def __init__(
        self,
        *,
        store_path: str | Path,
        env_root: str | Path,
        main_graph_uri: URIRef = DEFAULT_MAIN_GRAPH,
        qudt_converter: QUDTUnitConverter | None = None,
        base_namespace: str | None = None,
        ontologies_dir: str | Path = "ontologies",
    ):
        self.store_path = Path(store_path)
        self.env_root = Path(env_root)
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.env_root.mkdir(parents=True, exist_ok=True)

        self.dataset = Dataset(store="Oxigraph", default_union=False)
        self._open_store()

        self.main_graph_uri = main_graph_uri
        self.qudt_converter = qudt_converter
        self.base_namespace = base_namespace

        # Any graph mutation (ontoenv loading/refreshing ontologies, or an
        # instance insert/update) bumps this; the cached data-graph closure
        # is rebuilt only when its build version is stale.
        self._graph_version = 0
        self._closure_cache: tuple[int, Graph] | None = None

        # ontoenv shares this Oxigraph store via the graph-store protocol,
        # so its graphs and the instance data live together and ontoenv's
        # closure tooling runs over Oxigraph. Its .ontoenv metadata dir
        # goes under the configured env_root (not the cwd). graph_store is
        # incompatible with recreate/create_or_use_cached, so neither is
        # passed. Every vocabulary (incl. s223, vendored as
        # ontologies/s223.ttl) is a local file discovered by the directory
        # crawl — no remote fetch, then pulled out by IRI via
        # ontology_iris()/named_graph().
        ont_dir = Path(ontologies_dir)
        search_dirs = [str(ont_dir)] if ont_dir.is_dir() else []
        self._ontoenv_store = _OntoenvOxigraphStore(
            self.dataset, self._invalidate_closure
        )
        self.env = OntoEnv(
            path=str(self.env_root),
            graph_store=self._ontoenv_store,
            search_directories=search_dirs,
        )
        try:
            self.env.update()
        except Exception as exc:
            _logger.warning("ontoenv: directory crawl failed: %s", exc)
        self._commit()

    # -------------------- ontoenv named-graph access --------------------
    def named_graph(self, iri: str) -> Graph:
        """One ontology's own graph from ontoenv (owl:imports NOT followed)."""
        return self.env.get_graph(iri)

    # Default exact ontology IRIs declared by the vendored files in
    # `ontologies/` (qudt_unit.ttl pins QUDT 3.1.5; qudt_qk.ttl uses the
    # version-agnostic quantitykind IRI). Exact match avoids the QUDT
    # version sprawl ontoenv pulls in via owl:imports. Overridable via the
    # acquirium.toml `[ontologies]` table (cli.py -> ACQUIRIUM_ONTOLOGY_IRIS).
    _ONTOLOGY_IRIS = {
        "water": "urn:nawi-water-ontology",
        "s223": "http://data.ashrae.org/standard223/1.0/model/all",
        "unit": "http://qudt.org/3.1.5/vocab/unit",
        "quantity_kind": "http://qudt.org/vocab/quantitykind",
    }

    @classmethod
    def _resolve_iri_map(cls) -> dict[str, str]:
        """The configured logical-name -> IRI map (env override or default)."""
        raw = os.getenv("ACQUIRIUM_ONTOLOGY_IRIS")
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict) and data:
                    return {str(k): str(v) for k, v in data.items()}
                _logger.warning(
                    "ACQUIRIUM_ONTOLOGY_IRIS not a non-empty object; using defaults"
                )
            except ValueError as exc:
                _logger.warning(
                    "ACQUIRIUM_ONTOLOGY_IRIS invalid JSON (%s); using defaults", exc
                )
        return dict(cls._ONTOLOGY_IRIS)

    def ontology_iris(self) -> dict[str, str]:
        """Logical name -> ontology IRI for the ontologies ontoenv discovered.

        Keys: ``water``, ``s223``, ``unit``, ``quantity_kind`` (a key is
        absent if ontoenv did not register that exact IRI).
        """
        try:
            names = set(self.env.get_ontology_names())
        except Exception as exc:
            _logger.warning("ontoenv: get_ontology_names failed: %s", exc)
            return {}
        return {
            name: iri
            for name, iri in self._resolve_iri_map().items()
            if iri in names
        }

    def qualify_uri(self, value: str) -> str:
        if "://" in value or value.startswith("urn:"):
            return value
        base = self.base_namespace
        if base:
            if not base.endswith("/") and not base.endswith("#"):
                base = base + "/"
            return base + value.lstrip("/")
        return str(ACQUIRIUM_POINT_NS[value])

    def _uri(self, value: str) -> URIRef:
        return URIRef(self.qualify_uri(value))

    # -------------------- dependency + union management --------------------
    def register_ontology(self, source: str) -> str:
        """Add an ontology source (IRI or path) to the ontoenv environment."""
        name = self.env.add(source, fetch_imports=False)
        return name

    def ensure_ontology_root(self, graph_iri: str, imports: list[str]) -> None:
        """Ensure an owl:Ontology root node with optional owl:imports declarations."""
        main_graph = self._main_graph()
        root = URIRef(graph_iri)
        if (root, RDF.type, OWL.Ontology) not in main_graph:
            main_graph.add((root, RDF.type, OWL.Ontology))
        for dep in imports:
            main_graph.add((root, OWL.imports, URIRef(dep)))
        self._commit()

    # -------------------- cached data-graph closure --------------------
    def _invalidate_closure(self) -> None:
        """Mark the cached data-graph closure stale (any graph changed)."""
        self._graph_version += 1
        self._closure_cache = None

    def _data_closure(self) -> Graph:
        """Instance data + its ontoenv-resolved owl:imports closure.

        Computed by ontoenv over the shared Oxigraph store and cached;
        rebuilt only when a graph changed since the cached build (cheap
        no-op otherwise — this is the whole point of the version guard).
        Returned as a plain rdflib Graph, so it serialises to turtle
        (the Oxigraph-backed Dataset serialiser does not).
        """
        if self._closure_cache and self._closure_cache[0] == self._graph_version:
            return self._closure_cache[1]
        # import_dependencies merges the data graph's owl:imports closure
        # *into* the graph (data + imports). get_dependencies returns only
        # the imported triples, so no-imports data would yield an empty
        # closure — which is wrong here.
        closure = Graph()
        for triple in self._main_graph():
            closure.add(triple)
        self.env.import_dependencies(closure)
        self._closure_cache = (self._graph_version, closure)
        return closure

    def refresh_union(self, snapshot_path: str | Path | None = None) -> dict[str, int]:
        """Invalidate and rebuild the cached data-graph closure.

        Kept for API compatibility. The union is the data graph's import
        closure (instance data + owl:imports), not a materialised copy of
        every ontology ontoenv discovered. Optionally writes a snapshot.
        """
        self._invalidate_closure()
        closure = self._data_closure()
        if snapshot_path:
            snapshot_path = Path(snapshot_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            closure.serialize(destination=str(snapshot_path), format="turtle")
        return {
            "main_triples": len(self._main_graph()),
            "union_triples": len(closure),
        }

    # -------------------- SPARQL surface --------------------
    def sparql_query(self, query: str, use_union: bool = False) -> dict:
        graph = self._data_closure() if use_union else self._main_graph()
        results = graph.query(query)
        cols = results.vars
        rows = [[cell for cell in row] for row in results]
        return {"columns": [str(c) for c in cols], "rows": rows}

    def sparql_update(self, update: str) -> dict:
        self._main_graph().update(update)
        self._commit()
        self._invalidate_closure()
        return {"message": "update applied"}

    def export_graph(self, *, include_union: bool = True, format: str = "turtle") -> str:
        """Serialize for download: the data-graph closure, or just the data."""
        fmt = (format or "turtle").lower()
        graph = self._data_closure() if include_union else self._main_graph()
        return graph.serialize(format=fmt)

    def export_dependency_graph(self, *, format: str = "trig") -> str:
        """Serialize only the imported triples (closure minus instance data)."""
        fmt = (format or "trig").lower()
        main = self._main_graph()
        merged = Graph()
        for triple in self._data_closure():
            if triple not in main:
                merged.add(triple)
        return merged.serialize(format=fmt)

    def insert_graph(self, content: str | bytes | Graph, *, format: str = "turtle", replace: bool = False) -> dict[str, int]:
        """Parse incoming graph data and merge (or replace) into the main graph.
        format: turtle | n3 | xml | trix
        """

        fmt = (format or "turtle").lower()
        incoming = Graph()
        if isinstance(content, Graph):
            incoming = content
        else:
            incoming.parse(data=content, format=fmt)

        main = self._main_graph()
        if replace:
            main.remove((None, None, None))
        for triple in incoming:
            main.add(triple)
        self._commit()
        self._invalidate_closure()
        return {
            "main_triples": len(main),
            "union_triples": len(self._data_closure()),
            "replaced": replace,
        }

    # -------------------- helpers --------------------
    def _materialize_point(self, subject: URIRef) -> Point:
        main_graph = self._main_graph()
        types = [str(o) for o in main_graph.objects(subject, RDF.type)]
        unit_literal = next(main_graph.objects(subject, QUDT.hasUnit), None)
        last_literal = next(main_graph.objects(subject, LAST_REPORTED), None)
        return Point(
            uri=_external_uri(subject),
            types=types,
            unit=str(unit_literal) if unit_literal else None,
            last_reported=_maybe_literal_dt(last_literal),
        )

    def _main_graph(self) -> Graph:
        return self.dataset.graph(self.main_graph_uri)

    def _commit(self) -> None:
        try:
            self.dataset.commit()
        except Exception:
            # Oxigraph's commit is a no-op but keep for forward compatibility.
            pass

    def close(self) -> None:
        try:
            self.dataset.close()
        except Exception:
            pass

    # -------------------- internal: store bootstrap --------------------
    def _open_store(self) -> None:
        """Open the Oxigraph-backed Dataset, clearing stale locks and falling back to temp if needed."""

        def try_open(path: Path) -> None:
            self.dataset.open(str(path))

        try:
            try_open(self.store_path)
            return
        except OSError as exc:  # pragma: no cover - depends on fs state
            if "LOCK" in str(exc) or "No locks available" in str(exc):
                lock_file = self.store_path / "LOCK"
                if lock_file.exists():
                    lock_file.unlink(missing_ok=True)
                try:
                    try_open(self.store_path)
                    return
                except OSError:
                    # fall through to temp fallback
                    pass
            # As a last resort (e.g., sandbox disallows file locking), use a temp store.
            import tempfile

            tmp_dir = Path(tempfile.mkdtemp(prefix="oxigraph-store-"))
            self.store_path = tmp_dir
            self.dataset = Dataset(store="Oxigraph", default_union=False)
            try_open(self.store_path)
