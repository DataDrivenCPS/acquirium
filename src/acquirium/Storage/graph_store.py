from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from ontoenv import OntoEnv
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


class OxigraphGraphStore:
    """Persistent graph store backed by oxrdflib and managed by ontoenv.

    - Instance data lives in a dedicated named graph (main graph).
    - A materialized union graph is kept in-store for query speed and backups.
    - ontoenv computes import closures; closures are merged into the union graph
      and can be stored in per-ontology named graphs for traceability.
    """

    def __init__(
        self,
        *,
        store_path: str | Path,
        env_root: str | Path,
        main_graph_uri: URIRef = DEFAULT_MAIN_GRAPH,
        union_graph_uri: URIRef = DEFAULT_UNION_GRAPH,
        include_dependency_graphs: bool = True,
        qudt_converter: QUDTUnitConverter | None = None,
        base_namespace: str | None = None,
        ontologies_dir: str | Path = "ontologies",
    ):
        self.store_path = Path(store_path)
        self.env_root = Path(env_root)
        self.store_path.mkdir(parents=True, exist_ok=True)

        self.dataset = Dataset(store="Oxigraph", default_union=False)
        self._open_store()

        # ontoenv is the single source of ontology graphs: it discovers the
        # local `ontologies/` directory; s223 is not a local file so it is
        # added once from the canonical URL. Graphs are read out per-IRI via
        # named_graph()/ontology_iris() (imports NOT followed).
        ont_dir = Path(ontologies_dir)
        search_dirs = [str(ont_dir)] if ont_dir.is_dir() else []
        env_exists = (self.env_root / ".ontoenv").exists()
        try:
            self.env = OntoEnv(
                path=str(self.env_root),
                recreate=not env_exists,
                search_directories=search_dirs,
            )
        except ValueError:
            self.env = OntoEnv(
                path=str(self.env_root),
                recreate=True,
                search_directories=search_dirs,
            )
        try:
            self.env.update()
        except Exception as exc:
            _logger.warning("ontoenv: directory crawl failed: %s", exc)
        try:
            self.env.add("https://open223.info/223p.ttl", fetch_imports=False)
        except Exception as exc:
            _logger.warning("ontoenv: could not add s223 (open223.info): %s", exc)

        self.main_graph_uri = main_graph_uri
        self.union_graph_uri = union_graph_uri
        self.include_dependency_graphs = include_dependency_graphs
        self.qudt_converter = qudt_converter
        self.base_namespace = base_namespace

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

    def refresh_union(self, snapshot_path: str | Path | None = None) -> dict[str, int]:
        """Rebuild the union graph from main graph + import closure.

        - Uses ontoenv to resolve owl:imports and transitively load dependencies.
        - Optionally writes a serialized snapshot for backup/validation.
        Returns counts for basic observability.
        """

        union_graph = self._union_graph(clear=True)
        main_graph = self._main_graph()

        # Pull imports transitively using ontoenv; keeps work invisible to callers.
        closure = Graph()
        for triple in main_graph:
            closure.add(triple)
        self.env.import_dependencies(closure)
        for triple in closure:
            union_graph.add(triple)

        # Optionally persist per-ontology graphs for traceability.
        if self.include_dependency_graphs:
            ontology_names = (
                self.env.get_ontology_names()
                if hasattr(self.env, "get_ontology_names")
                else self.env.list()
            )
            for ont in ontology_names:
                closure_graph, _ = self.env.get_closure(ont)
                ctx = self.dataset.graph(URIRef(ont))
                ctx.remove((None, None, None))
                for triple in closure_graph:
                    ctx.add(triple)
                for triple in closure_graph:
                    union_graph.add(triple)

        if snapshot_path:
            snapshot_path = Path(snapshot_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            fmt = "ox-trig" if snapshot_path.suffix.endswith("trig") else "trig"
            union_graph.serialize(destination=str(snapshot_path), format=fmt)

        self._commit()
        return {"main_triples": len(main_graph), "union_triples": len(union_graph)}

    # -------------------- SPARQL surface --------------------
    def sparql_query(self, query: str, use_union: bool = False) -> dict:
        graph = self._union_graph() if use_union else self._main_graph()
        results = graph.query(query)
        cols = results.vars
        rows = [[cell for cell in row] for row in results]
        return {"columns": [str(c) for c in cols], "rows": rows}

    def sparql_update(self, update: str) -> dict:
        # Apply to main (source of truth) and mirror into union.
        self._main_graph().update(update)
        self._union_graph().update(update)
        self._commit()
        return {"message": "update applied"}

    def export_graph(self, *, include_union: bool = True, format: str = "turtle") -> str:
        """Serialize the graph for download. Refreshes the union graph if requested."""
        fmt = (format or "turtle").lower()
        if include_union:
            # Ensure union graph includes latest main graph + imports before serialization.
            self.refresh_union()
            graph = self._union_graph()
        else:
            graph = self._main_graph()
        return graph.serialize(format=fmt)

    def export_dependency_graph(self, *, format: str = "trig") -> str:
        """Serialize the merged dependency graphs (import closures) only."""
        fmt = (format or "trig").lower()
        merged = Graph()
        for ctx in self.dataset.contexts():
            if ctx.identifier in {self.main_graph_uri, self.union_graph_uri}:
                continue
            for triple in ctx:
                merged.add(triple)
        return merged.serialize(format=fmt)

    def insert_graph(self, content: str | bytes | Graph, *, format: str = "turtle", replace: bool = False) -> dict[str, int]:
        """Parse incoming graph data and merge (or replace) into the main graph, then refresh union.
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

        # refresh union to pick up new data + imports
        counts = self.refresh_union()
        return {"main_triples": len(main), "union_triples": counts["union_triples"], "replaced": replace}

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

    def _union_graph(self, *, clear: bool = False) -> Graph:
        graph = self.dataset.graph(self.union_graph_uri)
        if clear:
            graph.remove((None, None, None))
        return graph

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
