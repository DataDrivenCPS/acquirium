from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ontoenv import OntoEnv
from rdflib import Dataset, Graph, Literal, RDF, URIRef
from rdflib.namespace import XSD
from rdflib.namespace import OWL


## ALL NAMESPACES AND INTERNAL PREDICATES HERE ##
from acquirium.internals.internals_namespaces import *

from acquirium.internals.models import Point, PointCreateRequest
from acquirium.internals.qudt_units import QUDTUnitConverter, UnitNotFound



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
    ):
        self.store_path = Path(store_path)
        self.env_root = Path(env_root)
        self.store_path.mkdir(parents=True, exist_ok=True)

        self.dataset = Dataset(store="Oxigraph", default_union=False)
        self._open_store()

        # OntoEnv expects the *root* directory; it manages .ontoenv within it.
        env_exists = (self.env_root / ".ontoenv").exists()
        try:
            self.env = OntoEnv(path=str(self.env_root), recreate=not env_exists)
        except ValueError:
            # First run or missing config; recreate to initialize metadata.
            self.env = OntoEnv(path=str(self.env_root), recreate=True)

        self.main_graph_uri = main_graph_uri
        self.union_graph_uri = union_graph_uri
        self.include_dependency_graphs = include_dependency_graphs
        self.qudt_converter = qudt_converter
        self.base_namespace = base_namespace

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

    # -------------------- point catalog --------------------
    def upsert_node(self, payload: PointCreateRequest) -> Point:
        subject = self._uri(payload.uri)
        main_graph = self._main_graph()
        union_graph = self._union_graph()

        # Ensure the point exists as a Stream node for dependency reasoning
        main_graph.add((subject, RDF.type, STREAM))
        union_graph.add((subject, RDF.type, STREAM))

        for rdf_type in payload.types:
            t_uri = self._uri(rdf_type)
            main_graph.add((subject, RDF.type, t_uri))
            union_graph.add((subject, RDF.type, t_uri))

        if payload.unit:
            unit_value = self._normalize_unit(payload.unit)
            if unit_value is not None:
                main_graph.set((subject, QUDT.hasUnit, unit_value))
                union_graph.set((subject, QUDT.hasUnit, unit_value))

        self._commit()
        return self._materialize_point(subject)

    def register_virtual_point(self, uri: str, sources: list[str]) -> Point:
        payload = PointCreateRequest(uri=uri, types=[str(VIRTUAL_POINT)])
        point = self.upsert_point(payload)
        subject = self._uri(uri)
        main_graph = self._main_graph()
        union_graph = self._union_graph()
        main_graph.add((subject, RDF.type, STREAM))
        union_graph.add((subject, RDF.type, STREAM))
        for src in sources:
            src_uri = self._uri(src)
            main_graph.add((subject, IS_CALCULATED_FROM, src_uri))
            union_graph.add((subject, IS_CALCULATED_FROM, src_uri))
        self._commit()
        return point

    def get_point(self, uri: str) -> Point | None:
        subject = self._uri(uri)
        main_graph = self._main_graph()
        if (subject, None, None) not in main_graph:
            return None
        return self._materialize_point(subject)

    def list_points(self) -> list[Point]:
        main_graph = self._main_graph()
        subjects = {s for s, _, _ in main_graph}
        return [self._materialize_point(s) for s in subjects]

    def points_by_type(self, rdf_class: str) -> list[Point]:
        class_uri = self._uri(rdf_class)
        main_graph = self._main_graph()
        subjects = {s for s, _, _ in main_graph.triples((None, RDF.type, class_uri))}
        return [self._materialize_point(s) for s in subjects]

    def record_last_reported(self, uri: str, ts: datetime) -> None:
        subject = self._uri(uri)
        main_graph = self._main_graph()
        if (subject, None, None) not in main_graph:
            return
        literal = _literal_dt(ts)
        change_literal = _literal_dt(datetime.now(timezone.utc))
        main_graph.set((subject, LAST_REPORTED, literal))
        main_graph.set((subject, LAST_INPUT_CHANGE, change_literal))
        self._union_graph().set((subject, LAST_REPORTED, literal))
        self._union_graph().set((subject, LAST_INPUT_CHANGE, change_literal))
        self._commit()

    def get_last_reported(self, uri: str) -> datetime | None:
        subject = self._uri(uri)
        literal = next(self._main_graph().objects(subject, LAST_REPORTED), None)
        return _maybe_literal_dt(literal)

    def get_last_input_change(self, uri: str) -> datetime | None:
        subject = self._uri(uri)
        literal = next(self._main_graph().objects(subject, LAST_INPUT_CHANGE), None)
        return _maybe_literal_dt(literal)

    def set_last_run(self, uri: str, ts: datetime) -> None:
        subject = self._uri(uri)
        literal = _literal_dt(ts)
        main_graph = self._main_graph()
        if (subject, None, None) not in main_graph:
            main_graph.add((subject, RDF.type, STREAM))
        main_graph.set((subject, LAST_RUN, literal))
        self._union_graph().set((subject, LAST_RUN, literal))
        self._commit()

    def get_last_run(self, uri: str) -> datetime | None:
        subject = self._uri(uri)
        literal = next(self._main_graph().objects(subject, LAST_RUN), None)
        return _maybe_literal_dt(literal)

    def get_soft_sensor_version(self, uri: str) -> str | None:
        subject = self._uri(uri)
        literal = next(self._main_graph().objects(subject, HAS_VERSION), None)
        return str(literal) if literal is not None else None

    def upsert_soft_sensor_metadata(self, uri: str, sources: list[str], module_path: str, version: str) -> None:
        subject = self._uri(uri)
        main_graph = self._main_graph()
        union_graph = self._union_graph()
        # basic typing
        main_graph.set((subject, RDF.type, SOFT_SENSOR))
        main_graph.add((subject, RDF.type, STREAM))
        union_graph.set((subject, RDF.type, SOFT_SENSOR))
        union_graph.add((subject, RDF.type, STREAM))
        # link produced stream to the sensor (sensor produces its own output point)
        main_graph.set((subject, PRODUCES, subject))
        union_graph.set((subject, PRODUCES, subject))
        # replace dependsOn edges
        main_graph.remove((subject, DEPENDS_ON, None))
        union_graph.remove((subject, DEPENDS_ON, None))
        for src in sources:
            src_uri = self._uri(src)
            main_graph.add((subject, DEPENDS_ON, src_uri))
            union_graph.add((subject, DEPENDS_ON, src_uri))
        main_graph.set((subject, HAS_MODULE, Literal(module_path)))
        main_graph.set((subject, HAS_VERSION, Literal(version)))
        union_graph.set((subject, HAS_MODULE, Literal(module_path)))
        union_graph.set((subject, HAS_VERSION, Literal(version)))
        self._commit()

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
                try:
                    closure_graph, _ = self.env.get_closure(ont)
                except ValueError:
                    closure_graph = self.env.get_closure(ont)
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

    def insert_graph(self, content: str | bytes, *, format: str = "turtle", replace: bool = False) -> dict[str, int]:
        """Parse incoming graph data and merge (or replace) into the main graph, then refresh union.
        format: turtle | n3 | xml | trix
        """

        fmt = (format or "turtle").lower()
        incoming = Graph()
        incoming.parse(data=content, format=fmt)

        main = self._main_graph()
        if replace:
            main.remove((None, None, None))
        for triple in incoming:
            main.add(triple)

        # refresh union to pick up new data + imports
        counts = self.refresh_union()
        return {"main_triples": len(main), "union_triples": counts["union_triples"], "replaced": replace}

    def delete_point(self, uri: str) -> None:
        """Remove a point (typically a virtual soft sensor) from the store."""
        subject = self._uri(uri)
        main = self._main_graph()
        union = self._union_graph()
        main.remove((subject, None, None))
        main.remove((None, None, subject))
        union.remove((subject, None, None))
        union.remove((None, None, subject))
        self._commit()

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

    def _normalize_unit(self, unit_str: str) -> URIRef | Literal:
        if self.qudt_converter is None:
            return Literal(unit_str)
        try:
            unit_def = self.qudt_converter.infer_unit(unit_str)
            return unit_def.uri
        except Exception:
            return Literal(unit_str)

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
