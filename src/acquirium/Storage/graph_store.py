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
from rdflib.compare import to_isomorphic
from rdflib.namespace import XSD
from rdflib.namespace import OWL


## ALL NAMESPACES AND INTERNAL PREDICATES HERE ##
from acquirium.internals.internals_namespaces import *

from acquirium.internals.models import Point, PointCreateRequest
from acquirium.internals.qudt_units import QUDTUnitConverter

_logger = logging.getLogger("acquirium.graph_store")


def _graph_digest(graph: Graph) -> int:
    """Stable digest for graph content, insensitive to blank-node renaming."""
    return to_isomorphic(graph).graph_digest()


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
        if len(ctx) and overwrite and _graph_digest(ctx) == _graph_digest(graph):
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
    """Authoritative ontoenv source store plus a derived Oxigraph query cache."""

    _UNION_GRAPH_URI = URIRef("urn:acquirium:internal:union")

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
        self.source_store_path = self.store_path / "source"
        self.query_store_path = self.store_path / "query"
        self.source_store_path.mkdir(parents=True, exist_ok=True)
        self.query_store_path.mkdir(parents=True, exist_ok=True)

        self.source_dataset, self.source_store_path = self._open_dataset(self.source_store_path)
        self.query_dataset, self.query_store_path = self._open_dataset(self.query_store_path)

        self.main_graph_uri = main_graph_uri
        self.qudt_converter = qudt_converter
        self.base_namespace = base_namespace
        self._source_version = self._load_source_version()
        self._query_source_version = self._load_query_source_version()

        # ontoenv shares this Oxigraph store via the graph-store protocol,
        # but only over the authoritative source dataset. The query dataset
        # is a derived materialization used only for SPARQL/export reads.
        ont_dir = Path(ontologies_dir)
        search_dirs = [str(ont_dir)] if ont_dir.is_dir() else []
        self._ontoenv_store = _OntoenvOxigraphStore(
            self.source_dataset,
            self._mark_source_changed,
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
        self._commit_dataset(self.source_dataset)
        self._ensure_query_cache_current()

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
        return self.env.add(source, fetch_imports=False)

    def ensure_ontology_root(self, graph_iri: str, imports: list[str]) -> None:
        """Ensure an owl:Ontology root node with optional owl:imports declarations."""
        main_graph = self._source_main_graph()
        root = URIRef(graph_iri)
        changed = False
        if (root, RDF.type, OWL.Ontology) not in main_graph:
            main_graph.add((root, RDF.type, OWL.Ontology))
            changed = True
        for dep in imports:
            triple = (root, OWL.imports, URIRef(dep))
            if triple in main_graph:
                continue
            main_graph.add(triple)
            changed = True
        if not changed:
            return
        self._commit_dataset(self.source_dataset)
        self._mark_source_changed()
        self._refresh_query_cache()

    # -------------------- source/query cache coordination --------------------
    def _source_state_path(self) -> Path:
        return self.source_store_path / "acquirium_source_state.json"

    def _query_state_path(self) -> Path:
        return self.query_store_path / "acquirium_query_state.json"

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

    def _load_query_source_version(self) -> int:
        path = self._query_state_path()
        if not path.exists():
            return -1
        try:
            raw = json.loads(path.read_text())
        except (OSError, ValueError):
            return -1
        value = raw.get("source_version") if isinstance(raw, dict) else -1
        return int(value) if isinstance(value, int) else -1

    def _write_query_source_version(self) -> None:
        self._query_state_path().write_text(
            json.dumps({"source_version": self._query_source_version})
        )

    def _ensure_query_cache_current(self) -> Graph:
        if self._query_source_version != self._source_version:
            return self._refresh_query_cache()
        return self._query_union_graph()

    def _refresh_query_cache(self) -> Graph:
        main_graph = self._source_main_graph()
        closure = Graph()
        for triple in main_graph:
            closure.add(triple)
        self.env.import_dependencies(closure)
        self._clear_query_cache()
        self._bulk_load_query_graph(self.main_graph_uri, main_graph)
        self._bulk_load_query_graph(self._UNION_GRAPH_URI, closure)
        self._commit_dataset(self.query_dataset)
        self._query_source_version = self._source_version
        self._write_query_source_version()
        return closure

    def _clear_query_cache(self) -> None:
        self.query_dataset.store._inner.clear()

    def refresh_union(self, snapshot_path: str | Path | None = None) -> dict[str, int]:
        """Ensure the query cache reflects the current ontoenv source store."""
        closure = self._ensure_query_cache_current()
        if snapshot_path:
            snapshot_path = Path(snapshot_path)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            closure.serialize(destination=str(snapshot_path), format="turtle")
        return {
            "main_triples": len(self._query_main_graph()),
            "union_triples": len(closure),
        }

    # -------------------- SPARQL surface --------------------
    def sparql_query(self, query: str, use_union: bool = False) -> dict:
        self._ensure_query_cache_current()
        graph = self._query_union_graph() if use_union else self._query_main_graph()
        results = graph.query(query)
        cols = results.vars
        rows = [[cell for cell in row] for row in results]
        return {"columns": [str(c) for c in cols], "rows": rows}

    def sparql_update(self, update: str) -> dict:
        main = self._source_main_graph()
        before = _graph_digest(main)
        main.update(update)
        self._commit_dataset(self.source_dataset)
        if _graph_digest(main) == before:
            return {"message": "update applied", "changed": False}
        self._mark_source_changed()
        self._refresh_query_cache()
        return {"message": "update applied", "changed": True}

    def export_graph(self, *, include_union: bool = True, format: str = "turtle") -> str:
        """Serialize for download: the data-graph closure, or just the data."""
        fmt = (format or "turtle").lower()
        self._ensure_query_cache_current()
        graph = self._query_union_graph() if include_union else self._query_main_graph()
        return graph.serialize(format=fmt)

    def export_dependency_graph(self, *, format: str = "trig") -> str:
        """Serialize only the imported triples (closure minus instance data)."""
        fmt = (format or "trig").lower()
        self._ensure_query_cache_current()
        main = self._query_main_graph()
        merged = Graph()
        for triple in self._query_union_graph():
            if triple not in main:
                merged.add(triple)
        return merged.serialize(format=fmt)

    def insert_graph(self, content: str | bytes | Graph, *, format: str = "turtle", replace: bool = False) -> dict[str, int | bool]:
        """Parse incoming graph data and merge (or replace) into the main graph.
        format: turtle | n3 | xml | trix
        """

        fmt = (format or "turtle").lower()
        incoming = Graph()
        if isinstance(content, Graph):
            incoming = content
        else:
            incoming.parse(data=content, format=fmt)

        main = self._source_main_graph()
        if replace:
            changed = _graph_digest(main) != _graph_digest(incoming)
            if not changed:
                return {
                    "main_triples": len(main),
                    "union_triples": len(self._ensure_query_cache_current()),
                    "replaced": replace,
                    "changed": False,
                }
            main.remove((None, None, None))
            for triple in incoming:
                main.add(triple)
        else:
            changed_count = 0
            for triple in incoming:
                if triple in main:
                    continue
                main.add(triple)
                changed_count += 1
            if changed_count == 0:
                return {
                    "main_triples": len(main),
                    "union_triples": len(self._ensure_query_cache_current()),
                    "replaced": replace,
                    "changed": False,
                }
        self._commit_dataset(self.source_dataset)
        self._mark_source_changed()
        closure = self._refresh_query_cache()
        return {
            "main_triples": len(main),
            "union_triples": len(closure),
            "replaced": replace,
            "changed": True,
        }

    # -------------------- helpers --------------------
    def _materialize_point(self, subject: URIRef) -> Point:
        self._ensure_query_cache_current()
        main_graph = self._query_main_graph()
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

    def _query_main_graph(self) -> Graph:
        return self.query_dataset.graph(self.main_graph_uri)

    def _query_union_graph(self) -> Graph:
        return self.query_dataset.graph(self._UNION_GRAPH_URI)

    def _bulk_load_query_graph(self, iri: URIRef, graph: Graph) -> None:
        if len(graph) == 0:
            return
        nt = graph.serialize(format="nt", encoding="utf-8")
        self.query_dataset.store._inner.bulk_load(
            input=nt,
            format=RdfFormat.N_TRIPLES,
            to_graph=NamedNode(str(iri)),
        )

    @staticmethod
    def _commit_dataset(dataset: Dataset) -> None:
        try:
            dataset.commit()
        except Exception:
            # Oxigraph's commit is a no-op but keep for forward compatibility.
            pass

    def close(self) -> None:
        try:
            self.source_dataset.close()
        except Exception:
            pass
        try:
            self.query_dataset.close()
        except Exception:
            pass

    # -------------------- internal: store bootstrap --------------------
    @staticmethod
    def _open_dataset(path: Path) -> tuple[Dataset, Path]:
        """Open an Oxigraph-backed Dataset, falling back to temp if needed."""

        dataset = Dataset(store="Oxigraph", default_union=False)

        def try_open(target: Path) -> None:
            dataset.open(str(target))

        try:
            try_open(path)
            return dataset, path
        except OSError as exc:  # pragma: no cover - depends on fs state
            if "LOCK" in str(exc) or "No locks available" in str(exc):
                lock_file = path / "LOCK"
                if lock_file.exists():
                    lock_file.unlink(missing_ok=True)
                try:
                    try_open(path)
                    return dataset, path
                except OSError:
                    # fall through to temp fallback
                    pass
            # As a last resort (e.g., sandbox disallows file locking), use a temp store.
            import tempfile

            tmp_dir = Path(tempfile.mkdtemp(prefix="oxigraph-store-"))
            dataset = Dataset(store="Oxigraph", default_union=False)
            try_open(tmp_dir)
            return dataset, tmp_dir
