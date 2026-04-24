"""
QUDT Unit and QuantityKind extraction, caching, and diff logic.

Parses QUDT ontologies (HTTP-first, local fallback) and stores extracted
concepts as gzipped JSON for change detection across restarts.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger("acquirium.qudt_store")

# RDF predicates we care about
_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_QUDT_UNIT = "http://qudt.org/schema/qudt/Unit"
_QUDT_QK = "http://qudt.org/schema/qudt/QuantityKind"
_QUDT_SYMBOL = "http://qudt.org/schema/qudt/symbol"
_QUDT_UCUM = "http://qudt.org/schema/qudt/ucumCode"

# HTTP sources
_UNIT_HTTP = "http://qudt.org/vocab/unit"
_QK_HTTP = "http://qudt.org/vocab/quantitykind"

# Local fallback paths for QUDT vocabs
_LOCAL_UNIT_PATH = Path("ontologies/qudt_unit.ttl")
_LOCAL_QK_PATH = Path("ontologies/qudt_qk.ttl")


def _split_local_name(uri: str) -> list[str]:
    """Split a URI local name on CamelCase, underscores, hyphens into lowercase tokens."""
    for sep in ("#", "/"):
        if sep in uri:
            local = uri.rsplit(sep, 1)[-1]
            break
    else:
        local = uri
    tokens = re.sub(r"([a-z])([A-Z])", r"\1 \2", local)
    tokens = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", tokens)
    parts = re.split(r"[_\-\s]+", tokens)
    return [p.lower() for p in parts if p]


def _build_surfaces(uri: str, labels: list[str], symbol: str | None, ucum: str | None) -> list[str]:
    """Build the set of surface forms for a single QUDT concept."""
    surfaces: list[str] = []
    seen: set[str] = set()

    def _add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            surfaces.append(s)

    for label in labels:
        _add(label.lower())

    tokens = _split_local_name(uri)
    if tokens:
        _add(" ".join(tokens))

    # Abbreviations kept as-is (case-sensitive matching handled at query time)
    if symbol:
        _add(symbol)
    if ucum:
        _add(ucum)

    return surfaces


def _extract_concepts_from_graph(graph: Any, rdf_type: str) -> list[dict[str, Any]]:
    """Extract QUDT concepts of the given type from any rdflib-compatible graph."""
    from rdflib import URIRef, Namespace
    from rdflib.namespace import SKOS

    QUDT = Namespace("http://qudt.org/schema/qudt/")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    RDF_NS = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

    type_uri = URIRef(rdf_type)
    kind = "unit" if rdf_type == _QUDT_UNIT else "quantity_kind"
    label_preds = [RDFS.label, SKOS.prefLabel, SKOS.altLabel]
    concepts: list[dict[str, Any]] = []

    for subj in graph.subjects(RDF_NS.type, type_uri):
        uri = str(subj)
        labels: list[str] = []
        display_label: str | None = None
        for pred in label_preds:
            for lit in graph.objects(subj, pred):
                lang = getattr(lit, "language", None)
                if lang and not lang.startswith("en"):
                    continue
                text = str(lit)
                if text and text not in labels:
                    labels.append(text)
                if display_label is None:
                    display_label = text

        symbol = next((str(s) for s in graph.objects(subj, QUDT.symbol)), None)
        ucum = next((str(u) for u in graph.objects(subj, QUDT.ucumCode)), None)

        surfaces = _build_surfaces(uri, labels, symbol, ucum)
        if not surfaces:
            continue

        concepts.append({
            "uri": uri,
            "kind": kind,
            "label": display_label or " ".join(_split_local_name(uri)) or uri,
            "surfaces": surfaces,
            "symbol": symbol,
            "ucum": ucum,
        })

    return concepts


class QUDTStore:
    """Extract, cache, and diff QUDT units and quantity kinds."""

    def __init__(self, data_dir: Path, dataset=None) -> None:
        self._cache_dir = data_dir / "qudt_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._units_path = self._cache_dir / "units.json.gz"
        self._qk_path = self._cache_dir / "qk.json.gz"
        self._dataset = dataset

    # ── Parsing ────────────────────────────────────────────────

    @staticmethod
    def _parse_ontology(http_url: str, local_path: Path, rdf_type: str) -> list[dict[str, Any]]:
        """Parse a QUDT ontology, HTTP-first with local fallback. Returns concept dicts."""
        from rdflib import Graph

        g = Graph()
        try:
            logger.info("Fetching QUDT from %s ...", http_url)
            g.parse(http_url, format="turtle")
            logger.info("Loaded QUDT from HTTP (%d triples)", len(g))
        except Exception as e:
            logger.warning("HTTP fetch failed for %s: %s", http_url, e)
            try:
                g.parse(str(local_path), format="turtle")
                logger.info("Loaded QUDT from local file %s (%d triples)", local_path, len(g))
            except OSError:
                logger.warning("No local QUDT file at %s", local_path)
                return []

        return _extract_concepts_from_graph(g, rdf_type)

    @staticmethod
    def _parse_named_graph(dataset, graph_iri: str, rdf_type: str) -> list[dict[str, Any]]:
        """Extract QUDT concepts from a pre-populated Oxigraph named graph."""
        from rdflib import URIRef
        named_graph = dataset.graph(URIRef(graph_iri))
        if not named_graph:
            return []
        return _extract_concepts_from_graph(named_graph, rdf_type)

    # ── Cache I/O ──────────────────────────────────────────────

    @staticmethod
    def _save_gz(path: Path, data: list[dict[str, Any]]) -> None:
        fd, tmp_str = tempfile.mkstemp(dir=path.parent, suffix=".gz.tmp")
        tmp = Path(tmp_str)
        try:
            os.close(fd)
            with gzip.open(tmp, "wt", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=True, sort_keys=True)
            tmp.rename(path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    @staticmethod
    def _load_gz(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    def _load_cached_uris(self) -> set[str]:
        uris: set[str] = set()
        for path in (self._units_path, self._qk_path):
            for c in self._load_gz(path):
                uris.add(c["uri"])
        return uris

    # ── Public API ─────────────────────────────────────────────

    def extract_and_diff(self) -> tuple[list[dict[str, Any]], list[str], bool]:
        """
        Parse QUDT ontologies, diff against cache, update cache.

        Reads from pre-populated Oxigraph named graphs when a dataset is
        available (preferred), falling back to HTTP/local-file parsing.

        Returns:
            (all_concepts, removed_uris, changed)
            - all_concepts: full list of freshly-parsed concept dicts
            - removed_uris: URIs present in old cache but missing from fresh parse
            - changed: True if there were any additions or removals
        """
        if self._dataset is not None:
            units = self._parse_named_graph(self._dataset, _UNIT_HTTP, _QUDT_UNIT)
            qks = self._parse_named_graph(self._dataset, _QK_HTTP, _QUDT_QK)
            if not units and not qks:
                # Named graphs empty (e.g. HTTP failed at startup); fall back
                units = self._parse_ontology(_UNIT_HTTP, _LOCAL_UNIT_PATH, _QUDT_UNIT)
                qks = self._parse_ontology(_QK_HTTP, _LOCAL_QK_PATH, _QUDT_QK)
        else:
            units = self._parse_ontology(_UNIT_HTTP, _LOCAL_UNIT_PATH, _QUDT_UNIT)
            qks = self._parse_ontology(_QK_HTTP, _LOCAL_QK_PATH, _QUDT_QK)
        fresh_concepts = units + qks
        fresh_uris = {c["uri"] for c in fresh_concepts}

        # Load old cache
        old_uris = self._load_cached_uris()

        # Diff
        added_uris = fresh_uris - old_uris
        removed_uris = old_uris - fresh_uris
        changed = bool(added_uris or removed_uris)

        logger.info(
            "QUDT diff: %d total (%d units, %d QKs), +%d added, -%d removed, changed=%s",
            len(fresh_concepts), len(units), len(qks),
            len(added_uris), len(removed_uris), changed,
        )

        # Update cache
        self._save_gz(self._units_path, units)
        self._save_gz(self._qk_path, qks)

        return fresh_concepts, list(removed_uris), changed

    def get_all_concepts(self) -> list[dict[str, Any]]:
        """Load all cached QUDT concepts (units + QKs) from disk."""
        return self._load_gz(self._units_path) + self._load_gz(self._qk_path)

    def has_cache(self) -> bool:
        """Return True if any QUDT cache files exist on disk."""
        return self._units_path.exists() or self._qk_path.exists()
