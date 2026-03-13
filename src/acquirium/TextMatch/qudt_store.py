"""
QUDT Unit and QuantityKind extraction, caching, and diff logic.

Parses QUDT ontologies (HTTP-first, local fallback) and stores extracted
concepts as gzipped JSON for change detection across restarts.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
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


def _build_surfaces(uri: str, label: str | None, symbol: str | None, ucum: str | None) -> list[str]:
    """Build the set of surface forms for a single QUDT concept."""
    surfaces: list[str] = []
    seen: set[str] = set()

    def _add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            surfaces.append(s)

    if label:
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


class QUDTStore:
    """Extract, cache, and diff QUDT units and quantity kinds."""

    def __init__(self, data_dir: Path) -> None:
        self._cache_dir = data_dir / "qudt_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._units_path = self._cache_dir / "units.json.gz"
        self._qk_path = self._cache_dir / "qk.json.gz"

    # ── Parsing ────────────────────────────────────────────────

    @staticmethod
    def _parse_ontology(
        http_url: str,
        local_path: Path,
        rdf_type: str,
    ) -> list[dict[str, Any]]:
        """Parse a QUDT ontology, HTTP-first with local fallback. Returns concept dicts."""
        from rdflib import Graph, URIRef, Namespace

        QUDT = Namespace("http://qudt.org/schema/qudt/")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

        g = Graph()
        loaded = False

        # Try HTTP first
        try:
            logger.info("Fetching QUDT from %s ...", http_url)
            g.parse(http_url, format="turtle")
            loaded = True
            logger.info("Loaded QUDT from HTTP (%d triples)", len(g))
        except Exception as e:
            logger.warning("HTTP fetch failed for %s: %s", http_url, e)

        # Fall back to local file
        if not loaded:
            if local_path.exists():
                logger.info("Loading QUDT from local file %s", local_path)
                g.parse(str(local_path), format="turtle")
                logger.info("Loaded QUDT from local file (%d triples)", len(g))
            else:
                logger.warning("No local QUDT file at %s", local_path)
                return []

        type_uri = URIRef(rdf_type)
        concepts: list[dict[str, Any]] = []

        for subj in g.subjects(RDF.type, type_uri):
            uri = str(subj)

            # Collect labels (may be multi-lang; take first non-empty)
            labels = list(g.objects(subj, RDFS.label))
            label = str(labels[0]) if labels else None

            # Symbol and ucumCode
            symbols = list(g.objects(subj, QUDT.symbol))
            symbol = str(symbols[0]) if symbols else None

            ucums = list(g.objects(subj, QUDT.ucumCode))
            ucum = str(ucums[0]) if ucums else None

            surfaces = _build_surfaces(uri, label, symbol, ucum)
            if not surfaces:
                continue

            kind = "unit" if rdf_type == _QUDT_UNIT else "quantity_kind"
            concepts.append({
                "uri": uri,
                "kind": kind,
                "label": label or " ".join(_split_local_name(uri)) or uri,
                "surfaces": surfaces,
                "symbol": symbol,
                "ucum": ucum,
            })

        return concepts

    # ── Cache I/O ──────────────────────────────────────────────

    @staticmethod
    def _save_gz(path: Path, data: list[dict[str, Any]]) -> None:
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, sort_keys=True)

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

    def extract_and_diff(
        self,
        local_unit_path: Path | None = None,
        local_qk_path: Path | None = None,
    ) -> tuple[list[dict[str, Any]], list[str], bool]:
        """
        Parse QUDT ontologies, diff against cache, update cache.

        Returns:
            (all_concepts, removed_uris, changed)
            - all_concepts: full list of freshly-parsed concept dicts
            - removed_uris: URIs present in old cache but missing from fresh parse
            - changed: True if there were any additions or removals
        """
        # Defaults for local paths
        if local_unit_path is None:
            local_unit_path = Path("ontologies/qudt_unit.ttl")
        if local_qk_path is None:
            local_qk_path = Path("ontologies/qudt_qk.ttl")

        # Parse fresh
        units = self._parse_ontology(_UNIT_HTTP, local_unit_path, _QUDT_UNIT)
        qks = self._parse_ontology(_QK_HTTP, local_qk_path, _QUDT_QK)
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
