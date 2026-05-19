"""QUDT Unit / QuantityKind concept extraction.

Pure extractor: given an ``rdflib.Graph`` (read out of ontoenv) and an RDF
type, return concept dicts (uri, kind, label, surfaces, symbol, ucum,
related) for the embedding index. The graphs are sourced and cached by
ontoenv; this module does no parsing, fetching, or disk caching.
"""

from __future__ import annotations

import logging
from typing import Any

from rdflib import Graph, URIRef
from rdflib.namespace import SKOS

from acquirium.internals.internals_namespaces import *  # noqa: F403
from acquirium.TextMatch.embedding_matcher import _split_local_name

logger = logging.getLogger("acquirium.qudt_store")


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

    # Symbol / UCUM code as surfaces; the matcher's exact stage normalizes
    # case and whitespace so "kg", "KG", "mg/L" match without embeddings.
    if symbol:
        _add(symbol)
    if ucum:
        _add(ucum)

    return surfaces


class QUDTStore:
    """Extract QUDT unit / quantity-kind concepts from a given graph."""

    @staticmethod
    def extract_concepts(graph: Graph, rdf_type: str) -> list[dict[str, Any]]:
        """Concept dicts for every ``rdf_type`` subject in *graph*.

        ``related`` captures the cross-reference used by joint/context
        rerank: a unit's ``qudt:hasQuantityKind``, a quantity kind's
        ``qudt:applicableUnit``.
        """
        type_uri = URIRef(rdf_type)
        is_unit = rdf_type == str(QUDT.Unit)  # noqa: F405
        label_preds = [RDFS.label, SKOS.prefLabel, SKOS.altLabel]  # noqa: F405
        relation_preds = (
            [QUDT.hasQuantityKind] if is_unit else [QUDT.applicableUnit]  # noqa: F405
        )

        concepts: list[dict[str, Any]] = []
        for subj in graph.subjects(RDF.type, type_uri):  # noqa: F405
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

            symbols = list(graph.objects(subj, QUDT.symbol))  # noqa: F405
            symbol = str(symbols[0]) if symbols else None
            ucums = list(graph.objects(subj, QUDT.ucumCode))  # noqa: F405
            ucum = str(ucums[0]) if ucums else None

            surfaces = _build_surfaces(uri, labels, symbol, ucum)
            if not surfaces:
                continue

            related = sorted(
                {
                    str(obj)
                    for pred in relation_preds
                    for obj in graph.objects(subj, pred)
                }
            )
            concepts.append({
                "uri": uri,
                "kind": "unit" if is_unit else "quantity_kind",
                "label": display_label or " ".join(_split_local_name(uri)) or uri,
                "surfaces": surfaces,
                "symbol": symbol,
                "ucum": ucum,
                "related": related,
            })
        return concepts
