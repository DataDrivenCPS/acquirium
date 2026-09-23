"""QUDT Unit / QuantityKind concept extraction.

Pure extractor: given the rows of :meth:`QUDTStore.concept_query` (run over
an ontology graph) and an RDF type, return concept dicts (uri, kind, label,
surfaces, exact_surfaces, symbol, ucum, related) for the embedding index. This module does
no querying, parsing, fetching, or disk caching.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from rdflib.namespace import SKOS

from acquirium.internals.internals_namespaces import *  # noqa: F403
from acquirium.TextMatch.embedding_matcher import _split_local_name

logger = logging.getLogger("acquirium.qudt_store")


def _build_surfaces(
    uri: str, labels: list[str], symbol: str | None, ucum: str | None, is_unit: bool
) -> tuple[list[str], list[str]]:
    """Surface forms of a single QUDT concept: ``(embedded, exact-only)``."""
    surfaces: list[str] = []
    exact: list[str] = []
    seen: set[str] = set()

    def _add(s: str | None, into: list[str]) -> None:
        if s and s not in seen:
            seen.add(s)
            into.append(s)

    for label in labels:
        _add(label.lower(), surfaces)

    # For a unit with a label, everything else is looked up, never embedded.
    # Its local name is the QUDT code spelled out ("gal us per min", "milli gm
    # per l"), which the label already says in words. Its symbol and UCUM code
    # are answered by the matcher's exact stage ("kg", "KG", "mg/L") and, for
    # every other spelling ("mg·L⁻¹", "mg per L"), by UnitKeyIndex.
    # A unit without a label embeds its local name instead. A quantity kind
    # keeps its local name embedded: most have no label.
    def _rest() -> list[str]:
        return exact if (is_unit and surfaces) else surfaces

    tokens = _split_local_name(uri)
    if tokens:
        _add(" ".join(tokens), _rest())

    # A quantity kind's symbol is a formula letter ("T", "ε", "C_D", "l_{ph}")
    # that nobody types for it, and as text it attracts unrelated short
    # queries ("pH" matched "l_{ph}"). It is not a surface at all.
    if is_unit:
        _add(symbol, _rest())
        _add(ucum, _rest())

    return surfaces, exact


class QUDTStore:
    """Extract QUDT unit / quantity-kind concepts from SPARQL rows."""

    @staticmethod
    def concept_query(rdf_type: str) -> str:
        """SELECT ``?s ?p ?o ?lang`` for every ``rdf_type`` subject.

        One row per indexed property value, plus an unbound row for subjects
        with none (their local name still yields a surface).
        """
        is_unit = rdf_type == str(QUDT.Unit)  # noqa: F405
        preds = [
            RDFS.label, SKOS.prefLabel, SKOS.altLabel,  # noqa: F405
            QUDT.symbol, QUDT.ucumCode,  # noqa: F405
            QUDT.hasQuantityKind if is_unit else QUDT.applicableUnit,  # noqa: F405
        ]
        values = " ".join(f"<{p}>" for p in preds)
        return f"""
        SELECT ?s ?p ?o ?lang WHERE {{
          ?s a <{rdf_type}> .
          OPTIONAL {{
            VALUES ?p {{ {values} }}
            ?s ?p ?o .
            BIND(LANG(?o) AS ?lang)
          }}
        }}
        """

    @staticmethod
    def extract_concepts(
        rows: Iterable[tuple[str | None, ...]], rdf_type: str
    ) -> list[dict[str, Any]]:
        """Concept dicts from :meth:`concept_query` rows, sorted by URI.

        ``related`` captures the cross-reference used by joint/context
        rerank: a unit's ``qudt:hasQuantityKind``, a quantity kind's
        ``qudt:applicableUnit``.
        """
        is_unit = rdf_type == str(QUDT.Unit)  # noqa: F405
        label_preds = [str(RDFS.label), str(SKOS.prefLabel), str(SKOS.altLabel)]  # noqa: F405
        relation_pred = str(QUDT.hasQuantityKind if is_unit else QUDT.applicableUnit)  # noqa: F405

        values: dict[str, dict[str, list[tuple[str, str | None]]]] = {}
        for subj, pred, obj, lang in rows:
            by_pred = values.setdefault(subj, {})
            if pred is not None and obj is not None:
                by_pred.setdefault(pred, []).append((obj, lang))

        concepts: list[dict[str, Any]] = []
        # Sort everything so store iteration order never changes the concept
        # dicts; otherwise the cache hash drifts and warm starts re-embed.
        for uri in sorted(values):
            by_pred = values[uri]
            labels: list[str] = []
            display_label: str | None = None
            for pred in label_preds:
                pred_labels = {
                    text for text, lang in by_pred.get(pred, ())
                    if text and not (lang and not lang.startswith("en"))
                }
                for text in sorted(pred_labels):
                    if text not in labels:
                        labels.append(text)
                    if display_label is None:
                        display_label = text

            symbols = sorted({o for o, _ in by_pred.get(str(QUDT.symbol), ())})  # noqa: F405
            symbol = symbols[0] if symbols else None
            ucums = sorted({o for o, _ in by_pred.get(str(QUDT.ucumCode), ())})  # noqa: F405
            ucum = ucums[0] if ucums else None

            surfaces, exact_surfaces = _build_surfaces(uri, labels, symbol, ucum, is_unit)
            if not surfaces:
                continue

            concepts.append({
                "uri": uri,
                "kind": "unit" if is_unit else "quantity_kind",
                "label": display_label or " ".join(_split_local_name(uri)) or uri,
                "surfaces": surfaces,
                "exact_surfaces": exact_surfaces,
                "symbol": symbol,
                "ucum": ucum,
                "related": sorted({o for o, _ in by_pred.get(relation_pred, ())}),
            })
        return concepts
