"""Single entry point for concept *normalization* (text → canonical URI).

`ConceptResolver` owns the whole resolution policy that was previously split
between `Manager.resolve_text` (data-graph + QUDT embedding matchers, context
rerank) and the client-side deterministic path that called the QUDT unit
converter. Conversion (numeric value math, multiplier/offset/compatibility) is
a *separate* concern and deliberately not handled here — `Manager` keeps
calling `QUDTUnitConverter` directly for that.

Pipeline (one ordered policy):

1. data-graph exact   — `_graph_matcher` exact surface lookup
2. converter exact    — `QUDTUnitConverter` deterministic resolution, *units
                         only* (mirrors the old client deterministic path,
                         which only ran for ``kind == "unit"``)
3. data-graph semantic
4. QUDT semantic      — `_qudt_matcher`
5. context rerank     — promote candidates linked to a context URI

Only `kind == "unit"` gains the deterministic converter tier; ``class`` /
``predicate`` / ``quantity_kind`` / ``None`` keep the exact behavior they had
in `Manager.resolve_text`, so existing ``/resolve_text`` responses are
unchanged for those kinds.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable

from acquirium.TextMatch.embedding_matcher import (
    EmbeddingMatcher,
    ResolveResult,
    _split_local_name,
)
from acquirium.internals.qudt_units import UnitNotFound

if TYPE_CHECKING:
    from acquirium.internals.qudt_units import QUDTUnitConverter

logger = logging.getLogger("acquirium.concept_resolver")


class ConceptResolver:
    """Resolve natural-language text to ontology / QUDT URIs.

    Args:
        graph_matcher: embedding index built from the union (data) graph.
        qudt_matcher: embedding index over the broad QUDT vocabulary.
        converter_provider: zero-arg callable returning a ready
            ``QUDTUnitConverter``. It may raise if no QUDT graph is available;
            the deterministic tier degrades silently to the matchers in that
            case (preserving pre-unification behavior).
    """

    # Over-fetch this many candidates when context is present so the rerank
    # has enough material to reorder (a context hit may sit past ``top_k``).
    _CONTEXT_FETCH_K = 20

    def __init__(
        self,
        graph_matcher: EmbeddingMatcher,
        qudt_matcher: EmbeddingMatcher,
        converter_provider: Callable[[], "QUDTUnitConverter"],
    ) -> None:
        self._graph_matcher = graph_matcher
        self._qudt_matcher = qudt_matcher
        self._converter_provider = converter_provider

    # -------------------- public API --------------------
    def resolve(
        self,
        text: str,
        kind: str | None = None,
        top_k: int = 5,
        min_score: float = 0.5,
        context: list[str] | None = None,
    ) -> list[ResolveResult]:
        """Resolve *text* to ranked concepts.

        Routing:
          class / predicate    -> graph matcher only
          unit                 -> graph exact short-circuit, else
                                  graph(>=0.8) + deterministic converter + QUDT
          quantity_kind        -> graph(>=0.8) + QUDT (no converter)
          None                 -> graph + QUDT

        ``context`` is an optional list of already-chosen URIs (e.g. a
        resolved quantity kind / medium). Candidates linked to a context URI
        via their ``related`` set are moved ahead; empty or unmatched context
        leaves ranking unchanged (so a failed sibling resolution is harmless).
        """
        fetch_k = max(top_k, self._CONTEXT_FETCH_K) if context else top_k

        def _q(matcher: EmbeddingMatcher, score: float = min_score) -> list[ResolveResult]:
            return matcher.query(text=text, kind=kind, top_k=fetch_k, min_score=score)

        if kind in ("class", "predicate"):
            results = _q(self._graph_matcher)
        elif kind in ("unit", "quantity_kind"):
            # Graph concepts take priority on ties; an exact graph hit already
            # wins, so skip the QUDT query (and its embedding) entirely then.
            graph_hits = _q(self._graph_matcher, score=0.8)
            if graph_hits and graph_hits[0].match_stage == "exact":
                results = graph_hits
            else:
                # Converter tier is units-only and mirrors the old client
                # deterministic path; it carries the unit's quantity kinds as
                # ``related`` so context rerank still works.
                det = self._deterministic_unit(text) if kind == "unit" else []
                results = self._combine(
                    graph_hits + det, _q(self._qudt_matcher), limit=fetch_k
                )
        else:
            results = self._combine(
                _q(self._graph_matcher),
                _q(self._qudt_matcher),
                limit=fetch_k,
            )

        if context:
            results = self._rerank_by_context(results, context)

        return results[:top_k]

    # -------------------- tiers --------------------
    def _deterministic_unit(self, text: str) -> list[ResolveResult]:
        """Authoritative QUDT unit resolution via the converter.

        Returns at most one exact (score 1.0) result. Silently yields nothing
        when no converter is available, the text is not a unit, or the only
        result is a *synthetic composed ratio* (``urn:qudt:ratio:...``) — in
        that last case the embedding matchers handled it before unification, so
        we keep deferring to them to avoid a regression.
        """
        try:
            conv = self._converter_provider()
        except Exception:
            return []

        try:
            unit_def = conv.resolve_unit(text)
        except UnitNotFound:
            try:
                unit_def = conv.infer_unit(text)
            except UnitNotFound:
                return []
            except Exception:
                logger.debug("infer_unit(%r) raised", text, exc_info=True)
                return []
        except Exception:
            logger.debug("resolve_unit(%r) raised", text, exc_info=True)
            return []

        uri = str(unit_def.uri)
        if uri.startswith("urn:qudt:ratio:"):
            return []

        label = unit_def.label or " ".join(_split_local_name(uri)) or uri
        return [
            ResolveResult(
                uri=uri,
                kind="unit",
                label=label,
                score=1.0,
                matched_surface=text,
                match_stage="exact",
                related=tuple(str(q) for q in unit_def.quantity_kinds),
            )
        ]

    # -------------------- merge / rerank policy --------------------
    @staticmethod
    def _combine(
        primary: list[ResolveResult],
        secondary: list[ResolveResult],
        limit: int,
    ) -> list[ResolveResult]:
        """Merge two ranked lists: exact-stage hits first, then by score.

        Stable sort with ``primary`` concatenated first, so a graph / converter
        concept wins ties against an equal QUDT one. Dedupes by URI, caps at
        ``limit``.
        """
        ordered = sorted(
            primary + secondary,
            key=lambda r: (r.match_stage == "exact", r.score),
            reverse=True,
        )
        seen: set[str] = set()
        out: list[ResolveResult] = []
        for r in ordered:
            if r.uri in seen:
                continue
            seen.add(r.uri)
            out.append(r)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _rerank_by_context(
        results: list[ResolveResult], context: list[str]
    ) -> list[ResolveResult]:
        """Move results linked to a context URI ahead of the rest.

        Stable within each group, so existing order is kept when nothing is
        connected. A failed/wrong context URI simply fails to intersect any
        candidate's ``related`` set → ordering unchanged.
        """
        ctx = set(context)
        connected: list[ResolveResult] = []
        rest: list[ResolveResult] = []
        for r in results:
            (connected if ctx.intersection(r.related) else rest).append(r)
        return connected + rest
