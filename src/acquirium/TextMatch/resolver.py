"""Single entry point for concept *normalization* (text → canonical URI).

`ConceptResolver` resolves text against an ordered set of candidate
**sources** and applies one ranking policy. Conversion (numeric value math,
multiplier/offset/compatibility) is a separate concern handled by
`QUDTUnitConverter` directly, not here.

Sources, in authority order:

1. graph     — embedding index over the union (data) graph; all kinds.
2. converter — `QUDTUnitConverter` deterministic resolution; units only.
3. qudt      — embedding index over the broad QUDT vocabulary; unit /
               quantity_kind / untyped.

A source declares which ``kind`` values it serves and an optional per-source
``min_score`` floor. `resolve` walks the sources in order, gathers
candidates, and stops early once an exact hit exists and no context rerank is
pending (a cascade early-exit: an exact hit from an earlier source cannot be
outranked by a later one, so querying the rest cannot change the result).
Candidates are then ranked by one total order (exact before semantic, then
score, with source order as a stable tiebreak), reranked by context, and cut
to ``top_k``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Source:
    """A candidate source in the resolution cascade.

    ``produce(text, kind, top_k, min_score) -> list[ResolveResult]``. The
    effective ``min_score`` is raised to ``floor`` when ``kind`` is in
    ``floor_kinds`` (e.g. the graph source distrusts weak guesses for
    vocabulary kinds, where the curated QUDT vocabulary is authoritative).
    """

    name: str
    kinds: frozenset[str | None]
    produce: Callable[[str, str | None, int, float], list[ResolveResult]]
    floor: float = 0.0
    floor_kinds: frozenset[str] = frozenset()

    def min_score(self, kind: str | None, base: float) -> float:
        return max(base, self.floor) if kind in self.floor_kinds else base


class ConceptResolver:
    """Resolve natural-language text to ontology / QUDT URIs.

    Args:
        graph_matcher: embedding index built from the union (data) graph.
        qudt_matcher: embedding index over the broad QUDT vocabulary.
        converter_provider: zero-arg callable returning a ready
            ``QUDTUnitConverter``. It may raise if no QUDT graph is available;
            the converter source then yields nothing and the cascade falls
            through to the matchers.
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

        def _matcher(m: EmbeddingMatcher):
            return lambda text, kind, k, ms: m.query(
                text=text, kind=kind, top_k=k, min_score=ms
            )

        # Authority order — list position is the precedence (a stable rank
        # makes earlier sources win ties against later ones).
        self._sources: list[Source] = [
            Source(
                "graph",
                kinds=frozenset({None, "class", "predicate", "unit", "quantity_kind"}),
                produce=_matcher(graph_matcher),
                floor=0.8,
                floor_kinds=frozenset({"unit", "quantity_kind"}),
            ),
            Source(
                "converter",
                kinds=frozenset({"unit"}),
                produce=lambda text, kind, k, ms: self._deterministic_unit(text),
            ),
            Source(
                "qudt",
                kinds=frozenset({None, "unit", "quantity_kind"}),
                produce=_matcher(qudt_matcher),
            ),
        ]

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

        ``context`` is an optional list of already-chosen URIs (e.g. a
        resolved quantity kind / medium). Candidates linked to a context URI
        via their ``related`` set are moved ahead; empty or unmatched context
        leaves ranking unchanged (so a failed sibling resolution is harmless).
        """
        fetch_k = max(top_k, self._CONTEXT_FETCH_K) if context else top_k

        candidates: list[ResolveResult] = []
        for src in self._sources:
            if kind not in src.kinds:
                continue
            candidates += src.produce(
                text, kind, fetch_k, src.min_score(kind, min_score)
            )
            # Cascade early-exit: an exact hit from this (higher-authority)
            # source cannot be outranked by a later one, so further sources
            # cannot change the result. Skipped under context, where the
            # rerank needs the full candidate set.
            if not context and any(c.match_stage == "exact" for c in candidates):
                break

        ranked = self._rank(candidates, limit=fetch_k)
        if context:
            ranked = self._rerank_by_context(ranked, context)
        return ranked[:top_k]

    # -------------------- tiers --------------------
    def _deterministic_unit(self, text: str) -> list[ResolveResult]:
        """Authoritative QUDT unit resolution via the converter.

        Returns at most one exact (score 1.0) result. Silently yields nothing
        when no converter is available, the text is not a unit, or the only
        result is a *synthetic composed ratio* (``urn:qudt:ratio:...``);
        synthetic ratios are left to the embedding matchers instead.
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

    # -------------------- rank / rerank policy --------------------
    @staticmethod
    def _rank(candidates: list[ResolveResult], limit: int) -> list[ResolveResult]:
        """One total order: exact before semantic, then score.

        Stable sort, so candidates from earlier (higher-authority) sources
        win ties against equal candidates from later ones. Dedupes by URI,
        caps at ``limit``.
        """
        ordered = sorted(
            candidates,
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
