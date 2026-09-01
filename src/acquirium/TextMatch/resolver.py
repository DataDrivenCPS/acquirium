"""Single entry point for concept *normalization* (text → canonical URI).

`ConceptResolver` resolves text against an ordered set of candidate
**sources** and applies one ranking policy. Conversion (numeric value math,
multiplier/offset/compatibility) is a separate concern handled by
`QUDTUnitConverter` directly, not here.

Sources, in authority order:

1. graph     — embedding index over the union (data) graph; all kinds.
2. converter — `QUDTUnitConverter` deterministic resolution; units only.
3. label     — exact rdfs:label / skos lookup against the graph store; all
               kinds. Only present when embeddings are disabled (the graph
               and qudt matchers then are absent) so resolution of exact
               names keeps working without a model.
4. qudt      — embedding index over the broad QUDT vocabulary; unit /
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


def _qudt_related_compat(a: ResolveResult, b: ResolveResult) -> float:
    """1.0 if the two candidates are linked by a QUDT relation, else 0.0.

    Symmetric: a unit's ``related`` holds its quantity kinds and a quantity
    kind's holds its applicable units, so either direction suffices.
    """
    return 1.0 if (b.uri in a.related or a.uri in b.related) else 0.0


@dataclass(frozen=True)
class Relation:
    """A cross-field compatibility used by joint record resolution.

    The joint decoder references only this registry — it carries no
    kind-specific logic. New relations (a graph-path check, a co-occurrence
    prior, …) are added by appending a ``Relation`` with its own ``compat``;
    no algorithm change.
    """

    kind_a: str
    kind_b: str
    compat: Callable[[ResolveResult, ResolveResult], float]  # → [0, 1]
    weight: float = 0.25


# The only authoritative cross-field relation today: a unit and its quantity
# kind, via QUDT. medium/substance have no type-level relation (instance-only
# in S223/water); a co-occurrence-prior relation is a designed follow-up.
RELATIONS: list[Relation] = [
    Relation("unit", "quantity_kind", _qudt_related_compat),
]

# Kinds that participate in some relation; only these benefit from the joint
# decode, so only they need the wider fetch + disabled early-exit.
_RELATION_KINDS: frozenset[str] = frozenset(
    k for r in RELATIONS for k in (r.kind_a, r.kind_b)
)


class ConceptResolver:
    """Resolve natural-language text to ontology / QUDT URIs.

    Args:
        graph_matcher: embedding index built from the union (data) graph,
            or ``None`` when embeddings are disabled.
        qudt_matcher: embedding index over the broad QUDT vocabulary, or
            ``None`` when embeddings are disabled.
        converter_provider: zero-arg callable returning a ready
            ``QUDTUnitConverter``. It may raise if no QUDT graph is available;
            the converter source then yields nothing and the cascade falls
            through to the matchers.
        label_lookup: optional exact-label resolution callable with the
            ``produce`` signature (``(text, kind, top_k, min_score) ->
            list[ResolveResult]``), slotted after the converter. Supplied by
            the Manager when embeddings are disabled so exact names still
            resolve without a model; must return exact (score 1.0) hits only.
    """

    # Over-fetch this many candidates when context is present so the rerank
    # has enough material to reorder (a context hit may sit past ``top_k``).
    _CONTEXT_FETCH_K = 20

    def __init__(
        self,
        graph_matcher: EmbeddingMatcher | None,
        qudt_matcher: EmbeddingMatcher | None,
        converter_provider: Callable[[], "QUDTUnitConverter"],
        label_lookup: Callable[
            [str, str | None, int, float], list[ResolveResult]
        ] | None = None,
    ) -> None:
        self._graph_matcher = graph_matcher
        self._qudt_matcher = qudt_matcher
        self._converter_provider = converter_provider

        def _matcher(m: EmbeddingMatcher):
            return lambda text, kind, k, ms: m.query(
                text=text, kind=kind, top_k=k, min_score=ms
            )

        _all_kinds = frozenset(
            {None, "class", "predicate", "unit", "quantity_kind",
             "substance", "process"}
        )

        # Authority order — list position is the precedence (a stable rank
        # makes earlier sources win ties against later ones). Matcher
        # sources are present only when their index exists; the converter
        # always is. The label source stands in for the matchers when
        # embeddings are off, after the converter so deterministic unit
        # resolution keeps priority.
        self._sources: list[Source] = []
        if graph_matcher is not None:
            self._sources.append(
                Source(
                    "graph",
                    kinds=_all_kinds,
                    produce=_matcher(graph_matcher),
                    floor=0.8,
                    floor_kinds=frozenset({"unit", "quantity_kind"}),
                )
            )
        self._sources.append(
            Source(
                "converter",
                kinds=frozenset({"unit"}),
                produce=lambda text, kind, k, ms: self._deterministic_unit(text),
            )
        )
        if label_lookup is not None:
            self._sources.append(
                Source("label", kinds=_all_kinds, produce=label_lookup)
            )
        if qudt_matcher is not None:
            self._sources.append(
                Source(
                    "qudt",
                    kinds=frozenset({None, "unit", "quantity_kind"}),
                    produce=_matcher(qudt_matcher),
                )
            )

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

        ``context`` is an optional list of already-resolved sibling URIs.
        It is a *best-effort rerank hint only* — see ``_rerank_by_context``
        for the exact (and deliberately narrow) contract. When ``context``
        is present the cascade early-exit is disabled so every applicable
        source contributes, and ``fetch_k`` is widened so a context-relevant
        candidate ranked below ``top_k`` survives into the rerank.

        Example::

            # engineering-unit cell from a SCADA CSV column header
            resolve("mg/L", kind="unit", top_k=3)
            # -> [ResolveResult(uri="http://qudt.org/vocab/unit/MilliGM-PER-L",
            #                    kind="unit", score=1.0, match_stage="exact",
            #                    ...), ...]
        """
        fetch_k = max(top_k, self._CONTEXT_FETCH_K) if context else top_k
        ranked = self._ranked_candidates(
            text, kind, fetch_k, min_score, early_exit=not context
        )
        if context:
            ranked = self._rerank_by_context(ranked, context)
        return ranked[:top_k]

    def resolve_record(
        self,
        fields: dict[str, tuple[str, str | None]],
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> dict[str, list[ResolveResult]]:
        """Resolve a record's fields *jointly*.

        ``fields`` maps an arbitrary *result label* to ``(text, kind)``.
        The label is only used to key the returned dict (typically a source
        column / sensor tag). ``kind`` constrains *how* the text is
        resolved (which vocabulary — "resolve as a unit"), like
        ``resolve_text(kind=...)``; it is not a claim about the answer, and
        may be ``None`` to resolve across all kinds. The typed cross-field
        relations (e.g. unit↔quantity_kind) only connect fields whose
        ``kind`` says which role they play — declaring ``kind`` declares the
        role, not the result. Example — the metadata columns a driver
        pulled from a plant historian point export for chlorine-residual
        analyzer ``AIT-330`` (the keys are that export's column headers)::

            {"AIT-330.EU":  ("mg/L", "unit"),
             "AIT-330.QTY": ("mass concentration", "quantity_kind"),
             "AIT-330.MED": ("treated water", "class")}

        Each field's candidates are gathered independently (cascade
        early-exit off, so the joint decode sees depth); then for every
        :data:`RELATIONS` entry
        whose two kinds are both present, the pair is chosen by
        ``argmax a.score + b.score + weight·compat(a, b)``. A side whose top
        candidate is an exact hit is pinned (the compat bonus may not demote
        an authoritative match for an uncertain sibling). Fields in no
        relation take their own top candidate. When nothing is compatible
        this reduces exactly to independent resolution — no special case.

        Returns, per field, the candidate list with the chosen winner first
        (then the rest by base score), truncated to ``top_k``.
        """
        # Only fields whose kind participates in a relation can be reranked
        # by the joint decode, so only they need the wider fetch + disabled
        # early-exit; the rest resolve with the normal cheap settings.
        def _gather(text: str, kind: str | None) -> list[ResolveResult]:
            joint = kind in _RELATION_KINDS
            return self._ranked_candidates(
                text,
                kind,
                max(top_k, self._CONTEXT_FETCH_K) if joint else top_k,
                min_score,
                early_exit=not joint,
            )

        cands = {
            name: _gather(text, kind) for name, (text, kind) in fields.items()
        }

        # First field per kind (multiple fields of one kind: first wins;
        # a field-sharing relation graph is a documented future seam).
        by_kind: dict[str | None, str] = {}
        for name, (_text, kind) in fields.items():
            by_kind.setdefault(kind, name)

        winners: dict[str, ResolveResult] = {}
        for rel in RELATIONS:
            na, nb = by_kind.get(rel.kind_a), by_kind.get(rel.kind_b)
            if na is None or nb is None or na == nb:
                continue
            if na in winners or nb in winners or not cands[na] or not cands[nb]:
                continue
            # An exact hit (exact surface / URI, score 1.0) is authoritative:
            # pin that side to its top candidate so the compat bonus cannot
            # demote a confident match because of an uncertain sibling (e.g.
            # unit "byte"->BYTE must not flip to a data-rate unit just
            # because a weak quantity_kind matched DataRate). The other side
            # still optimises against the pin; both pinned -> independent.
            def _pool(cs: list[ResolveResult]) -> list[ResolveResult]:
                return [cs[0]] if cs[0].match_stage == "exact" else cs

            pool_a, pool_b = _pool(cands[na]), _pool(cands[nb])
            best, best_pair = None, None
            for a in pool_a:
                for b in pool_b:
                    s = a.score + b.score + rel.weight * rel.compat(a, b)
                    if best is None or s > best:
                        best, best_pair = s, (a, b)
            winners[na], winners[nb] = best_pair  # type: ignore[misc]

        out: dict[str, list[ResolveResult]] = {}
        for name, ranked in cands.items():
            win = winners.get(name) or (ranked[0] if ranked else None)
            if win is None:
                out[name] = []
                continue
            out[name] = [win, *[c for c in ranked if c.uri != win.uri]][:top_k]
        return out

    # -------------------- candidate generation --------------------
    def _ranked_candidates(
        self,
        text: str,
        kind: str | None,
        limit: int,
        min_score: float,
        *,
        early_exit: bool,
    ) -> list[ResolveResult]:
        """Gather candidates from applicable sources, ranked and capped.

        With ``early_exit`` an exact hit from a higher-authority source
        stops the cascade (a later source cannot outrank it). Callers that
        rerank afterwards (context, joint record decode) pass
        ``early_exit=False`` so the full candidate set is available.
        """
        candidates: list[ResolveResult] = []
        for src in self._sources:
            if kind not in src.kinds:
                continue
            candidates += src.produce(
                text, kind, limit, src.min_score(kind, min_score)
            )
            if early_exit and any(c.match_stage == "exact" for c in candidates):
                break
        return self._rank(candidates, limit=limit)

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
        """Stable-partition: candidates whose ``related`` set intersects
        ``context`` move ahead of the rest; order is preserved within each
        group. It only reorders the already-ranked, already-fetched list —
        it never adds, drops, or rescores a candidate.

        How ``context`` is produced and what it can actually do:

        - Origin (client side): the two-pass ``flex_query_rdf_inputs``
          decorator and ``Acquirium.register_streams`` resolve the *non-unit*
          siblings first and pass their resolved URIs here when resolving a
          unit. It travels client → ``GET /resolve_text`` (repeated query
          params) → ``Manager.resolve_text`` → ``resolve``.

        - Match is exact-URI set intersection against
          ``ResolveResult.related``. ``related`` is populated *only* for
          QUDT-backed candidates: a unit's ``qudt:hasQuantityKind`` and a
          quantity kind's ``qudt:applicableUnit`` (see ``QUDTStore``; the
          converter source carries ``unit_def.quantity_kinds``).
          Graph-matcher concepts always have ``related == ()``
          (``_aggregate_uri_label_rows``), so context can never move a
          class/predicate or a graph-defined unit/QK.

        Consequences (the narrowness is deliberate, but real):

        - The only effective use is disambiguating an ambiguous *unit* by a
          *quantity-kind* context URI (e.g. "kg" + Mass → KiloGM, not
          KiloGAUSS), or a quantity kind by a unit. Medium/substance URIs
          that ``register_streams`` adds to the unit context are inert —
          nothing links a unit to a medium/substance via ``related``.
        - URIs must be byte-identical; a sibling resolved under a different
          scheme (nawi vs s223 vs qudt) will not intersect.
        - "wrong context" and "no context" are indistinguishable: both yield
          the identity ordering. This is safe (context can never inject a
          wrong answer) but silent — a mis-resolved sibling simply provides
          no signal.
        - Reorders only within the fetched ``fetch_k`` window; a relevant
          concept ranked past it cannot be promoted.
        """
        ctx = set(context)
        connected: list[ResolveResult] = []
        rest: list[ResolveResult] = []
        for r in results:
            (connected if ctx.intersection(r.related) else rest).append(r)
        return connected + rest
