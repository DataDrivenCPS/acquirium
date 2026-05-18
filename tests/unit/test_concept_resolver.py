"""No-Docker tests for ConceptResolver pipeline policy.

Fakes the two embedding matchers and the QUDT converter so the routing,
the units-only deterministic tier, synthetic-ratio skipping, and context
rerank can be asserted without a server or the embedding model.
"""

from __future__ import annotations


from acquirium.TextMatch.embedding_matcher import ResolveResult
from acquirium.TextMatch.resolver import ConceptResolver
from acquirium.internals.qudt_units import UnitNotFound


class FakeMatcher:
    """Returns canned ResolveResults; records the kinds it was queried for."""

    def __init__(self, results: list[ResolveResult]):
        self._results = results
        self.queried_kinds: list[str | None] = []

    def query(self, text, kind=None, top_k=5, min_score=0.5):
        self.queried_kinds.append(kind)
        return [r for r in self._results if kind is None or r.kind == kind][:top_k]


class FakeUnitDef:
    def __init__(self, uri, label="lbl", qks=()):
        self.uri = uri
        self.label = label
        self.quantity_kinds = tuple(qks)


class FakeConverter:
    def __init__(self, mapping: dict[str, FakeUnitDef]):
        self._mapping = mapping
        self.calls: list[str] = []

    def resolve_unit(self, text):
        self.calls.append(text)
        if text in self._mapping:
            return self._mapping[text]
        raise UnitNotFound(text)

    def infer_unit(self, text):
        self.calls.append(f"infer:{text}")
        if text in self._mapping:
            return self._mapping[text]
        raise UnitNotFound(text)


def _rr(uri, kind, *, stage="semantic", score=0.7, related=()):
    return ResolveResult(
        uri=uri, kind=kind, label=uri.rsplit("/", 1)[-1], score=score,
        matched_surface="x", match_stage=stage, related=tuple(related),
    )


def _no_converter():
    raise RuntimeError("converter_provider should not be called")


def _resolver(graph=None, qudt=None, conv=None):
    return ConceptResolver(
        graph_matcher=FakeMatcher(graph or []),
        qudt_matcher=FakeMatcher(qudt or []),
        converter_provider=(lambda: conv) if conv is not None else _no_converter,
    )


class TestRouting:
    def test_class_uses_graph_only(self):
        g = FakeMatcher([_rr("urn:C", "class")])
        q = FakeMatcher([_rr("urn:Q", "class")])
        r = ConceptResolver(g, q, lambda: None)
        out = r.resolve("thing", kind="class")
        assert [m.uri for m in out] == ["urn:C"]
        assert q.queried_kinds == []  # QUDT matcher untouched for class

    def test_quantity_kind_skips_converter(self):
        # converter_provider raises if invoked — QK must not call it.
        r = _resolver(qudt=[_rr("http://qudt.org/qk/Mass", "quantity_kind")])
        out = r.resolve("mass", kind="quantity_kind")
        assert out and out[0].uri == "http://qudt.org/qk/Mass"


class TestDeterministicUnitTier:
    def test_unit_converter_hit_wins_over_semantic(self):
        conv = FakeConverter(
            {"kg": FakeUnitDef("http://qudt.org/unit/KiloGM", qks=["http://qk/Mass"])}
        )
        r = _resolver(
            graph=[],
            qudt=[_rr("http://qudt.org/unit/Wrong", "unit", score=0.9)],
            conv=conv,
        )
        out = r.resolve("kg", kind="unit")
        assert out[0].uri == "http://qudt.org/unit/KiloGM"
        assert out[0].match_stage == "exact" and out[0].score == 1.0
        assert out[0].related == ("http://qk/Mass",)

    def test_synthetic_ratio_uri_is_skipped(self):
        conv = FakeConverter({"mg/L": FakeUnitDef("urn:qudt:ratio:a__per__b")})
        r = _resolver(
            qudt=[_rr("http://qudt.org/unit/MilliGM-PER-L", "unit", score=0.8)],
            conv=conv,
        )
        out = r.resolve("mg/L", kind="unit")
        # Synthetic composed ratio not emitted; defers to the matcher result.
        assert out[0].uri == "http://qudt.org/unit/MilliGM-PER-L"

    def test_converter_unavailable_degrades_to_matchers(self):
        r = _resolver(qudt=[_rr("http://qudt.org/unit/L", "unit", score=0.7)])
        out = r.resolve("liter", kind="unit")  # provider raises -> graceful
        assert out and out[0].uri == "http://qudt.org/unit/L"


class TestContextRerank:
    def test_context_promotes_related_candidate(self):
        conv = FakeConverter(
            {"kg": FakeUnitDef("http://unit/KiloGM", qks=["http://qk/Mass"])}
        )
        # KiloGAUSS (flux) also an exact qudt hit, related to flux QK.
        r = _resolver(
            qudt=[_rr("http://unit/KiloGAUSS", "unit", stage="exact", score=1.0,
                      related=["http://qk/Flux"])],
            conv=conv,
        )
        flux = r.resolve("kg", kind="unit", top_k=1, context=["http://qk/Flux"])
        assert flux[0].uri == "http://unit/KiloGAUSS"
        mass = r.resolve("kg", kind="unit", top_k=1, context=["http://qk/Mass"])
        assert mass[0].uri == "http://unit/KiloGM"

    def test_irrelevant_context_keeps_order(self):
        conv = FakeConverter({"kg": FakeUnitDef("http://unit/KiloGM")})
        r = _resolver(conv=conv)
        out = r.resolve("kg", kind="unit", top_k=1, context=["http://unrelated"])
        assert out[0].uri == "http://unit/KiloGM"
