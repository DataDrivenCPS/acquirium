"""No-Docker tests for ConceptResolver.resolve_record (joint resolution).

Fakes the matchers and converter so the joint decode policy is asserted
without a server or the embedding model.
"""

from __future__ import annotations

from acquirium.TextMatch.embedding_matcher import ResolveResult
from acquirium.TextMatch.resolver import ConceptResolver


class FakeMatcher:
    def __init__(self, results: list[ResolveResult]):
        self._results = results

    def query(self, text, kind=None, top_k=5, min_score=0.5):
        return [r for r in self._results if kind is None or r.kind == kind][:top_k]


def _no_converter():
    raise RuntimeError("converter not used in these tests")


def _rr(uri, kind, score, *, stage="semantic", related=()):
    return ResolveResult(
        uri=uri, kind=kind, label=uri, score=score,
        matched_surface="x", match_stage=stage, related=tuple(related),
    )


def _resolver(graph=None, qudt=None):
    return ConceptResolver(
        FakeMatcher(graph or []), FakeMatcher(qudt or []), _no_converter
    )


# unit/QK candidates live in the QUDT matcher here (converter unused).
class TestBidirectional:
    def test_qk_disambiguates_unit(self):
        r = _resolver(qudt=[
            _rr("u:KiloGAUSS", "unit", 0.92),
            _rr("u:KiloGM", "unit", 0.90, related=["q:Mass"]),
            _rr("q:Mass", "quantity_kind", 0.95, related=["u:KiloGM"]),
        ])
        out = r.resolve_record(
            {"unit": ("kg", "unit"), "qk": ("mass", "quantity_kind")}
        )
        # KiloGM has lower own score but is compatible with the confident QK.
        assert out["unit"][0].uri == "u:KiloGM"
        assert out["qk"][0].uri == "q:Mass"

    def test_unit_disambiguates_qk(self):
        r = _resolver(qudt=[
            _rr("u:KiloGM", "unit", 0.95, related=["q:Mass"]),
            _rr("q:Flux", "quantity_kind", 0.91),
            _rr("q:Mass", "quantity_kind", 0.90, related=["u:KiloGM"]),
        ])
        out = r.resolve_record(
            {"u": ("kg", "unit"), "q": ("m", "quantity_kind")}
        )
        assert out["q"][0].uri == "q:Mass"
        assert out["u"][0].uri == "u:KiloGM"


class TestIndependence:
    def test_all_incompatible_is_independent_argmax(self):
        r = _resolver(qudt=[
            _rr("u:A", "unit", 0.9), _rr("u:B", "unit", 0.6),
            _rr("q:X", "quantity_kind", 0.8), _rr("q:Y", "quantity_kind", 0.7),
        ])
        out = r.resolve_record(
            {"u": ("x", "unit"), "q": ("y", "quantity_kind")}
        )
        assert out["u"][0].uri == "u:A" and out["q"][0].uri == "q:X"

    def test_unrelated_field_is_independent(self):
        # medium has no relation; it just takes its own top.
        r = _resolver(graph=[_rr("m:Water", "class", 0.7)],
                      qudt=[_rr("u:L", "unit", 0.9, related=["q:Vol"]),
                            _rr("q:Vol", "quantity_kind", 0.9,
                                related=["u:L"])])
        out = r.resolve_record({
            "unit": ("L", "unit"),
            "qk": ("volume", "quantity_kind"),
            "medium": ("water", "class"),
        })
        assert out["medium"][0].uri == "m:Water"
        assert out["unit"][0].uri == "u:L" and out["qk"][0].uri == "q:Vol"

    def test_only_one_side_present_falls_back(self):
        r = _resolver(qudt=[_rr("u:A", "unit", 0.8, related=["q:Z"])])
        out = r.resolve_record({"unit": ("a", "unit")})
        assert out["unit"][0].uri == "u:A"


class TestWeight:
    def test_alpha_rescues_close_compatible_but_not_clear_winner(self):
        # weight default 0.25. Compatible pair total bonus 0.25.
        r = _resolver(qudt=[
            _rr("u:Strong", "unit", 0.95),                    # incompatible
            _rr("u:Compat", "unit", 0.80, related=["q:K"]),   # compatible
            _rr("q:K", "quantity_kind", 0.90, related=["u:Compat"]),
        ])
        # Compat pair: 0.80+0.90+0.25 = 1.95 vs Strong pair: 0.95+0.90 = 1.85
        out = r.resolve_record(
            {"u": ("x", "unit"), "q": ("y", "quantity_kind")}
        )
        assert out["u"][0].uri == "u:Compat"

        # Now the incompatible unit is clearly better; α must not override.
        r2 = _resolver(qudt=[
            _rr("u:Strong", "unit", 0.99),
            _rr("u:Compat", "unit", 0.60, related=["q:K"]),
            _rr("q:K", "quantity_kind", 0.90, related=["u:Compat"]),
        ])
        # Compat: 0.60+0.90+0.25 = 1.75 vs Strong: 0.99+0.90 = 1.89
        out2 = r2.resolve_record(
            {"u": ("x", "unit"), "q": ("y", "quantity_kind")}
        )
        assert out2["u"][0].uri == "u:Strong"

    def test_exact_match_is_pinned_against_weak_sibling(self):
        # The byte/"data size" failure: unit "byte" is an EXACT hit
        # (BYTE, 1.0) unrelated to anything; the quantity_kind text is junk
        # whose best is a semantic DataRate, and a data-rate unit is related
        # to it. Without pinning, the compat bonus (0.25) would flip the
        # unit off its authoritative exact match. It must not.
        r = _resolver(qudt=[
            _rr("u:BYTE", "unit", 1.0, stage="exact"),          # authoritative
            _rr("u:MegaBIT-PER-SEC", "unit", 0.93,
                related=["q:DataRate"]),                          # compatible
            _rr("q:DataRate", "quantity_kind", 0.85,
                related=["u:MegaBIT-PER-SEC"]),
        ])
        # Unpinned argmax would prefer 0.93+0.85+0.25=2.03 over
        # 1.0+0.85+0=1.85; the pin forces the exact BYTE to win.
        out = r.resolve_record(
            {"unit": ("byte", "unit"),
             "quantity_kind": ("data size", "quantity_kind")}
        )
        assert out["unit"][0].uri == "u:BYTE"
        assert out["unit"][0].match_stage == "exact"
        # The uncertain sibling still resolves (against the pinned unit).
        assert out["quantity_kind"][0].uri == "q:DataRate"
