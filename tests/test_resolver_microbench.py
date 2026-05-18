"""No-Docker microbenchmarks: regression tripwires for the resolve hot path.

These do NOT need the server. They guard the two tiers the QUDT/concept
unification newly put on the hot path so a perf regression can't land
silently without a running stack:

  - QUDTUnitConverter.resolve_unit HIT  — fast deterministic path
  - QUDTUnitConverter.resolve_unit MISS — the ~O(graph) literal scan
  - EmbeddingMatcher exact-stage lookup — model-free dict lookup

Ceilings are deliberately loose: they catch order-of-magnitude regressions
(e.g. an accidental full-graph scan added to the hit path), not micro-jitter.
For a detailed per-tier breakdown use scripts/benchmark/resolver_latency.py.
"""

from __future__ import annotations

import statistics
import time
from pathlib import Path

import pytest

from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher
from acquirium.internals.qudt_units import QUDTUnitConverter, UnitNotFound

_QUDT_TTL = Path("ontologies/qudt_unit.ttl")


def _median_ms(fn, iters: int) -> float:
    fn()  # warmup (discarded)
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples)


@pytest.fixture(scope="module")
def converter() -> QUDTUnitConverter:
    if not _QUDT_TTL.exists():
        pytest.skip(f"{_QUDT_TTL} not available")
    return QUDTUnitConverter(str(_QUDT_TTL))


def test_converter_resolve_hit_is_fast(converter: QUDTUnitConverter) -> None:
    """A symbol that resolves should not touch the O(graph) fallback."""
    def _hit() -> None:
        converter.resolve_unit("kg")

    med = _median_ms(_hit, iters=50)
    assert med < 50.0, f"resolve_unit('kg') median {med:.2f}ms — expected < 50ms"


def test_converter_resolve_miss_bounded(converter: QUDTUnitConverter) -> None:
    """A miss runs the literal scan; bound it so it can't blow up unnoticed."""
    def _miss() -> None:
        with pytest.raises(UnitNotFound):
            converter.resolve_unit("zzz_definitely_not_a_unit_zzz")

    med = _median_ms(_miss, iters=20)
    assert med < 750.0, f"resolve_unit(miss) median {med:.2f}ms — expected < 750ms"


def test_exact_stage_lookup_is_fast() -> None:
    """Graph-exact surface lookup is a dict hit; no embedding model loaded."""
    m = EmbeddingMatcher()
    m.build_index(
        [
            {
                "uri": f"urn:x#C{i}",
                "kind": "class",
                "label": f"thing {i}",
                "surfaces": [f"thing {i}", f"C{i}"],
            }
            for i in range(500)
        ]
    )

    # Drop the model that build_index loaded; an exact-only query must not
    # re-instantiate it (it would if the semantic stage ran).
    m._model = None

    def _lookup() -> None:
        # top_k=1 with a guaranteed exact hit short-circuits before the
        # semantic stage, so _embed / _ensure_model are never called.
        res = m.query("thing 250", kind="class", top_k=1)
        assert res and res[0].match_stage == "exact"

    med = _median_ms(_lookup, iters=200)
    assert med < 10.0, f"exact-stage lookup median {med:.2f}ms — expected < 10ms"
    assert m._model is None, "exact-only query must not load the embedding model"
