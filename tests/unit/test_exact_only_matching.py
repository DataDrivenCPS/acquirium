"""Behavioral tests for exact-only matching (``Manager(exact_only=True)``).

Exact-only builds the same concept indexes as a normal start but never
embeds them. The matcher-level tests below are fast; the Manager ones
exercise a *real* Manager (Oxigraph + ontoenv + DuckDB under a tmp dir, no
Docker): no model is loaded, nothing is written under embedding_cache/, and
resolution still works through the deterministic converter and the indexes'
exact stage — with the same kinds a semantic start would report.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from acquirium.Server.manager import Manager
from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher
from acquirium.TextMatch.qudt_store import QUDTStore

_DIM = 8

_CONCEPTS = [
    {"uri": "urn:t:Pump", "kind": "class", "label": "Pump", "surfaces": ["pump"]},
    {"uri": "urn:t:KG", "kind": "unit", "label": "Kilogram", "surfaces": ["kg"]},
]


def _fake_vectors(n: int) -> np.ndarray:
    if n == 0:
        return np.empty((0, _DIM), dtype=np.float32)
    arr = np.arange(n * _DIM, dtype=np.float32).reshape(n, _DIM) + 1.0
    return arr / np.linalg.norm(arr, axis=1, keepdims=True)


# ---------------------------------------------------------------- matcher


def test_exact_only_matcher_never_embeds(tmp_path: Path) -> None:
    """The index builds and answers without a model or a cache file."""
    cache = tmp_path / "cache"
    m = EmbeddingMatcher(cache_dir=cache, model_cache_dir=tmp_path / "models",
                         exact_only=True)
    m._embed = lambda texts: pytest.fail("exact-only must not embed")  # type: ignore[assignment]

    m.build_index(_CONCEPTS)

    assert m.is_ready
    assert not cache.exists(), "exact-only holds no vectors, so it writes no cache"
    hits = m.query("pump", kind="class")
    assert [h.uri for h in hits] == ["urn:t:Pump"]
    assert hits[0].score == 1.0
    assert hits[0].match_stage == "exact"


def test_exact_only_matcher_filters_by_kind(tmp_path: Path) -> None:
    """A concept only ever answers for the kind it was indexed under."""
    m = EmbeddingMatcher(cache_dir=tmp_path / "cache", exact_only=True)
    m.build_index(_CONCEPTS)

    assert m.query("pump", kind="class")
    assert m.query("pump", kind="unit") == []
    assert m.query("kg", kind="unit")
    assert m.query("kg", kind="class") == []


def test_exact_only_matcher_has_no_fuzzy_matching(tmp_path: Path) -> None:
    m = EmbeddingMatcher(cache_dir=tmp_path / "cache", exact_only=True)
    m.build_index(_CONCEPTS)
    assert m.query("pumping station", kind="class") == []


def test_semantic_matcher_still_runs_both_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default mode is unchanged: exact hits first, then the fuzzy fill."""
    monkeypatch.setattr(
        EmbeddingMatcher, "_embed",
        lambda self, texts: _fake_vectors(len(texts)),
    )
    m = EmbeddingMatcher(cache_dir=tmp_path / "cache")
    m.build_index(_CONCEPTS)

    hits = m.query("pump", kind="class", top_k=2, min_score=0.0)
    assert hits[0].uri == "urn:t:Pump"
    assert hits[0].match_stage == "exact"
    assert m.query("pumping station", kind="class", min_score=0.0), (
        "a semantic index must still answer a near-miss"
    )


# ---------------------------------------------------------------- manager


def _make_manager(data_dir: Path, *, exact_only: bool) -> Manager:
    return Manager(
        data_dir=data_dir,
        timeseries_backend="duckdb",
        exact_only=exact_only,
    )


@pytest.fixture(scope="module")
def exact_manager(tmp_path_factory: pytest.TempPathFactory):
    data_dir = tmp_path_factory.mktemp("acq-exact-only")
    m = _make_manager(data_dir, exact_only=True)
    yield m
    m.close()


def test_status_reports_ready_without_semantics(exact_manager: Manager) -> None:
    status = exact_manager.embedding_status()
    assert status["semantic"] is False
    # The indexes are real — they just hold no vectors.
    assert status["graph"]["state"] == "ready"
    assert status["qudt"]["state"] == "ready"
    assert status["graph"]["concepts"] > 0


def test_no_model_and_no_cache_dir(exact_manager: Manager) -> None:
    assert exact_manager._graph_matcher.exact_only
    assert exact_manager._qudt_matcher.exact_only
    assert not (exact_manager.data_dir / "embedding_cache").exists()
    assert "fastembed" not in sys.modules


def test_unit_resolution_via_converter(exact_manager: Manager) -> None:
    """kind="unit" falls through to the deterministic QUDT converter."""
    results = exact_manager.resolve_text("mg/L", kind="unit", top_k=3)
    assert results, "converter tier must resolve an exact unit symbol"
    top = results[0]
    assert top["score"] == 1.0
    assert top["match_stage"] == "exact"
    assert "MilliGM-PER-L" in top["uri"]


def test_exact_label_resolves_class(exact_manager: Manager) -> None:
    """An exact ontology label resolves without any embeddings."""
    results = exact_manager.resolve_text("aeration basin", kind="class")
    assert results, "exact rdfs:label match must resolve"
    top = results[0]
    assert top["uri"].endswith("AerationBasin")
    assert top["kind"] == "class"
    assert top["score"] == 1.0
    assert top["match_stage"] == "exact"


@pytest.mark.parametrize("kind", ["unit", "quantity_kind"])
def test_class_label_never_answers_as_a_vocabulary_kind(
    exact_manager: Manager, kind: str
) -> None:
    """Equipment labels must not resolve as units or quantity kinds.

    The index carries each concept's extracted kind, so "pump" answers only
    for the kinds it was indexed under — a hand-rolled label lookup that
    tagged kinds separately used to return s223:Pump as a `unit` at score 1.0,
    which then got written onto a point as qudt:hasUnit.
    """
    assert exact_manager.resolve_text("pump", kind=kind) == []
    assert exact_manager.resolve_text("valve", kind=kind) == []


def test_fuzzy_text_needs_embeddings(exact_manager: Manager) -> None:
    """Near-misses resolve to nothing rather than to a wrong exact hit."""
    assert exact_manager.resolve_text("zzz total gibberish", kind="class") == []
    assert exact_manager.resolve_text("basin for aeration", kind="class") == []


def test_from_env_reads_exact_only_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_init(self: Manager, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(Manager, "__init__", fake_init)
    monkeypatch.delenv("ACQUIRIUM_CONFIG", raising=False)
    monkeypatch.setenv("ACQUIRIUM_EXACT_ONLY", "true")
    Manager.from_env()
    assert captured["exact_only"] is True

    monkeypatch.delenv("ACQUIRIUM_EXACT_ONLY")
    captured.clear()
    Manager.from_env()
    assert captured["exact_only"] is False


def test_warm_start_with_embeddings_builds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exact-only run must not stop a later semantic run from building.

    First start opens the data dir exact-only; the second opens the same
    (warm) dir with embeddings on and must actually build both indexes.
    Extraction and the model are stubbed so the test stays fast — what's
    asserted is that the build pipeline ran and populated the disk cache.
    """
    data_dir = tmp_path / "acq"

    m1 = _make_manager(data_dir, exact_only=True)
    m1.close()
    assert not (data_dir / "embedding_cache").exists()

    embed_calls = {"n": 0}

    def fake_embed(self: EmbeddingMatcher, texts: list[str]) -> np.ndarray:
        embed_calls["n"] += 1
        return _fake_vectors(len(texts))

    monkeypatch.setattr(EmbeddingMatcher, "_embed", fake_embed)
    monkeypatch.setattr(
        Manager, "_extract_concepts_for_embedding", lambda self, graph: list(_CONCEPTS)
    )
    monkeypatch.setattr(
        QUDTStore, "extract_concepts",
        staticmethod(lambda graph, kind_uri: list(_CONCEPTS)),
    )

    m2 = _make_manager(data_dir, exact_only=False)
    try:
        status = m2.embedding_status()
        assert status["semantic"] is True
        assert status["graph"]["state"] == "ready"
        assert status["qudt"]["state"] == "ready"
        assert status["graph"]["concepts"] == len(_CONCEPTS)
        assert embed_calls["n"] > 0, "second start must embed, not skip"
        assert any((data_dir / "embedding_cache" / "graph").iterdir())
        assert any((data_dir / "embedding_cache" / "qudt").iterdir())
    finally:
        m2.close()
