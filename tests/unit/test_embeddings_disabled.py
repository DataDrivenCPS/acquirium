"""Behavioral tests for Manager(embeddings=False).

Exercises a *real* Manager (Oxigraph + ontoenv + DuckDB under a tmp dir, no
Docker) with embeddings disabled: no matchers are constructed, nothing is
written under embedding_cache/, status reports disabled, and resolution
still works through the deterministic converter and the exact-label lookup.
The warm-start test proves a later start *with* embeddings builds the
indexes even when the data dir was first populated by a no-embeddings run.
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


def _fake_vectors(n: int) -> np.ndarray:
    if n == 0:
        return np.empty((0, _DIM), dtype=np.float32)
    arr = np.arange(n * _DIM, dtype=np.float32).reshape(n, _DIM) + 1.0
    return arr / np.linalg.norm(arr, axis=1, keepdims=True)


def _make_manager(data_dir: Path, *, embeddings: bool) -> Manager:
    return Manager(
        data_dir=data_dir,
        timeseries_backend="duckdb",
        embeddings=embeddings,
    )


@pytest.fixture(scope="module")
def disabled_manager(tmp_path_factory: pytest.TempPathFactory):
    data_dir = tmp_path_factory.mktemp("acq-no-emb")
    m = _make_manager(data_dir, embeddings=False)
    yield m
    m.close()


def test_status_reports_disabled(disabled_manager: Manager) -> None:
    status = disabled_manager.embedding_status()
    assert status["enabled"] is False
    assert status["graph"]["state"] == "disabled"
    assert status["qudt"]["state"] == "disabled"


def test_no_matchers_and_no_cache_dir(disabled_manager: Manager) -> None:
    assert disabled_manager._graph_matcher is None
    assert disabled_manager._qudt_matcher is None
    assert disabled_manager.embedding_matcher is None
    assert not (disabled_manager.data_dir / "embedding_cache").exists()
    assert "fastembed" not in sys.modules


def test_unit_resolution_via_converter(disabled_manager: Manager) -> None:
    """kind="unit" falls through to the deterministic QUDT converter."""
    results = disabled_manager.resolve_text("mg/L", kind="unit", top_k=3)
    assert results, "converter tier must resolve an exact unit symbol"
    top = results[0]
    assert top["score"] == 1.0
    assert top["match_stage"] == "exact"
    assert "MilliGM-PER-L" in top["uri"]


def test_label_fallback_resolves_class(disabled_manager: Manager) -> None:
    """An exact ontology label resolves without any embedding index."""
    results = disabled_manager.resolve_text("aeration basin", kind="class")
    assert results, "exact rdfs:label match must resolve"
    top = results[0]
    assert top["uri"].endswith("AerationBasin")
    assert top["score"] == 1.0
    assert top["match_stage"] == "exact"


def test_unknown_text_returns_empty(disabled_manager: Manager) -> None:
    assert disabled_manager.resolve_text("zzz total gibberish", kind="class") == []


def test_from_env_reads_embeddings_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_init(self: Manager, *args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(Manager, "__init__", fake_init)
    monkeypatch.delenv("ACQUIRIUM_CONFIG", raising=False)
    monkeypatch.setenv("ACQUIRIUM_EMBEDDINGS", "false")
    Manager.from_env()
    assert captured["embeddings"] is False

    monkeypatch.delenv("ACQUIRIUM_EMBEDDINGS")
    captured.clear()
    Manager.from_env()
    assert captured["embeddings"] is True


def test_warm_start_with_embeddings_builds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A no-embeddings run must not stop a later embeddings run from building.

    First start opens the data dir with embeddings off; the second opens the
    same (warm) dir with embeddings on and must actually build both indexes.
    Extraction and the model are stubbed so the test stays fast — what's
    asserted is that the build pipeline ran and populated the disk cache.
    """
    data_dir = tmp_path / "acq"

    m1 = _make_manager(data_dir, embeddings=False)
    m1.close()
    assert not (data_dir / "embedding_cache").exists()

    concepts = [
        {"uri": "urn:t:A", "kind": "class", "label": "Alpha", "surfaces": ["alpha"]},
        {"uri": "urn:t:B", "kind": "predicate", "label": "Beta", "surfaces": ["beta"]},
    ]
    embed_calls = {"n": 0}

    def fake_embed(self: EmbeddingMatcher, texts: list[str]) -> np.ndarray:
        embed_calls["n"] += 1
        return _fake_vectors(len(texts))

    monkeypatch.setattr(EmbeddingMatcher, "_embed", fake_embed)
    monkeypatch.setattr(
        Manager, "_extract_concepts_for_embedding", lambda self, graph: list(concepts)
    )
    monkeypatch.setattr(
        QUDTStore, "extract_concepts", staticmethod(lambda graph, kind_uri: list(concepts))
    )

    m2 = _make_manager(data_dir, embeddings=True)
    try:
        status = m2.embedding_status()
        assert status["enabled"] is True
        assert status["graph"]["state"] == "ready"
        assert status["qudt"]["state"] == "ready"
        assert status["graph"]["concepts"] == len(concepts)
        assert embed_calls["n"] > 0, "second start must embed, not skip"
        assert any((data_dir / "embedding_cache" / "graph").iterdir())
        assert any((data_dir / "embedding_cache" / "qudt").iterdir())
    finally:
        m2.close()
