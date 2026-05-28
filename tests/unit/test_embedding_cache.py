"""Tests for EmbeddingMatcher's content-addressed disk cache.

The cache is keyed by ``_concepts_hash(concepts)`` — a SHA-256 of a
canonicalized JSON of the concept list, sorted by ``(uri, kind, label)``.
That means re-running ``build_index`` with the same logical concepts
must hit the cache (no embedding work), while any change to the
concept payloads must miss the cache and re-embed.

Embedding the real model would pull in fastembed and a network model
download, so we monkeypatch ``EmbeddingMatcher._embed`` to a counter
stub that returns deterministic L2-normalized vectors.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher


_DIM = 4  # arbitrary small vector width for the stub


def _fake_vectors(n: int) -> np.ndarray:
    """Return *n* deterministic, L2-normalized stub vectors of width _DIM."""
    if n == 0:
        return np.empty((0, _DIM), dtype=np.float32)
    # Build a stable, non-degenerate matrix and L2-normalize each row.
    arr = np.arange(n * _DIM, dtype=np.float32).reshape(n, _DIM) + 1.0
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    return arr / norms


@pytest.fixture
def stub_embed(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Replace EmbeddingMatcher._embed with a counter; return call-count dict.

    Returning a dict (not an int) so the closure can mutate it and tests
    can read the latest count by name.
    """
    counter = {"calls": 0, "last_n": 0}

    def fake_embed(self: EmbeddingMatcher, texts: list[str]) -> np.ndarray:
        counter["calls"] += 1
        counter["last_n"] = len(texts)
        return _fake_vectors(len(texts))

    monkeypatch.setattr(EmbeddingMatcher, "_embed", fake_embed)
    return counter


def _concepts() -> list[dict[str, Any]]:
    """A small, representative concept list."""
    return [
        {"uri": "urn:t:A", "kind": "class", "label": "Alpha", "surfaces": ["alpha"]},
        {"uri": "urn:t:B", "kind": "class", "label": "Beta",  "surfaces": ["beta", "b"]},
        {"uri": "urn:t:C", "kind": "predicate", "label": "Gamma", "surfaces": ["gamma"]},
    ]


def _make_matcher(cache_dir: Path) -> EmbeddingMatcher:
    return EmbeddingMatcher(model_name="stub-model", cache_dir=cache_dir)


def test_cache_hit_when_concepts_unchanged(tmp_path: Path, stub_embed: dict[str, int]) -> None:
    """Second matcher pointed at the same cache dir must NOT call _embed."""
    cache_dir = tmp_path / "cache"
    concepts = _concepts()

    # First build: populates disk cache.
    m1 = _make_matcher(cache_dir)
    m1.build_index(concepts)
    assert stub_embed["calls"] == 1, "first build_index must embed"
    # Two cache files written (.npz + .json) for one hash.
    cache_files = sorted(cache_dir.iterdir())
    assert len(cache_files) == 2
    assert any(p.suffix == ".npz" for p in cache_files)
    assert any(p.suffix == ".json" for p in cache_files)

    # Second matcher, fresh in-memory state, same cache dir, same concepts.
    m2 = _make_matcher(cache_dir)
    m2.build_index(concepts)
    assert stub_embed["calls"] == 1, "second build_index must hit cache"
    # Loaded index must reflect the original concept list.
    assert len(m2._meta) == len(m1._meta) > 0
    assert {row["uri"] for row in m2._meta} == {row["uri"] for row in m1._meta}


def test_cache_hit_is_stable_across_input_order(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """Reordering the input concept list must yield the same hash → cache hit."""
    cache_dir = tmp_path / "cache"
    concepts = _concepts()

    _make_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    shuffled = list(reversed(concepts))
    _make_matcher(cache_dir).build_index(shuffled)
    assert stub_embed["calls"] == 1, (
        "input order must not affect the cache key (concepts are sorted in _concepts_hash)"
    )


def test_cache_miss_when_a_concept_changes(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """Mutating a single concept's label must miss the cache and re-embed."""
    cache_dir = tmp_path / "cache"
    concepts = _concepts()

    _make_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    # Change a label — alters the canonical JSON, hence the SHA hash.
    mutated = [dict(c) for c in concepts]
    mutated[0]["label"] = "Alpha-renamed"
    _make_matcher(cache_dir).build_index(mutated)
    assert stub_embed["calls"] == 2, "label change must invalidate the cache"

    # Both hashes' artifacts should now coexist on disk (no GC of old entries).
    npz_files = list(cache_dir.glob("*_vectors.npz"))
    assert len(npz_files) == 2


def test_cache_miss_when_a_concept_is_added(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """Adding a brand-new concept must miss the cache."""
    cache_dir = tmp_path / "cache"
    concepts = _concepts()

    _make_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    extended = concepts + [
        {"uri": "urn:t:D", "kind": "class", "label": "Delta", "surfaces": ["delta"]},
    ]
    _make_matcher(cache_dir).build_index(extended)
    assert stub_embed["calls"] == 2, "adding a concept must invalidate the cache"


def test_no_cache_dir_means_no_cache(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """With cache_dir=None, every build_index re-embeds; no files are written."""
    concepts = _concepts()

    m = EmbeddingMatcher(model_name="stub-model", cache_dir=None)
    m.build_index(concepts)
    m.build_index(concepts)
    assert stub_embed["calls"] == 2, (
        "no cache configured → every build must embed"
    )
    # No accidental directory creation under tmp_path.
    assert list(tmp_path.iterdir()) == []
