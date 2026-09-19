"""Unit tests for text matching (``acquirium.TextMatch``), with fakes.

No model is loaded and nothing is embedded for real: ``_embed``, the
fastembed model, the matchers and the QUDT converter are stubbed as each
section needs. Resolution quality against the real model and the bundled
ontologies is covered by ``tests/text_match/test_text_matching.py``.

Sections:

1. Matcher helpers        — local-name splitting, concept hash, surfaces/meta
2. Disk cache             — content-addressed cache hits and misses
3. Batching and model     — ``_embed`` ordering/dedup, FP32 model registration
4. Exact-only matcher     — index without vectors; semantic start after it
5. ConceptResolver policy — source routing, converter tier, context rerank
6. resolve_record         — joint unit / quantity-kind decode
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from acquirium.Server.manager import Manager
from acquirium.TextMatch.embedding_matcher import (
    DEFAULT_MODEL,
    EmbeddingMatcher,
    ResolveResult,
    _split_local_name,
    load_embedding_model,
)
from acquirium.TextMatch.qudt_store import QUDTStore
from acquirium.TextMatch.resolver import ConceptResolver
from acquirium.internals.qudt_units import UnitNotFound

_DIM = 8  # arbitrary small vector width for the stub


def _fake_vectors(n: int) -> np.ndarray:
    """Return *n* deterministic, L2-normalized stub vectors of width _DIM."""
    if n == 0:
        return np.empty((0, _DIM), dtype=np.float32)
    # Build a stable, non-degenerate matrix and L2-normalize each row.
    arr = np.arange(n * _DIM, dtype=np.float32).reshape(n, _DIM) + 1.0
    return arr / np.linalg.norm(arr, axis=1, keepdims=True)


# ══════════════════════════════════════════════════════════════════════
# 1. Matcher helpers
# ══════════════════════════════════════════════════════════════════════


# ── _split_local_name ──────────────────────────────────────


class TestSplitLocalName:
    def test_hash_camel_case(self):
        result = _split_local_name("http://example.org/ont#CamelCaseWord")
        assert result == ["camel", "case", "word"]

    def test_slash_underscore(self):
        result = _split_local_name("http://example.org/ont/snake_case_word")
        assert result == ["snake", "case", "word"]

    def test_hyphens(self):
        result = _split_local_name("http://example.org/ont#word-with-hyphens")
        assert result == ["word", "with", "hyphens"]

    def test_acronym(self):
        result = _split_local_name("http://example.org/ont#HTTPServer")
        assert "http" in result
        assert "server" in result

    def test_no_separator(self):
        result = _split_local_name("plainword")
        assert result == ["plainword"]

    def test_empty_string(self):
        result = _split_local_name("")
        assert result == []


# ── EmbeddingMatcher._concepts_hash ───────────────────────


class TestConceptsHash:
    def test_deterministic(self):
        concepts = [{"uri": "urn:a", "kind": "class"}, {"uri": "urn:b", "kind": "class"}]
        h1 = EmbeddingMatcher._concepts_hash(concepts)
        h2 = EmbeddingMatcher._concepts_hash(concepts)
        assert h1 == h2

    def test_order_independent(self):
        c1 = [{"uri": "urn:a", "kind": "class"}, {"uri": "urn:b", "kind": "class"}]
        c2 = [{"uri": "urn:b", "kind": "class"}, {"uri": "urn:a", "kind": "class"}]
        assert EmbeddingMatcher._concepts_hash(c1) == EmbeddingMatcher._concepts_hash(c2)

    def test_different_input_different_hash(self):
        c1 = [{"uri": "urn:a", "kind": "class"}]
        c2 = [{"uri": "urn:z", "kind": "predicate"}]
        assert EmbeddingMatcher._concepts_hash(c1) != EmbeddingMatcher._concepts_hash(c2)

    def test_nested_list_order_does_not_change_hash(self):
        c1 = [{
            "uri": "urn:a",
            "kind": "class",
            "label": "Alpha",
            "surfaces": ["alpha", "a"],
            "related": ["urn:z", "urn:y"],
        }]
        c2 = [{
            "uri": "urn:a",
            "kind": "class",
            "label": "Alpha",
            "surfaces": ["a", "alpha"],
            "related": ["urn:y", "urn:z"],
        }]
        assert EmbeddingMatcher._concepts_hash(c1) == EmbeddingMatcher._concepts_hash(c2)


# ── EmbeddingMatcher._build_surfaces_and_meta ─────────────


class TestBuildSurfacesAndMeta:
    def test_with_surfaces(self):
        concepts = [
            {"uri": "urn:a", "kind": "class", "label": "Pump", "surfaces": ["pump", "water pump"]},
        ]
        surfaces, meta = EmbeddingMatcher._build_surfaces_and_meta(concepts)
        assert "pump" in surfaces
        assert "water pump" in surfaces
        assert len(meta) == 2
        assert all(m["uri"] == "urn:a" for m in meta)

    def test_without_surfaces_falls_back_to_split(self):
        concepts = [
            {"uri": "http://example.org/ont#WaterPump", "kind": "class", "label": ""},
        ]
        surfaces, meta = EmbeddingMatcher._build_surfaces_and_meta(concepts)
        assert len(surfaces) >= 1
        assert "water pump" in surfaces

    def test_empty_input(self):
        surfaces, meta = EmbeddingMatcher._build_surfaces_and_meta([])
        assert surfaces == []
        assert meta == []

    def test_meta_structure(self):
        concepts = [
            {"uri": "urn:x", "kind": "predicate", "label": "hasUnit", "surfaces": ["has unit"]},
        ]
        _, meta = EmbeddingMatcher._build_surfaces_and_meta(concepts)
        assert meta[0]["uri"] == "urn:x"
        assert meta[0]["kind"] == "predicate"
        assert meta[0]["label"] == "hasUnit"
        assert meta[0]["surface"] == "has unit"


# ══════════════════════════════════════════════════════════════════════
# 2. Disk cache
# ══════════════════════════════════════════════════════════════════════


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


def _cache_concepts() -> list[dict[str, Any]]:
    """A small, representative concept list."""
    return [
        {"uri": "urn:t:A", "kind": "class", "label": "Alpha", "surfaces": ["alpha"]},
        {"uri": "urn:t:B", "kind": "class", "label": "Beta",  "surfaces": ["beta", "b"]},
        {"uri": "urn:t:C", "kind": "predicate", "label": "Gamma", "surfaces": ["gamma"]},
    ]


def _cache_matcher(cache_dir: Path) -> EmbeddingMatcher:
    return EmbeddingMatcher(model_name="stub-model", cache_dir=cache_dir)


def test_cache_hit_when_concepts_unchanged(tmp_path: Path, stub_embed: dict[str, int]) -> None:
    """Second matcher pointed at the same cache dir must NOT call _embed."""
    cache_dir = tmp_path / "cache"
    concepts = _cache_concepts()

    # First build: populates disk cache.
    m1 = _cache_matcher(cache_dir)
    m1.build_index(concepts)
    assert stub_embed["calls"] == 1, "first build_index must embed"
    # Two cache files written (.npz + .json) for one hash.
    cache_files = sorted(cache_dir.iterdir())
    assert len(cache_files) == 2
    assert any(p.suffix == ".npz" for p in cache_files)
    assert any(p.suffix == ".json" for p in cache_files)

    # Second matcher, fresh in-memory state, same cache dir, same concepts.
    m2 = _cache_matcher(cache_dir)
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
    concepts = _cache_concepts()

    _cache_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    shuffled = list(reversed(concepts))
    _cache_matcher(cache_dir).build_index(shuffled)
    assert stub_embed["calls"] == 1, (
        "input order must not affect the cache key (concepts are sorted in _concepts_hash)"
    )


def test_cache_miss_when_a_concept_changes(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """Mutating a single concept's label must miss the cache and re-embed."""
    cache_dir = tmp_path / "cache"
    concepts = _cache_concepts()

    _cache_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    # Change a label — alters the canonical JSON, hence the SHA hash.
    mutated = [dict(c) for c in concepts]
    mutated[0]["label"] = "Alpha-renamed"
    _cache_matcher(cache_dir).build_index(mutated)
    assert stub_embed["calls"] == 2, "label change must invalidate the cache"

    # Both hashes' artifacts should now coexist on disk (no GC of old entries).
    npz_files = list(cache_dir.glob("*_vectors.npz"))
    assert len(npz_files) == 2


def test_cache_miss_when_a_concept_is_added(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """Adding a brand-new concept must miss the cache."""
    cache_dir = tmp_path / "cache"
    concepts = _cache_concepts()

    _cache_matcher(cache_dir).build_index(concepts)
    assert stub_embed["calls"] == 1

    extended = concepts + [
        {"uri": "urn:t:D", "kind": "class", "label": "Delta", "surfaces": ["delta"]},
    ]
    _cache_matcher(cache_dir).build_index(extended)
    assert stub_embed["calls"] == 2, "adding a concept must invalidate the cache"


def test_no_cache_dir_means_no_cache(
    tmp_path: Path, stub_embed: dict[str, int]
) -> None:
    """With cache_dir=None, every build_index re-embeds; no files are written."""
    concepts = _cache_concepts()

    m = EmbeddingMatcher(model_name="stub-model", cache_dir=None)
    m.build_index(concepts)
    m.build_index(concepts)
    assert stub_embed["calls"] == 2, (
        "no cache configured → every build must embed"
    )
    # No accidental directory creation under tmp_path.
    assert list(tmp_path.iterdir()) == []


# ══════════════════════════════════════════════════════════════════════
# 3. Batching and model
# ══════════════════════════════════════════════════════════════════════


def _vector_for(text: str) -> np.ndarray:
    """A deterministic, non-normalized vector that identifies *text*."""
    return np.array([len(text), sum(map(ord, text)), 1.0], dtype=np.float32)


class _StubModel:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def embed(self, documents: list[str], **kwargs: Any):
        self.calls.append((list(documents), kwargs))
        return (_vector_for(t) for t in documents)


@pytest.fixture
def matcher() -> tuple[EmbeddingMatcher, _StubModel]:
    m = EmbeddingMatcher()
    stub = _StubModel()
    m._model = stub
    return m, stub


def test_embed_returns_rows_in_caller_order(matcher):
    m, _stub = matcher
    texts = ["kilogram per cubic metre", "kg", "milligram per litre", "L", "pascal"]

    out = m._embed(texts)

    expected = np.array([_vector_for(t) for t in texts])
    expected /= np.linalg.norm(expected, axis=1, keepdims=True)
    assert out.shape == (len(texts), 3)
    np.testing.assert_allclose(out, expected, rtol=1e-6)


def test_embed_sends_each_distinct_text_once_in_length_order(matcher):
    m, stub = matcher
    texts = ["pascal", "kg", "kilogram per cubic metre", "kg", "L", "pascal"]

    out = m._embed(texts)

    assert len(stub.calls) == 1
    sent, kwargs = stub.calls[0]
    assert sent == ["L", "kg", "pascal", "kilogram per cubic metre"]
    assert kwargs == {"batch_size": 64}
    # Repeated texts share one embedding.
    np.testing.assert_array_equal(out[1], out[3])
    np.testing.assert_array_equal(out[0], out[5])


def test_embed_order_is_deterministic_for_equal_lengths(matcher):
    m, stub = matcher

    m._embed(["kg", "mg", "cm"])
    m._embed(["cm", "kg", "mg"])

    assert stub.calls[0][0] == stub.calls[1][0] == ["cm", "kg", "mg"]


def test_embed_single_query_text(matcher):
    m, _stub = matcher

    out = m._embed(["flow rate"])

    assert out.shape == (1, 3)
    np.testing.assert_allclose(np.linalg.norm(out, axis=1), 1.0, rtol=1e-6)


@pytest.fixture
def no_model_load(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Patch TextEmbedding.__init__ so constructing one loads nothing."""
    from fastembed import TextEmbedding

    created: list[tuple[str, Any]] = []

    def fake_init(self, model_name, cache_dir=None, **kwargs):
        created.append((model_name, cache_dir))

    monkeypatch.setattr(TextEmbedding, "__init__", fake_init)
    return created


def test_default_model_is_the_fp32_bge_small_export(no_model_load):
    from fastembed import TextEmbedding

    load_embedding_model()
    # A second matcher loads the model again; registering twice must not raise.
    load_embedding_model(cache_dir="/some/cache")

    assert no_model_load == [(DEFAULT_MODEL, None), (DEFAULT_MODEL, "/some/cache")]
    entries = [
        m for m in TextEmbedding.list_supported_models() if m["model"] == DEFAULT_MODEL
    ]
    assert len(entries) == 1
    assert entries[0]["sources"]["hf"] == "BAAI/bge-small-en-v1.5"
    assert entries[0]["model_file"] == "onnx/model.onnx"
    assert entries[0]["dim"] == 384


def test_other_model_names_pass_through(no_model_load):
    load_embedding_model("BAAI/bge-base-en-v1.5")

    assert no_model_load == [("BAAI/bge-base-en-v1.5", None)]


# ══════════════════════════════════════════════════════════════════════
# 4. Exact-only matcher
# ══════════════════════════════════════════════════════════════════════


_EXACT_CONCEPTS = [
    {"uri": "urn:t:Pump", "kind": "class", "label": "Pump", "surfaces": ["pump"]},
    {"uri": "urn:t:KG", "kind": "unit", "label": "Kilogram", "surfaces": ["kg"]},
]


def test_exact_only_matcher_never_embeds(tmp_path: Path) -> None:
    """The index builds and answers without a model or a cache file."""
    cache = tmp_path / "cache"
    m = EmbeddingMatcher(cache_dir=cache, model_cache_dir=tmp_path / "models",
                         exact_only=True)
    m._embed = lambda texts: pytest.fail("exact-only must not embed")  # type: ignore[assignment]

    m.build_index(_EXACT_CONCEPTS)

    assert m.is_ready
    assert not cache.exists(), "exact-only holds no vectors, so it writes no cache"
    hits = m.query("pump", kind="class")
    assert [h.uri for h in hits] == ["urn:t:Pump"]
    assert hits[0].score == 1.0
    assert hits[0].match_stage == "exact"


def test_exact_only_matcher_filters_by_kind(tmp_path: Path) -> None:
    """A concept only ever answers for the kind it was indexed under."""
    m = EmbeddingMatcher(cache_dir=tmp_path / "cache", exact_only=True)
    m.build_index(_EXACT_CONCEPTS)

    assert m.query("pump", kind="class")
    assert m.query("pump", kind="unit") == []
    assert m.query("kg", kind="unit")
    assert m.query("kg", kind="class") == []


def test_exact_only_matcher_has_no_fuzzy_matching(tmp_path: Path) -> None:
    m = EmbeddingMatcher(cache_dir=tmp_path / "cache", exact_only=True)
    m.build_index(_EXACT_CONCEPTS)
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
    m.build_index(_EXACT_CONCEPTS)

    hits = m.query("pump", kind="class", top_k=2, min_score=0.0)
    assert hits[0].uri == "urn:t:Pump"
    assert hits[0].match_stage == "exact"
    assert m.query("pumping station", kind="class", min_score=0.0), (
        "a semantic index must still answer a near-miss"
    )


def _make_manager(data_dir: Path, *, exact_only: bool) -> Manager:
    return Manager(
        data_dir=data_dir,
        timeseries_backend="duckdb",
        exact_only=exact_only,
    )


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
        Manager, "_extract_concepts_for_embedding", lambda self, graph: list(_EXACT_CONCEPTS)
    )
    monkeypatch.setattr(
        QUDTStore, "extract_concepts",
        staticmethod(lambda graph, kind_uri: list(_EXACT_CONCEPTS)),
    )

    m2 = _make_manager(data_dir, exact_only=False)
    try:
        status = m2.embedding_status()
        assert status["semantic"] is True
        assert status["graph"]["state"] == "ready"
        assert status["qudt"]["state"] == "ready"
        assert status["graph"]["concepts"] == len(_EXACT_CONCEPTS)
        assert embed_calls["n"] > 0, "second start must embed, not skip"
        assert any((data_dir / "embedding_cache" / "graph").iterdir())
        assert any((data_dir / "embedding_cache" / "qudt").iterdir())
    finally:
        m2.close()


# ══════════════════════════════════════════════════════════════════════
# 5. ConceptResolver policy
# ══════════════════════════════════════════════════════════════════════


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

    def infer_unit(self, text, *, fuzzy=True):
        self.calls.append(f"infer:{text}")
        if text in self._mapping:
            return self._mapping[text]
        raise UnitNotFound(text)

def _rr(uri, kind, score=0.7, *, stage="semantic", related=()):
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

    def test_substance_uses_graph_only(self):
        g = FakeMatcher([_rr("urn:nawi#Chlorine", "substance")])
        q = FakeMatcher([_rr("urn:Q", "substance")])
        r = ConceptResolver(g, q, lambda: None)
        out = r.resolve("chlorine", kind="substance")
        assert [m.uri for m in out] == ["urn:nawi#Chlorine"]
        assert q.queried_kinds == []  # QUDT/converter untouched for substance

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


class TestProcessKindRouting:
    def test_graph_source_serves_process_kind(self):
        """Regression: the process extraction kind existed but the graph
        source's kind set didn't include it, so resolve(kind='process')
        silently returned nothing."""
        r = _resolver(graph=[
            _rr("urn:nawi-water-ontology#Process-ReverseOsmosis", "process", score=0.9),
        ])
        out = r.resolve("reverse osmosis", kind="process", min_score=0.4)
        assert out and out[0].uri == "urn:nawi-water-ontology#Process-ReverseOsmosis"


# ══════════════════════════════════════════════════════════════════════
# 6. resolve_record
# ══════════════════════════════════════════════════════════════════════


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
