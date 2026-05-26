"""Tests for pure helpers in acquirium.TextMatch.embedding_matcher."""

import pytest

from acquirium.TextMatch.embedding_matcher import (
    _split_local_name,
    EmbeddingMatcher,
)


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
