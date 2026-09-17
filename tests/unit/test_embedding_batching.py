"""Tests for how EmbeddingMatcher feeds texts to the model.

``_embed`` embeds each distinct text once, in length order, and returns
the rows in the caller's order. ``load_embedding_model`` registers the
FP32 export of bge-small with fastembed under ``DEFAULT_MODEL``.

No model is loaded: ``_embed`` runs against a stub model, and
``TextEmbedding.__init__`` is patched out for the registration tests.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

from acquirium.TextMatch.embedding_matcher import (
    DEFAULT_MODEL,
    EmbeddingMatcher,
    load_embedding_model,
)


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
