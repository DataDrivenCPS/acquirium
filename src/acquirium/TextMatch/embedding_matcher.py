from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger("acquirium.embedding_matcher")


@dataclass
class ResolveResult:
    uri: str
    kind: str
    label: str
    score: float
    matched_surface: str


def _split_local_name(uri: str) -> list[str]:
    """Split a URI local name on CamelCase, underscores, and hyphens into lowercase tokens."""
    # Extract local name from URI
    for sep in ("#", "/"):
        if sep in uri:
            local = uri.rsplit(sep, 1)[-1]
            break
    else:
        local = uri

    # Split CamelCase
    tokens = re.sub(r"([a-z])([A-Z])", r"\1 \2", local)
    tokens = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", tokens)
    # Split on underscores and hyphens
    parts = re.split(r"[_\-\s]+", tokens)
    return [p.lower() for p in parts if p]


class EmbeddingMatcher:
    def __init__(
        self,
        model_name: str = "BAAI/bge-small-en-v1.5",
        cache_dir: str | Path | None = None,
    ) -> None:
        self._model_name = model_name
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._model = None  # lazy
        self._lock = threading.Lock()

        # Index state — always read/swapped under self._lock
        self._vectors: np.ndarray | None = None  # shape (N, dim), L2-normalized
        self._meta: list[dict[str, str]] = []  # parallel list: uri, kind, label, surface
        self._index_hash: str | None = None

    def _ensure_model(self) -> None:
        if self._model is not None:
            return
        from fastembed import TextEmbedding

        self._model = TextEmbedding(self._model_name)

    def _embed(self, texts: list[str]) -> np.ndarray:
        self._ensure_model()
        # fastembed returns a generator of numpy arrays
        vecs = list(self._model.embed(texts))
        arr = np.array(vecs, dtype=np.float32)
        # L2-normalize so dot product = cosine similarity
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        return arr / norms

    @staticmethod
    def _concepts_hash(concepts: list[dict[str, Any]]) -> str:
        canonical = json.dumps(
            sorted(concepts, key=lambda c: c["uri"]),
            sort_keys=True,
            ensure_ascii=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _build_surfaces_and_meta(concepts: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, str]]]:
        """Extract surface strings and parallel metadata from concept dicts."""
        surfaces: list[str] = []
        meta: list[dict[str, str]] = []
        for concept in concepts:
            uri = concept["uri"]
            kind = concept.get("kind", "class")
            label = concept.get("label", "")
            concept_surfaces = concept.get("surfaces", [])

            if not concept_surfaces:
                tokens = _split_local_name(uri)
                if tokens:
                    concept_surfaces = [" ".join(tokens)]

            if not concept_surfaces:
                continue

            for surface in concept_surfaces:
                surfaces.append(surface)
                meta.append(
                    {
                        "uri": uri,
                        "kind": kind,
                        "label": label or surface,
                        "surface": surface,
                    }
                )
        return surfaces, meta

    def build_index(self, concepts: list[dict[str, Any]]) -> None:
        """Build embedding index from concept dicts with keys: uri, kind, label, surfaces."""
        new_hash = self._concepts_hash(concepts)

        # Check disk cache
        if self._cache_dir and self._try_load_cache(new_hash):
            logger.info(
                "Loaded embedding index from cache (%d entries)", len(self._meta)
            )
            return

        surfaces, meta = self._build_surfaces_and_meta(concepts)

        if not surfaces:
            logger.warning("No surfaces to embed; index will be empty")
            with self._lock:
                self._vectors = np.empty((0, 1), dtype=np.float32)
                self._meta = []
                self._index_hash = new_hash
            return

        logger.info("Embedding %d surfaces from %d concepts...", len(surfaces), len(concepts))
        # Embed outside the lock (expensive I/O)
        vectors = self._embed(surfaces)

        # Atomic swap under lock
        with self._lock:
            self._vectors = vectors
            self._meta = meta
            self._index_hash = new_hash

        # Save to disk cache
        if self._cache_dir:
            self._save_cache(new_hash)

        logger.info("Embedding index built with %d entries", len(meta))

    def query(
        self,
        text: str,
        kind: str | None = None,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> list[ResolveResult]:
        # Snapshot state under lock so reads are consistent
        with self._lock:
            vectors = self._vectors
            meta = self._meta

        if vectors is None or len(meta) == 0:
            return []

        q_vec = self._embed([text])  # shape (1, dim)
        scores = (q_vec @ vectors.T).squeeze(0)  # shape (N,)

        # Filter by kind if specified
        if kind:
            mask = np.array([m["kind"] == kind for m in meta], dtype=bool)
            scores = np.where(mask, scores, -1.0)

        # Get top_k indices
        # We need more than top_k to handle deduplication
        n_candidates = min(len(scores), top_k * 3)
        if n_candidates >= len(scores):
            # All entries fit, just sort
            top_indices = np.argsort(-scores)
        else:
            top_indices = np.argpartition(-scores, n_candidates)[:n_candidates]
            top_indices = top_indices[np.argsort(-scores[top_indices])]

        # Deduplicate by URI (keep highest score per URI)
        seen_uris: set[str] = set()
        results: list[ResolveResult] = []
        for idx in top_indices:
            s = float(scores[idx])
            if s < min_score:
                break
            m = meta[idx]
            if m["uri"] in seen_uris:
                continue
            seen_uris.add(m["uri"])
            results.append(
                ResolveResult(
                    uri=m["uri"],
                    kind=m["kind"],
                    label=m["label"],
                    score=s,
                    matched_surface=m["surface"],
                )
            )
            if len(results) >= top_k:
                break

        return results

    def update_index(
        self,
        added_concepts: list[dict[str, Any]],
        removed_uris: list[str],
        all_concepts: list[dict[str, Any]] | None = None,
    ) -> None:
        """Incrementally update the embedding index: remove old URIs, add new concepts.

        Args:
            added_concepts: Only the newly added concepts to embed.
            removed_uris: URIs to remove from the existing index.
            all_concepts: The complete concept list (for correct cache hashing).
                          If None, falls back to full build from added_concepts.
        """
        with self._lock:
            vectors = self._vectors
            meta = list(self._meta) if self._meta else []

        if vectors is None or len(meta) == 0:
            # No existing index — need full concept list to build correctly
            if all_concepts:
                self.build_index(all_concepts)
            elif added_concepts:
                self.build_index(added_concepts)
            return

        # 1. Filter out removed URIs
        removed_set = set(removed_uris)
        if removed_set:
            keep = [i for i, m in enumerate(meta) if m["uri"] not in removed_set]
            meta = [meta[i] for i in keep]
            vectors = vectors[keep]

        # 2. Build surfaces for added concepts and embed them
        if added_concepts:
            new_surfaces, new_meta = self._build_surfaces_and_meta(added_concepts)

            if new_surfaces:
                logger.info("Embedding %d new surfaces from %d added concepts...", len(new_surfaces), len(added_concepts))
                new_vectors = self._embed(new_surfaces)
                vectors = np.concatenate([vectors, new_vectors], axis=0)
                meta = meta + new_meta

        # 3. Compute hash from full concept list (matches build_index output)
        if all_concepts is not None:
            new_hash = self._concepts_hash(all_concepts)
        else:
            # Fallback: hash from current meta URIs (won't match build_index cache)
            seen: set[str] = set()
            unique = []
            for m in meta:
                if m["uri"] not in seen:
                    seen.add(m["uri"])
                    unique.append({"uri": m["uri"]})
            new_hash = self._concepts_hash(unique)

        # 4. Atomic swap under lock
        with self._lock:
            self._vectors = vectors
            self._meta = meta
            self._index_hash = new_hash

        if self._cache_dir:
            self._save_cache(new_hash)

        logger.info("Updated embedding index: %d total entries", len(meta))

    @property
    def is_ready(self) -> bool:
        """Return True if the index has been built and has entries."""
        with self._lock:
            return self._vectors is not None and len(self._meta) > 0

    def get_indexed_uris(self) -> set[str]:
        """Return the set of URIs currently in the index (thread-safe)."""
        with self._lock:
            return {m["uri"] for m in self._meta}

    # --- Disk caching ---

    def _cache_path(self, prefix: str) -> tuple[Path, Path]:
        assert self._cache_dir is not None
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        return (
            self._cache_dir / f"{prefix}_vectors.npz",
            self._cache_dir / f"{prefix}_meta.json",
        )

    def _try_load_cache(self, hash_val: str) -> bool:
        try:
            vec_path, meta_path = self._cache_path(hash_val)
            if not vec_path.exists() or not meta_path.exists():
                return False
            vectors = np.load(vec_path)["vectors"]
            meta = json.loads(meta_path.read_text())
            with self._lock:
                self._vectors = vectors
                self._meta = meta
                self._index_hash = hash_val
            return True
        except Exception:
            logger.warning("Failed to load embedding cache, will rebuild")
            return False

    def _save_cache(self, hash_val: str) -> None:
        try:
            with self._lock:
                vectors = self._vectors
                meta = self._meta
            vec_path, meta_path = self._cache_path(hash_val)
            np.savez_compressed(vec_path, vectors=vectors)
            meta_path.write_text(json.dumps(meta, ensure_ascii=True))
            logger.info("Saved embedding cache to %s", self._cache_dir)
        except Exception:
            logger.warning("Failed to save embedding cache", exc_info=True)
