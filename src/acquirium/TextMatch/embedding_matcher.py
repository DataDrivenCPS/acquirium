from __future__ import annotations

import hashlib
import json
import logging
import re
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

        # Index state
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

    def build_index(self, concepts: list[dict[str, Any]]) -> None:
        """Build embedding index from concept dicts with keys: uri, kind, label, surfaces."""
        new_hash = self._concepts_hash(concepts)

        # Check disk cache
        if self._cache_dir and self._try_load_cache(new_hash):
            logger.info(
                "Loaded embedding index from cache (%d entries)", len(self._meta)
            )
            return

        # Build surface list and metadata
        surfaces: list[str] = []
        meta: list[dict[str, str]] = []
        for concept in concepts:
            uri = concept["uri"]
            kind = concept.get("kind", "class")
            label = concept.get("label", "")
            concept_surfaces = concept.get("surfaces", [])

            if not concept_surfaces:
                # Fallback: split URI local name
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

        if not surfaces:
            logger.warning("No surfaces to embed; index will be empty")
            self._vectors = np.empty((0, 1), dtype=np.float32)
            self._meta = []
            self._index_hash = new_hash
            return

        logger.info("Embedding %d surfaces from %d concepts...", len(surfaces), len(concepts))
        self._vectors = self._embed(surfaces)
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
        if self._vectors is None or len(self._meta) == 0:
            return []

        q_vec = self._embed([text])  # shape (1, dim)
        scores = (q_vec @ self._vectors.T).squeeze(0)  # shape (N,)

        # Filter by kind if specified
        if kind:
            mask = np.array([m["kind"] == kind for m in self._meta], dtype=bool)
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
            m = self._meta[idx]
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

    def update_index(self, added_concepts: list[dict[str, Any]], removed_uris: list[str]) -> None:
        """Incrementally update the embedding index: remove old URIs, add new concepts."""
        if self._vectors is None or len(self._meta) == 0:
            # No existing index — fall back to full build
            if added_concepts:
                self.build_index(added_concepts)
            return

        # 1. Filter out removed URIs
        removed_set = set(removed_uris)
        if removed_set:
            keep = [i for i, m in enumerate(self._meta) if m["uri"] not in removed_set]
            self._meta = [self._meta[i] for i in keep]
            self._vectors = self._vectors[keep]

        # 2. Build surfaces for added concepts and embed them
        if added_concepts:
            new_surfaces: list[str] = []
            new_meta: list[dict[str, str]] = []
            for concept in added_concepts:
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
                    new_surfaces.append(surface)
                    new_meta.append({
                        "uri": uri,
                        "kind": kind,
                        "label": label or surface,
                        "surface": surface,
                    })

            if new_surfaces:
                logger.info("Embedding %d new surfaces from %d added concepts...", len(new_surfaces), len(added_concepts))
                new_vectors = self._embed(new_surfaces)
                # 3. Concatenate
                self._vectors = np.concatenate([self._vectors, new_vectors], axis=0)
                self._meta = self._meta + new_meta

        # 4. Recompute hash and save
        all_concepts_for_hash = []
        seen: set[str] = set()
        for m in self._meta:
            if m["uri"] not in seen:
                seen.add(m["uri"])
                all_concepts_for_hash.append({"uri": m["uri"]})
        self._index_hash = self._concepts_hash(all_concepts_for_hash)

        if self._cache_dir:
            self._save_cache(self._index_hash)

        logger.info("Updated embedding index: %d total entries", len(self._meta))

    @property
    def is_ready(self) -> bool:
        """Return True if the index has been built and has entries."""
        return self._vectors is not None and len(self._meta) > 0

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
            data = np.load(vec_path)
            self._vectors = data["vectors"]
            self._meta = json.loads(meta_path.read_text())
            self._index_hash = hash_val
            return True
        except Exception:
            logger.warning("Failed to load embedding cache, will rebuild")
            return False

    def _save_cache(self, hash_val: str) -> None:
        try:
            vec_path, meta_path = self._cache_path(hash_val)
            np.savez_compressed(vec_path, vectors=self._vectors)
            meta_path.write_text(json.dumps(self._meta, ensure_ascii=True))
            logger.info("Saved embedding cache to %s", self._cache_dir)
        except Exception:
            logger.warning("Failed to save embedding cache", exc_info=True)
