from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np

logger = logging.getLogger("acquirium.embedding_matcher")

MatchStage = Literal["exact", "semantic"]


@dataclass
class ResolveResult:
    uri: str
    kind: str
    label: str
    score: float
    matched_surface: str
    # "exact" (surface lookup, score 1.0) or "semantic" (embedding similarity)
    match_stage: MatchStage = "semantic"
    # URIs this concept links to (e.g. a unit's quantity kinds), used for
    # context disambiguation in Manager.resolve_text. Empty if none captured.
    related: tuple[str, ...] = ()


def _normalize_surface(text: str) -> str:
    """Lower-case, strip, and collapse whitespace runs.

    Used for both index-time and query-time exact-match keys.
    """
    return re.sub(r"\s+", " ", text.strip().lower())


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

        # Index state — read/swapped under self._lock via _set_index().
        self._vectors: np.ndarray | None = None  # shape (N, dim), L2-normalized
        self._meta: list[dict[str, Any]] = []  # parallel: uri, kind, label, surface, related
        self._index_hash: str | None = None
        # normalized surface -> meta row indices, derived from _meta
        self._surface_index: dict[str, list[int]] = {}

    def _set_index(
        self,
        vectors: np.ndarray | None,
        meta: list[dict[str, Any]],
        index_hash: str | None,
    ) -> None:
        """Swap the index under the lock and rebuild the surface map.

        All index mutations (build, update, cache load) route through here so
        _surface_index stays in sync with _meta.
        """
        surface_index: dict[str, list[int]] = {}
        for i, m in enumerate(meta):
            surface_index.setdefault(_normalize_surface(m["surface"]), []).append(i)
        with self._lock:
            self._vectors = vectors
            self._meta = meta
            self._index_hash = index_hash
            self._surface_index = surface_index

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
    def _build_surfaces_and_meta(concepts: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
        """Extract surface strings and parallel metadata from concept dicts."""
        surfaces: list[str] = []
        meta: list[dict[str, Any]] = []
        for concept in concepts:
            uri = concept["uri"]
            kind = concept.get("kind", "class")
            label = concept.get("label", "")
            related = concept.get("related", [])
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
                        "related": related,
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
            self._set_index(np.empty((0, 1), dtype=np.float32), [], new_hash)
            return

        logger.info("Embedding %d surfaces from %d concepts...", len(surfaces), len(concepts))
        # Embed outside the lock (expensive I/O)
        vectors = self._embed(surfaces)
        self._set_index(vectors, meta, new_hash)

        # Save to disk cache
        if self._cache_dir:
            self._save_cache(new_hash)

        logger.info("Embedding index built with %d entries", len(meta))

    @staticmethod
    def _row_to_result(m: dict[str, Any], score: float, stage: MatchStage) -> ResolveResult:
        return ResolveResult(
            uri=m["uri"],
            kind=m["kind"],
            label=m["label"],
            score=score,
            matched_surface=m["surface"],
            match_stage=stage,
            related=tuple(m.get("related", ())),
        )

    def query(
        self,
        text: str,
        kind: str | None = None,
        top_k: int = 5,
        min_score: float = 0.5,
    ) -> list[ResolveResult]:
        """Resolve *text* to concepts.

        Stage 1: normalized surface lookup (score 1.0). Stage 2: embedding
        cosine similarity, filling remaining slots and skipping URIs already
        returned by stage 1. Stage 1 first so short symbols ("kg", "mg/L")
        don't depend on cosine similarity over very short tokens.
        """
        # Snapshot together so _vectors / _meta / _surface_index stay aligned.
        with self._lock:
            vectors = self._vectors
            meta = self._meta
            surface_index = self._surface_index

        if vectors is None or len(meta) == 0:
            return []

        results: list[ResolveResult] = []
        seen_uris: set[str] = set()

        exact_hits = self._exact_stage(text, kind, meta, surface_index, top_k)
        for r in exact_hits:
            if r.uri not in seen_uris:
                seen_uris.add(r.uri)
                results.append(r)

        if len(results) < top_k:
            semantic_hits = self._semantic_stage(
                text, kind, vectors, meta, top_k, min_score, seen_uris
            )
            results.extend(semantic_hits)

        logger.debug(
            "query(%r, kind=%s) -> %d exact + %d semantic",
            text, kind, len(exact_hits), len(results) - len(exact_hits),
        )
        return results[:top_k]

    def _exact_stage(
        self,
        text: str,
        kind: str | None,
        meta: list[dict[str, Any]],
        surface_index: dict[str, list[int]],
        top_k: int,
    ) -> list[ResolveResult]:
        """Normalized exact surface lookup. Hits get score 1.0."""
        rows = surface_index.get(_normalize_surface(text))
        if not rows:
            return []
        hits: list[ResolveResult] = []
        seen: set[str] = set()
        for idx in rows:
            m = meta[idx]
            if kind and m["kind"] != kind:
                continue
            if m["uri"] in seen:
                continue
            seen.add(m["uri"])
            hits.append(self._row_to_result(m, 1.0, "exact"))
            if len(hits) >= top_k:
                break
        return hits

    def _semantic_stage(
        self,
        text: str,
        kind: str | None,
        vectors: np.ndarray,
        meta: list[dict[str, Any]],
        top_k: int,
        min_score: float,
        exclude_uris: set[str],
    ) -> list[ResolveResult]:
        """Embedding cosine-similarity search, skipping already-seen URIs."""
        q_vec = self._embed([text])  # shape (1, dim)
        scores = (q_vec @ vectors.T).squeeze(0)  # shape (N,)

        if kind:
            mask = np.array([m["kind"] == kind for m in meta], dtype=bool)
            scores = np.where(mask, scores, -1.0)

        # Over-fetch so dedup by URI still leaves enough candidates.
        n_candidates = min(len(scores), top_k * 3)
        if n_candidates >= len(scores):
            top_indices = np.argsort(-scores)
        else:
            top_indices = np.argpartition(-scores, n_candidates)[:n_candidates]
            top_indices = top_indices[np.argsort(-scores[top_indices])]

        seen_uris = set(exclude_uris)
        results: list[ResolveResult] = []
        for idx in top_indices:
            s = float(scores[idx])
            if s < min_score:
                break
            m = meta[idx]
            if m["uri"] in seen_uris:
                continue
            seen_uris.add(m["uri"])
            results.append(self._row_to_result(m, s, "semantic"))
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

        # 4. Atomic swap (also rebuilds the exact-match surface index)
        self._set_index(vectors, meta, new_hash)

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
            self._set_index(vectors, meta, hash_val)
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
