from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import re
import unicodedata
from difflib import SequenceMatcher

_WORD_RE = re.compile(r"[A-Za-z0-9]+")
_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")

def _strip_diacritics(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _split_identifier(s: str) -> str:
    # Split CamelCase and snake or dash
    s = _CAMEL_SPLIT_RE.sub(" ", s)
    s = s.replace("_", " ").replace("-", " ")
    return s

def normalize_text(s: str) -> str:
    s = s.strip()
    s = unicodedata.normalize("NFKC", s)
    s = _strip_diacritics(s)
    s = _split_identifier(s)
    s = s.lower()
    # Keep alnum words, collapse whitespace
    tokens = _WORD_RE.findall(s)
    return " ".join(tokens)

def tokenize(s: str) -> List[str]:
    s2 = normalize_text(s)
    if not s2:
        return []
    toks = s2.split()
    # very cheap singularization, helps for short phrases
    out = []
    for t in toks:
        if len(t) > 3 and t.endswith("s"):
            out.append(t[:-1])
        else:
            out.append(t)
    return out

def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def seq_ratio(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def damerau_levenshtein(a: str, b: str, max_dist: int | None = None) -> int:
    """
    Small, dependency free Damerau Levenshtein.
    max_dist can early exit.
    """
    if a == b:
        return 0
    la, lb = len(a), len(b)
    if la == 0:
        return lb
    if lb == 0:
        return la

    # early bound
    if max_dist is not None and abs(la - lb) > max_dist:
        return max_dist + 1

    d = [[0] * (lb + 1) for _ in range(la + 1)]
    for i in range(la + 1):
        d[i][0] = i
    for j in range(lb + 1):
        d[0][j] = j

    for i in range(1, la + 1):
        ai = a[i - 1]
        for j in range(1, lb + 1):
            bj = b[j - 1]
            cost = 0 if ai == bj else 1
            d[i][j] = min(
                d[i - 1][j] + 1,      # del
                d[i][j - 1] + 1,      # ins
                d[i - 1][j - 1] + cost,  # sub
            )
            if i > 1 and j > 1 and ai == b[j - 2] and a[i - 2] == bj:
                d[i][j] = min(d[i][j], d[i - 2][j - 2] + 1)  # transposition

        if max_dist is not None and min(d[i]) > max_dist:
            return max_dist + 1

    return d[la][lb]

def initialism(tokens: List[str]) -> str:
    if not tokens:
        return ""
    return "".join(t[0] for t in tokens if t)

@dataclass(frozen=True)
class MatchResult:
    uri: str
    kind: str                # "class" or "predicate" or "other"
    label: str
    score: float
    reason: str
    matched_surface: str

class ConceptMatcher:
    """
    Lightweight matcher over a prebuilt lexicon dictionary.

    Expected lexicon layout:

    {
      "version": "...",
      "abbrev": {"ahu": ["air handling unit"] , ...},
      "concepts": {
        "<uri>": {
          "kind": "class"|"predicate"|...,
          "label": "Air Handling Unit",
          "surfaces": ["air handling unit", "air handler unit", "ahu", ...],
          "comment": "...optional..."
        }
      }
    }
    """

    def __init__(self, lexicon: Dict[str, Any]):
        self.lexicon = lexicon
        self._compiled: List[Tuple[str, str, str, str, List[str], str]] = []
        self._abbrev = {normalize_text(k): [normalize_text(x) for x in v]
                        for k, v in (lexicon.get("abbrev") or {}).items()}

        concepts = lexicon.get("concepts") or {}
        for uri, meta in concepts.items():
            kind = meta.get("kind") or "other"
            label = meta.get("label") or uri
            surfaces = meta.get("surfaces") or []
            # also include label as a surface
            surfaces2 = list(dict.fromkeys([label] + list(surfaces)))
            for surf in surfaces2:
                ns = normalize_text(surf)
                toks = tokenize(surf)
                self._compiled.append((uri, kind, label, surf, toks, ns))

    def expand_abbrev(self, query: str) -> List[str]:
        qn = normalize_text(query)
        if not qn:
            return []
        # exact abbreviation key match
        if qn in self._abbrev:
            return self._abbrev[qn]
        return []

    def match(
        self,
        query: str,
        *,
        restrict_kinds: Optional[set[str]] = None,
        top_k: int = 5,
        min_score: float = 0.55,
    ) -> List[MatchResult]:
        qn = normalize_text(query)
        qtoks = tokenize(query)

        # include abbreviation expansions as extra query forms
        query_forms: List[Tuple[str, str, List[str]]] = [(query, qn, qtoks)]
        for exp in self.expand_abbrev(query):
            query_forms.append((exp, normalize_text(exp), tokenize(exp)))

        scored: Dict[Tuple[str, str], MatchResult] = {}

        for q_raw, q_norm, q_toks in query_forms:
            q_init = initialism(q_toks)
            for uri, kind, label, surf, stoks, snorm in self._compiled:
                if restrict_kinds and kind not in restrict_kinds:
                    continue

                # fast exact match bonus
                if q_norm == snorm and q_norm:
                    res = MatchResult(uri, kind, label, 1.0, "exact_normalized", surf)
                    scored[(uri, kind)] = max(scored.get((uri, kind), res), res, key=lambda r: r.score)
                    continue

                # abbreviation bonus if query looks like an initialism
                ab_bonus = 0.0
                if q_init and q_init == initialism(stoks):
                    ab_bonus = 0.12

                # main overlap
                tok_overlap = jaccard(q_toks, stoks)

                # phrase similarity
                phrase_sim = seq_ratio(q_norm, snorm)

                # typo tolerance tie breaker: edit distance on compact strings
                # we only compute if it can matter
                edit_bonus = 0.0
                if phrase_sim > 0.70 or tok_overlap > 0.50:
                    maxd = 2 if len(q_norm) <= 10 else 3
                    d = damerau_levenshtein(q_norm, snorm, max_dist=maxd)
                    if d <= maxd:
                        edit_bonus = (maxd - d) * 0.03

                # penalty for much longer candidate when query is short
                len_pen = 0.0
                if len(stoks) >= len(q_toks) + 3 and tok_overlap < 0.8:
                    len_pen = 0.08

                score = (
                    0.62 * tok_overlap +
                    0.32 * phrase_sim +
                    ab_bonus +
                    edit_bonus -
                    len_pen
                )

                # guardrail: if there is zero token overlap, require strong phrase similarity
                if tok_overlap == 0.0 and phrase_sim < 0.85:
                    continue

                if score < min_score:
                    continue

                reason = "fuzzy"
                if ab_bonus > 0:
                    reason = "initialism_match"
                if q_raw != query:
                    reason = "abbrev_expansion"

                res = MatchResult(uri, kind, label, float(score), reason, surf)
                key = (uri, kind)
                if key not in scored or res.score > scored[key].score:
                    scored[key] = res

        out = sorted(scored.values(), key=lambda r: r.score, reverse=True)[:top_k]
        return out
