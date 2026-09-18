"""Integration test for text matching: free text -> ontology / QUDT URI.

Runs the whole pipeline in-process, with no server and no Docker: a real
``Manager`` under a tmp data dir extracts concepts from the bundled
ontologies, embeds them with the real model, and resolves text through
``ConceptResolver`` (graph matcher, QUDT converter, QUDT matcher).

Two managers are built once per run:

- ``semantic`` — embeddings on. The cold index build is part of the test,
  so nothing is cached between runs and this takes a few minutes. The model
  itself is downloaded to the data dir unless ``FASTEMBED_CACHE_PATH`` points
  at a copy.
- ``exact``    — ``exact_only=True``: same indexes, no model, no vectors.

The cases live in ``corpus/<group>/<kind>.json``. A group says how the text
relates to the concept it should resolve to:

- ``label``    the text is an ontology label, or nearly one
- ``abbr``     a practitioner abbreviation ("RO", "TSS", "gpm")
- ``synonym``  another term for the same thing ("clarifier", "flow meter")
- ``variant``  casing, spelling, word order, typos ("ReverseOsmosis", "presure")
- ``generic``  a head word many concepts share ("pressure", "flow")
- ``tag``      a noisy point or column description ("RO feed pressure (psi)")

To add a case, append an object to ``cases`` in the file for its group and
kind::

    {
      "text": "pump",                  free text to resolve
      "expected": ["urn:...#Pump",     top-1 must be one of these
                   "http://...#Pump"],
      "stage": "exact",                optional, see below
      "context": ["http://..."]        optional resolve context URIs
    }

Every case counts toward its group's top-1 and recall@3 rates. Each group
has its own thresholds in ``GROUP_THRESHOLDS``: labels must nearly always
resolve, abbreviations are expected to be much harder. ``"stage": "exact"``
additionally claims the text resolves deterministically (exact surface or
the QUDT converter): such a case must resolve, with score 1.0, in both
managers.

``records.json`` holds joint cases for ``resolve_record`` and
``no_match.json`` holds text that must resolve to nothing.

Each run appends the rates per group and kind to
``tests/text_match_results/group_accuracy.csv`` and rewrites ``failures.txt``
there with the misses.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from acquirium.Server.manager import Manager

pytestmark = pytest.mark.textmatch

_CORPUS_DIR = Path(__file__).parent / "corpus"
_OUTPUT_DIR = Path(__file__).parent.parent / "text_match_results"

KINDS = ["class", "predicate", "unit", "quantity_kind", "substance", "process"]

MIN_SCORE = 0.6

# group -> (minimum top-1 %, minimum recall@3 %), over all kinds of the group.
# Set a few points under the measured rates, so a drop fails the run. Raise
# them when the matcher improves.
GROUP_THRESHOLDS: dict[str, tuple[int, int]] = {
    "label": (97, 99),
    "abbr": (50, 55),
    "synonym": (73, 84),
    "variant": (80, 83),
    "generic": (86, 94),
    "tag": (40, 63),
}
GROUPS = list(GROUP_THRESHOLDS)


def _load(name: str) -> dict[str, Any]:
    return json.loads((_CORPUS_DIR / f"{name}.json").read_text())


# (group, kind) -> cases. Not every group has a file for every kind.
CORPUS: dict[tuple[str, str], list[dict[str, Any]]] = {
    (g, k): _load(f"{g}/{k}")["cases"]
    for g in GROUPS for k in KINDS
    if (_CORPUS_DIR / g / f"{k}.json").exists()
}
RECORDS: list[dict[str, Any]] = _load("records")["cases"]
NO_MATCH: dict[str, Any] = _load("no_match")


def _exact_cases(kind: str) -> list[dict[str, Any]]:
    return [
        c for (_g, k), cases in CORPUS.items() if k == kind
        for c in cases if c.get("stage") == "exact"
    ]


def _resolve(manager: Manager, case: dict[str, Any], kind: str, top_k: int) -> list[dict[str, Any]]:
    return manager.resolve_text(
        case["text"], kind=kind, top_k=top_k, min_score=MIN_SCORE,
        context=case.get("context"),
    )


def _describe(case: dict[str, Any], got: list[dict[str, Any]]) -> str:
    context = f" + context {case['context']}" if case.get("context") else ""
    uris = [m["uri"] for m in got] or "<no matches>"
    return f"'{case['text']}'{context}: expected one of {case['expected']}, got {uris}"


# ──────────────────────────────────────────────────────────────
# Managers and the run report
# ──────────────────────────────────────────────────────────────

_report: dict[str, Any] = {"build_s": None, "model": None, "cells": {}}


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def _write_report() -> None:
    _OUTPUT_DIR.mkdir(exist_ok=True)
    now = datetime.now().isoformat(timespec="seconds")

    with open(_OUTPUT_DIR / "failures.txt", "w") as f:
        f.write(f"Text-matcher misses — {now}\n{'=' * 60}\n")
        for (group, kind), r in _report["cells"].items():
            f.write(f"\n{group} / {kind} top-1 misses ({len(r['misses'])} of {r['n']}):\n")
            for line in r["misses"]:
                f.write(f"  {line}\n")

    path = _OUTPUT_DIR / "group_accuracy.csv"
    write_header = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["timestamp", "git_sha", "model", "build_s", "group", "kind",
                        "n", "top1_pct", "recall3_pct"])
        for (group, kind), r in _report["cells"].items():
            w.writerow([now, _git_sha(), _report["model"], _report["build_s"], group,
                        kind, r["n"], f"{r['top1_pct']:.1f}", f"{r['recall3_pct']:.1f}"])


@pytest.fixture(scope="module")
def exact(tmp_path_factory: pytest.TempPathFactory):
    data_dir = tmp_path_factory.mktemp("acq-exact")
    # Another test may already have imported fastembed. Record that first so
    # the check below is about what this Manager imported itself.
    preloaded = "fastembed" in sys.modules
    m = Manager(data_dir=data_dir, timeseries_backend="duckdb", exact_only=True)
    m._test_imported_fastembed = "fastembed" in sys.modules and not preloaded
    yield m
    m.close()


@pytest.fixture(scope="module")
def semantic(tmp_path_factory: pytest.TempPathFactory):
    data_dir = tmp_path_factory.mktemp("acq-semantic")
    t0 = time.perf_counter()
    m = Manager(data_dir=data_dir, timeseries_backend="duckdb", exact_only=False)
    _report["build_s"] = round(time.perf_counter() - t0, 1)
    _report["model"] = m._graph_matcher._model_name
    yield m
    m.close()
    if _report["cells"]:
        _write_report()


# ──────────────────────────────────────────────────────────────
# Exact-only manager: indexes without a model
# ──────────────────────────────────────────────────────────────

def test_exact_only_status(exact: Manager) -> None:
    status = exact.embedding_status()
    assert status["semantic"] is False
    # The indexes are real — they just hold no vectors.
    for index in ("graph", "qudt"):
        assert status[index]["state"] == "ready", status[index]
        assert status[index]["concepts"] > 0


def test_exact_only_loads_no_model_and_writes_no_cache(exact: Manager) -> None:
    assert exact._graph_matcher.exact_only
    assert exact._qudt_matcher.exact_only
    assert not (exact.data_dir / "embedding_cache").exists()
    assert not exact._test_imported_fastembed


@pytest.mark.parametrize("kind", KINDS)
def test_exact_cases_resolve_without_embeddings(exact: Manager, kind: str) -> None:
    """Every ``"stage": "exact"`` case resolves with no model at all."""
    failures = []
    for case in _exact_cases(kind):
        got = _resolve(exact, case, kind, top_k=1)
        if not (got and got[0]["uri"] in case["expected"]
                and got[0]["score"] == 1.0 and got[0]["match_stage"] == "exact"):
            failures.append(_describe(case, got))
    assert not failures, "\n".join(failures)


@pytest.mark.parametrize("kind", ["unit", "quantity_kind"])
def test_class_label_never_answers_as_a_vocabulary_kind(exact: Manager, kind: str) -> None:
    """Equipment labels must not resolve as units or quantity kinds.

    The index carries each concept's extracted kind, so "pump" answers only
    for the kinds it was indexed under — a hand-rolled label lookup that
    tagged kinds separately used to return s223:Pump as a `unit` at score 1.0,
    which then got written onto a point as qudt:hasUnit.
    """
    assert exact.resolve_text("pump", kind=kind) == []
    assert exact.resolve_text("valve", kind=kind) == []


def test_fuzzy_text_needs_embeddings(exact: Manager) -> None:
    """Near-misses resolve to nothing rather than to a wrong exact hit."""
    assert exact.resolve_text("zzz total gibberish", kind="class") == []
    assert exact.resolve_text("basin for aeration", kind="class") == []


# ──────────────────────────────────────────────────────────────
# Semantic manager: cold index build
# ──────────────────────────────────────────────────────────────

def test_indexes_build_from_bundled_ontologies(semantic: Manager) -> None:
    status = semantic.embedding_status()
    assert status["semantic"] is True
    for index, matcher in (("graph", semantic._graph_matcher), ("qudt", semantic._qudt_matcher)):
        assert status[index]["state"] == "ready", status[index]
        assert status[index]["concepts"] > 0
        # One vector per surface, written to the disk cache.
        assert matcher._vectors is not None
        assert matcher._vectors.shape == (status[index]["surfaces"], 384)
        assert list((semantic.data_dir / "embedding_cache" / index).glob("*_vectors.npz"))


def test_both_managers_index_the_same_concepts(semantic: Manager, exact: Manager) -> None:
    a, b = semantic.embedding_status(), exact.embedding_status()
    for index in ("graph", "qudt"):
        assert a[index]["concepts"] == b[index]["concepts"]
        assert a[index]["surfaces"] == b[index]["surfaces"]


def test_every_kind_is_indexed(semantic: Manager) -> None:
    indexed = {m["kind"] for m in semantic._graph_matcher._meta}
    indexed |= {m["kind"] for m in semantic._qudt_matcher._meta}
    assert set(KINDS) <= indexed


# ──────────────────────────────────────────────────────────────
# Semantic manager: the corpus
# ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("group", GROUPS)
def test_resolve_rates(semantic: Manager, group: str) -> None:
    """Top-1 and recall@3 over the group's cases stay above its thresholds."""
    min_top1, min_recall3 = GROUP_THRESHOLDS[group]
    n = 0
    top1_misses, top3_misses = [], []
    for (g, kind), cases in CORPUS.items():
        if g != group:
            continue
        cell_top1, cell_top3 = [], []
        for case in cases:
            got = _resolve(semantic, case, kind, top_k=3)
            if not (got and got[0]["uri"] in case["expected"]):
                cell_top1.append(f"[{kind}] " + _describe(case, got[:1]))
            if not any(m["uri"] in case["expected"] for m in got):
                cell_top3.append(f"[{kind}] " + _describe(case, got))
        _report["cells"][(group, kind)] = {
            "n": len(cases),
            "top1_pct": (len(cases) - len(cell_top1)) / len(cases) * 100,
            "recall3_pct": (len(cases) - len(cell_top3)) / len(cases) * 100,
            "misses": cell_top1,
        }
        n += len(cases)
        top1_misses += cell_top1
        top3_misses += cell_top3

    top1 = (n - len(top1_misses)) / n * 100
    recall3 = (n - len(top3_misses)) / n * 100
    assert top1 >= min_top1, (
        f"{group} top-1 rate {top1:.1f}% is below {min_top1}%.\nMisses:\n"
        + "\n".join(top1_misses)
    )
    assert recall3 >= min_recall3, (
        f"{group} recall@3 {recall3:.1f}% is below {min_recall3}%.\nMisses:\n"
        + "\n".join(top3_misses)
    )


@pytest.mark.parametrize("kind", KINDS)
def test_exact_cases_stay_exact_with_embeddings(semantic: Manager, kind: str) -> None:
    """Embeddings never outrank a deterministic hit."""
    failures = []
    for case in _exact_cases(kind):
        got = _resolve(semantic, case, kind, top_k=1)
        if not (got and got[0]["uri"] in case["expected"]
                and got[0]["score"] == 1.0 and got[0]["match_stage"] == "exact"):
            failures.append(_describe(case, got))
    assert not failures, "\n".join(failures)


@pytest.mark.parametrize("case", NO_MATCH["cases"], ids=lambda c: c["text"])
def test_no_match(semantic: Manager, case: dict[str, Any]) -> None:
    got = semantic.resolve_text(case["text"], min_score=NO_MATCH["min_score"])
    assert got == [], f"expected no matches for '{case['text']}'"


@pytest.mark.parametrize("case", RECORDS, ids=lambda c: c["name"])
def test_resolve_record(semantic: Manager, case: dict[str, Any]) -> None:
    """Related fields of one record are resolved jointly."""
    fields = {name: (f["text"], f.get("kind")) for name, f in case["fields"].items()}
    got = semantic.resolve_record(fields, top_k=3, min_score=0.5)
    for name, expected in case["expected"].items():
        top = got[name][0]["uri"] if got[name] else "<no matches>"
        assert top in expected, f"{case['name']}.{name}: expected one of {expected}, got {top}"


# ──────────────────────────────────────────────────────────────
# Semantic manager: shape of the results
# ──────────────────────────────────────────────────────────────

def test_result_fields(semantic: Manager) -> None:
    matches = semantic.resolve_text("pump", top_k=3)
    assert matches
    for m in matches:
        assert isinstance(m["uri"], str)
        assert isinstance(m["kind"], str)
        assert isinstance(m["label"], str)
        assert isinstance(m["score"], float)
        assert isinstance(m["matched_surface"], str)
        assert m["match_stage"] in ("exact", "semantic")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("text", ["pump", "has unit", "kilogram", "temperature", "chlorine"])
def test_kind_filtering(semantic: Manager, kind: str, text: str) -> None:
    for m in semantic.resolve_text(text, kind=kind, top_k=5, min_score=0.3):
        assert m["kind"] == kind, f"kind={kind} returned a {m['kind']}: {m['uri']}"


def test_top_k_respected(semantic: Manager) -> None:
    for k in (1, 2, 3):
        assert len(semantic.resolve_text("connection", top_k=k, min_score=0.3)) <= k


def test_scores_descending(semantic: Manager) -> None:
    matches = semantic.resolve_text("connection point", top_k=5, min_score=0.3)
    scores = [m["score"] for m in matches]
    assert scores == sorted(scores, reverse=True)


def test_resolution_is_stable_across_calls(semantic: Manager) -> None:
    a = semantic.resolve_text("kg", kind="unit", top_k=3, min_score=MIN_SCORE)
    b = semantic.resolve_text("kg", kind="unit", top_k=3, min_score=MIN_SCORE)
    assert a and [m["uri"] for m in a] == [m["uri"] for m in b]
