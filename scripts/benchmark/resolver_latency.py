"""Resolver latency benchmark — track + debug ConceptResolver performance.

The unification put new work on the resolve hot path (a deterministic QUDT
converter tier for units, and N+1 round trips for two-pass context coerce).
This script isolates each tier so a regression can be pinned to one of them,
and appends a per-tier row to ``tests/text_match_results/resolver_bench.csv``
(timestamp + git SHA) so latency can be tracked across commits.

Two modes:

  inproc  (default, NO Docker)  Imports ConceptResolver / QUDTUnitConverter /
          EmbeddingMatcher directly and times each tier in isolation:
            - graph-exact dict lookup (no embedding model)
            - converter resolve  HIT  (fast path)
            - converter resolve  MISS (the ~O(graph) literal scan)
          This is the breakdown that pinpoints a regression.

  http    Talks to a running server (make testing-up) and times end-to-end
          /resolve_text per kind, with and without context, so the real
          deployed latency (incl. embedding + network) is tracked too.

Usage:
    python scripts/benchmark/resolver_latency.py --mode inproc --iters 200
    python scripts/benchmark/resolver_latency.py --mode http   --iters 100 \
        --host localhost --port 8000
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

_BENCH_CSV = Path(__file__).resolve().parents[2] / "tests" / "text_match_results" / "resolver_bench.csv"
_QUDT_TTL = Path("ontologies/qudt_unit.ttl")


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def _pctile(samples: list[float], q: float) -> float:
    """q in [0,1]; nearest-rank percentile in milliseconds."""
    if not samples:
        return 0.0
    s = sorted(samples)
    idx = min(len(s) - 1, int(round(q * (len(s) - 1))))
    return s[idx] * 1000.0


def _time(fn, iters: int) -> list[float]:
    """Return per-call wall times (seconds). One warmup call is discarded."""
    try:
        fn()
    except Exception:
        pass
    out: list[float] = []
    for _ in range(iters):
        t0 = time.perf_counter()
        try:
            fn()
        except Exception:
            pass
        out.append(time.perf_counter() - t0)
    return out


def _emit(rows: list[tuple[str, str, int, list[float]]], mode: str) -> None:
    """rows: (tier, kind, n, samples[s]). Print a table and append to CSV."""
    sha = _git_sha()
    ts = datetime.now().isoformat(timespec="seconds")
    print(f"\n{'tier':<26}{'kind':<14}{'n':>6}{'p50 ms':>10}{'p95 ms':>10}")
    print("-" * 66)
    _BENCH_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not _BENCH_CSV.exists()
    with open(_BENCH_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(
                ["timestamp", "git_sha", "mode", "tier", "kind", "n", "p50_ms", "p95_ms"]
            )
        for tier, kind, n, samples in rows:
            p50, p95 = _pctile(samples, 0.50), _pctile(samples, 0.95)
            print(f"{tier:<26}{kind:<14}{n:>6}{p50:>10.3f}{p95:>10.3f}")
            w.writerow([ts, sha, mode, tier, kind, n, f"{p50:.4f}", f"{p95:.4f}"])
    print(f"\nappended {len(rows)} rows to {_BENCH_CSV} (git {sha})")


# ───────────────────────── inproc mode ─────────────────────────

def run_inproc(iters: int) -> None:
    from acquirium.TextMatch.embedding_matcher import EmbeddingMatcher
    from acquirium.internals.qudt_units import QUDTUnitConverter, UnitNotFound

    rows: list[tuple[str, str, int, list[float]]] = []

    # Tier 1: graph-exact dict lookup (no embedding model is loaded because a
    # single exact hit with top_k=1 short-circuits before the semantic stage).
    m = EmbeddingMatcher()
    m.build_index(
        [
            {"uri": f"urn:x#C{i}", "kind": "class", "label": f"thing {i}",
             "surfaces": [f"thing {i}", f"C{i}"]}
            for i in range(500)
        ]
    )
    rows.append(
        ("graph_exact_lookup", "class", iters,
         _time(lambda: m.query("thing 250", kind="class", top_k=1), iters))
    )

    # Tiers 2/3: converter resolve HIT vs MISS (MISS triggers the O(graph)
    # literal scan now on the unit hot path).
    if _QUDT_TTL.exists():
        print(f"loading {_QUDT_TTL} ...", file=sys.stderr)
        conv = QUDTUnitConverter(str(_QUDT_TTL))

        def _hit() -> None:
            conv.resolve_unit("kg")

        def _miss() -> None:
            try:
                conv.resolve_unit("zzz_not_a_unit_zzz")
            except UnitNotFound:
                pass

        rows.append(("converter_resolve_hit", "unit", iters, _time(_hit, iters)))
        rows.append(("converter_resolve_miss", "unit", max(20, iters // 5),
                     _time(_miss, max(20, iters // 5))))
    else:
        print(f"skip converter tiers: {_QUDT_TTL} not found", file=sys.stderr)

    _emit(rows, "inproc")


# ───────────────────────── http mode ─────────────────────────

def run_http(iters: int, host: str, port: int) -> None:
    from acquirium import Acquirium

    aq = Acquirium(server_url=host, server_port=port, use_ssl=False)
    rc = aq.client.resolve_concept

    _MASS = "http://qudt.org/vocab/quantitykind/Mass"
    probes = [
        ("e2e_class", "class", lambda: rc("pump", kind="class")),
        ("e2e_predicate", "predicate", lambda: rc("has unit", kind="predicate")),
        ("e2e_unit_exact", "unit", lambda: rc("kg", kind="unit")),
        ("e2e_unit_semantic", "unit", lambda: rc("kilogram", kind="unit")),
        ("e2e_qk", "quantity_kind", lambda: rc("temperature", kind="quantity_kind")),
        ("e2e_unit_noctx", "unit", lambda: rc("kg", kind="unit")),
        ("e2e_unit_ctx", "unit",
         lambda: rc("kg", kind="unit", context=[_MASS])),
    ]
    rows = [(tier, kind, iters, _time(fn, iters)) for tier, kind, fn in probes]
    _emit(rows, "http")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=("inproc", "http"), default="inproc")
    p.add_argument("--iters", type=int, default=200)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()

    if args.mode == "inproc":
        run_inproc(args.iters)
    else:
        run_http(args.iters, args.host, args.port)


if __name__ == "__main__":
    main()
