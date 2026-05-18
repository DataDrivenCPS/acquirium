#!/usr/bin/env python3
"""Inspect what the concept-normalization module extracts from your inputs.

Talks to a running Acquirium server's unified resolver (ConceptResolver via
/resolve_text) so you see the *real* pipeline output: data-graph matches, the
deterministic QUDT unit tier, QUDT-semantic fallback, and context rerank.

Two input shapes are accepted:

  1. Query item   - a single text + optional kind/context:
       {"text": "mg/L", "kind": "unit", "context": ["http://.../Mass"]}
     (kind: unit | quantity_kind | class | predicate | null)

  2. Field record - a stream-like record with NO "text" key, e.g.:
       {"unit": "mg/L", "quantity_kind": "mass concentration",
        "medium": "water", "class": "pump"}
     Each field is resolved with two-pass sibling context (non-unit fields
     first, their URIs fed as context to units) - exactly how the ingest /
     register_stream / query-builder path coerces a record.

Modes
-----
  Interactive (default, no input given):
      $ python scripts/inspect_resolver.py --port 8010
      > mg/L                         # resolve as kind=None
      > unit: kg                     # kind shorthand
      > ctx: http://qudt.org/vocab/quantitykind/Mass
      > kg                           # uses the context set above
      > {"unit": "kg", "quantity_kind": "mass"}   # paste a record
      > :k unit        :n 5        :s 0.4        :help        :q

  Batch:
      $ ... --input records.jsonl                 # one JSON record per line
      $ ... --input records.json                  # or a JSON array
      $ ... --text "mg/L" --kind unit --context http://.../Mass
      $ ... --input recs.jsonl --json             # machine-readable output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

# Map record field names -> resolver kind. Mirrors register_stream / the
# flex_query_rdf_inputs decorator. Edit/extend as your records require.
FIELD_KINDS: dict[str, str] = {
    "unit": "unit",
    "quantity_kind": "quantity_kind",
    "qk": "quantity_kind",
    "medium": "class",
    "substance": "class",
    "class": "class",
    "type": "class",
    "predicate": "predicate",
    "property": "predicate",
}
_KINDS = {"unit", "quantity_kind", "class", "predicate"}


def _client(host: str, port: int):
    from acquirium import Acquirium

    aq = Acquirium(server_url=host, server_port=port, use_ssl=False)
    try:
        aq.client.embedding_status()  # cheap reachability + readiness probe
    except Exception as e:
        sys.exit(
            f"error: cannot reach Acquirium server at {host}:{port} "
            f"({type(e).__name__}). Is it up? The test stack uses port 8010 "
            f"(make testing-up); the dev stack uses 8000. Pass --host/--port."
        )
    return aq


def resolve_one(
    aq, text: str, kind: str | None, context: list[str] | None,
    top_k: int, min_score: float,
) -> list[dict[str, Any]]:
    """Raw ranked matches from /resolve_text for one text."""
    return aq.client.resolve_text(
        text, kind=kind or None, top_k=top_k, min_score=min_score,
        context=context or None,
    )


def resolve_record(
    aq, record: dict[str, Any], top_k: int, min_score: float,
) -> dict[str, Any]:
    """Resolve a query item or a field record.

    Field records use the two-pass rule: resolve non-unit fields first,
    feed their resolved URIs as context when resolving unit fields.
    """
    if "text" in record:
        text = str(record["text"])
        kind = record.get("kind")
        ctx = record.get("context")
        matches = resolve_one(aq, text, kind, ctx, top_k, min_score)
        return {"input": record, "matches": matches}

    # Field record: split unit fields from the rest for two-pass context.
    fields = [(k, v) for k, v in record.items() if v is not None]
    non_unit = [(k, v) for k, v in fields if FIELD_KINDS.get(k) != "unit"]
    unit_f = [(k, v) for k, v in fields if FIELD_KINDS.get(k) == "unit"]

    out: dict[str, Any] = {}
    context: list[str] = []
    for name, value in non_unit:
        kind = FIELD_KINDS.get(name)  # None -> resolve across all kinds
        m = resolve_one(aq, str(value), kind, None, top_k, min_score)
        out[name] = {"kind": kind, "input": value, "matches": m}
        if m:
            context.append(m[0]["uri"])
    for name, value in unit_f:
        m = resolve_one(aq, str(value), "unit", context or None, top_k, min_score)
        out[name] = {
            "kind": "unit", "input": value, "matches": m,
            "context_used": list(context),
        }
    return {"input": record, "fields": out}


# ---------------------------------------------------------------- rendering

def _fmt_matches(matches: list[dict[str, Any]], top_k: int) -> str:
    if not matches:
        return "    (no match)"
    lines = []
    for i, m in enumerate(matches[:top_k], 1):
        rel = m.get("related") or []
        lines.append(
            f"    {i}. {m['score']:.3f} [{m.get('match_stage','?'):<8}] "
            f"{m.get('kind',''):<13} {m['uri']}"
            + (f"  ({m.get('label','')})" if m.get("label") else "")
            + (f"  rel={len(rel)}" if rel else "")
        )
    return "\n".join(lines)


def render(result: dict[str, Any], top_k: int) -> str:
    if "matches" in result:
        inp = result["input"]
        head = (
            f'text={inp.get("text")!r} kind={inp.get("kind")} '
            f'context={inp.get("context") or []}'
        )
        return f"= {head}\n" + _fmt_matches(result["matches"], top_k)
    # field record
    parts = [f"= record {json.dumps(result['input'], ensure_ascii=False)}"]
    for name, info in result["fields"].items():
        m = info["matches"]
        top = m[0] if m else None
        ctx = (
            f"  [ctx={len(info['context_used'])}]"
            if info.get("context_used") else ""
        )
        if top:
            parts.append(
                f"  {name:<14} {info['input']!r:<26} -> {top['uri']}  "
                f"({top['score']:.3f} {top.get('match_stage','?')}){ctx}"
            )
        else:
            parts.append(f"  {name:<14} {info['input']!r:<26} -> (no match){ctx}")
    return "\n".join(parts)


# ---------------------------------------------------------------- batch

def _load_records(path: str) -> list[dict[str, Any]]:
    raw = open(path).read().strip()
    if not raw:
        return []
    if raw[0] == "[":
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def run_batch(aq, records, top_k, min_score, as_json: bool) -> None:
    results = [resolve_record(aq, r, top_k, min_score) for r in records]
    if as_json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        for r in results:
            print(render(r, top_k))
            print()


# ---------------------------------------------------------------- interactive

_HELP = """\
commands:
  <text>                 resolve <text> with the current default kind
  unit: <text>           resolve once with an explicit kind
                         (kinds: unit quantity_kind class predicate)
  ctx: u1,u2             set context URIs applied to subsequent queries
  ctx:                   clear context
  {json}                 resolve a query item or field record
  :k <kind>|none         set default kind        :n <int>  set top_k
  :s <float>             set min_score            :ctx      show context
  :help                  this help               :q        quit"""


def run_interactive(aq, top_k, min_score) -> None:
    try:
        import readline  # noqa: F401  (enables history/editing if available)
    except Exception:
        pass
    kind: str | None = None
    context: list[str] = []
    print(f"resolver inspector  (top_k={top_k} min_score={min_score})  "
          f":help for commands, :q to quit")
    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        if line in (":q", ":quit", ":exit"):
            return
        if line in (":help", "?"):
            print(_HELP)
            continue
        if line == ":ctx":
            print(f"  context = {context}")
            continue
        if line.startswith(":k"):
            arg = line[2:].strip()
            kind = None if arg in ("", "none", "null") else arg
            print(f"  default kind = {kind}")
            continue
        if line.startswith(":n"):
            top_k = int(line[2:].strip() or top_k)
            print(f"  top_k = {top_k}")
            continue
        if line.startswith(":s"):
            min_score = float(line[2:].strip() or min_score)
            print(f"  min_score = {min_score}")
            continue
        if line.startswith("ctx:"):
            body = line[4:].strip()
            context = [u for u in (s.strip() for s in body.split(",")) if u]
            print(f"  context = {context}")
            continue
        try:
            if line[0] == "{":
                rec = json.loads(line)
                print(render(resolve_record(aq, rec, top_k, min_score), top_k))
                continue
            this_kind = kind
            if ":" in line and line.split(":", 1)[0].strip() in _KINDS:
                k, line = line.split(":", 1)
                this_kind, line = k.strip(), line.strip()
            res = {
                "input": {"text": line, "kind": this_kind, "context": context},
                "matches": resolve_one(
                    aq, line, this_kind, context, top_k, min_score
                ),
            }
            print(render(res, top_k))
        except Exception as e:  # keep the REPL alive on any bad input/call
            print(f"  error: {type(e).__name__}: {e}")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--host", default=os.environ.get("ACQUIRIUM_HOST", "localhost"))
    p.add_argument("--port", type=int,
                   default=int(os.environ.get("ACQUIRIUM_PORT", "8000")),
                   help="server port (test stack uses 8010)")
    p.add_argument("--input", help="JSON array or JSONL file of records")
    p.add_argument("--text", help="one-shot: resolve this text")
    p.add_argument("--kind", help="kind for --text (unit|quantity_kind|class|predicate)")
    p.add_argument("--context", help="comma-separated context URIs for --text")
    p.add_argument("--top-k", type=int, default=5)
    p.add_argument("--min-score", type=float, default=0.5)
    p.add_argument("--json", action="store_true", help="machine-readable output")
    args = p.parse_args()

    aq = _client(args.host, args.port)

    if args.text is not None:
        rec: dict[str, Any] = {"text": args.text}
        if args.kind:
            rec["kind"] = args.kind
        if args.context:
            rec["context"] = [s.strip() for s in args.context.split(",") if s.strip()]
        run_batch(aq, [rec], args.top_k, args.min_score, args.json)
        return

    if args.input:
        records = _load_records(args.input)
        run_batch(aq, records, args.top_k, args.min_score, args.json)
        return

    run_interactive(aq, args.top_k, args.min_score)


if __name__ == "__main__":
    main()
