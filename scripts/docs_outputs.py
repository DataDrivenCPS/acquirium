"""Run the code blocks of docs pages and rewrite their output blocks.

A page opts in with a comment near its top naming the instance it runs on::

    <!-- capture: instance=seawater-ro -->   model and one year of data loaded
    <!-- capture: instance=empty -->         a fresh instance, nothing loaded

Its ``python`` blocks then run top to bottom in one namespace, like notebook
cells, with ``acq`` already connected to that instance. A ``text`` block that
directly follows a ``python`` block is that block's output and is replaced by
what the block printed plus the value of its last expression.

A comment on the line before a ``python`` block changes how it is treated::

    <!-- capture: skip -->     not run (connection code, shell-like examples)
    <!-- capture: raises -->   expected to raise; the output is the error

Usage::

    python scripts/docs_outputs.py docs/tutorials/data.md          # rewrite
    python scripts/docs_outputs.py --check docs/tutorials/*.md     # exit 1 on drift

Pages without the instance comment are left alone.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import difflib
import io
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

INSTANCE = re.compile(r"<!--\s*capture:\s*instance=([\w-]+)\s*-->")
DIRECTIVE = re.compile(r"<!--\s*capture:\s*(skip|raises)\s*-->")
FENCE = re.compile(r"^```(\w*)\s*$")
# Parts of an output that differ between runs of the same code.
VOLATILE = [(re.compile(r"(127\.0\.0\.1|localhost):\d+"), r"\1:8000")]


@dataclass
class Block:
    language: str
    start: int  # index of the opening fence line
    end: int  # index of the closing fence line
    directive: str | None = None

    def body(self, lines: list[str]) -> str:
        return "\n".join(lines[self.start + 1 : self.end])


def find_blocks(lines: list[str]) -> list[Block]:
    """The fenced blocks of a page, each with the directive on the line before it."""
    blocks: list[Block] = []
    opened: int | None = None
    language = ""
    for i, line in enumerate(lines):
        match = FENCE.match(line)
        if not match:
            continue
        if opened is None:
            opened, language = i, match.group(1)
        elif match.group(1) == "":
            directive = DIRECTIVE.search(lines[opened - 1]) if opened > 0 else None
            blocks.append(Block(language, opened, i, directive.group(1) if directive else None))
            opened = None
    return blocks


def run_block(code: str, namespace: dict[str, Any]) -> str:
    """Run *code* as a notebook cell: what it printed, then its last expression."""
    tree = ast.parse(code)
    last = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        exec(compile(tree, "<docs>", "exec"), namespace)
        value = eval(compile(ast.Expression(last.value), "<docs>", "eval"), namespace) if last else None
    out = printed.getvalue()
    if value is not None:
        out += repr(value)
    for pattern, replacement in VOLATILE:
        out = pattern.sub(replacement, out)
    return out.rstrip()


def capture(text: str, namespace: dict[str, Any]) -> str:
    """Return *text* with every output block replaced by a fresh run."""
    lines = text.split("\n")
    blocks = find_blocks(lines)
    replacements: list[tuple[Block, str]] = []
    for block, following in zip(blocks, blocks[1:] + [None]):
        if block.language != "python" or block.directive == "skip":
            continue
        code = block.body(lines)
        try:
            output = run_block(code, namespace)
            if block.directive == "raises":
                raise RuntimeError(f"line {block.start + 1}: block marked 'raises' did not raise")
        except Exception as exc:
            if block.directive != "raises" or isinstance(exc, RuntimeError) and "did not raise" in str(exc):
                raise RuntimeError(f"line {block.start + 1}: {type(exc).__name__}: {exc}") from exc
            output = f"{type(exc).__name__}: {exc}"
            for pattern, replacement in VOLATILE:
                output = pattern.sub(replacement, output)
        is_output = (
            following is not None
            and following.language == "text"
            and all(not line.strip() for line in lines[block.end + 1 : following.start])
        )
        if is_output:
            replacements.append((following, output))
    for block, output in reversed(replacements):
        lines[block.start + 1 : block.end] = output.split("\n") if output else []
    return "\n".join(lines)


# --- instances -------------------------------------------------------------


def _load_seawater_ro(acq: Any) -> None:
    from acquirium.examples import seawater_ro

    acq.insert_graph_file(seawater_ro.model_path, source_id="plant")
    data = seawater_ro.generate()
    acq.register_streams(
        [
            {"source_id": "seawater-ro", "ref_name": name, "point_uri": f"urn:swro/{name}", "value_kind": "numeric"}
            for name in data
        ]
    )
    acq.insert_timeseries_batch("seawater-ro", data)


LOADERS: dict[str, Callable[[Any], None]] = {"empty": lambda acq: None, "seawater-ro": _load_seawater_ro}


class Instances:
    """Throwaway instances, one per page, copied from a template built once per kind."""

    def __init__(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="acquirium-docs-"))
        self.templates: dict[str, Path] = {}
        self.count = 0

    def start(self, kind: str) -> Any:
        import acquirium as aq

        if kind not in LOADERS:
            raise ValueError(f"unknown instance {kind!r}; choose from {sorted(LOADERS)}")
        if kind not in self.templates:
            template = self.root / f"template-{kind}"
            LOADERS[kind](aq.init(data_dir=template))
            aq.shutdown()
            shutil.rmtree(template / ".runtime", ignore_errors=True)
            self.templates[kind] = template
        self.count += 1
        copy = self.root / f"page-{self.count}"
        shutil.copytree(self.templates[kind], copy)
        return aq.init(data_dir=copy)

    def stop(self) -> None:
        import acquirium as aq

        aq.shutdown()

    def close(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


def namespace_for(acq: Any) -> dict[str, Any]:
    import polars as pl

    pl.Config.restore_defaults()
    pl.Config.set_tbl_width_chars(100)
    return {"__name__": "__main__", "acq": acq}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("pages", nargs="+", type=Path)
    parser.add_argument("--check", action="store_true", help="report drift instead of rewriting")
    args = parser.parse_args(argv)

    instances = Instances()
    drifted = failed = 0
    try:
        for page in args.pages:
            text = page.read_text()
            kind = INSTANCE.search(text)
            if not kind:
                print(f"skip     {page} (no capture comment)")
                continue
            try:
                fresh = capture(text, namespace_for(instances.start(kind.group(1))))
            except Exception as exc:
                failed += 1
                print(f"FAILED   {page}: {exc}")
                continue
            finally:
                instances.stop()
            if fresh == text:
                print(f"current  {page}")
            elif args.check:
                drifted += 1
                print(f"DRIFT    {page}")
                sys.stdout.writelines(
                    difflib.unified_diff(
                        text.splitlines(True), fresh.splitlines(True), str(page), "fresh run", n=1
                    )
                )
            else:
                page.write_text(fresh)
                print(f"updated  {page}")
    finally:
        instances.close()
    return 1 if drifted or failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
