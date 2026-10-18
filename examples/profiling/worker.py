"""Diagnostic interpreter launcher; production workers are never modified.

Escapement invokes this executable with its actual worker script and arguments.
Only timing calls are injected into that script's reviewed execution boundaries.
"""

import time

_entry_ns = time.perf_counter_ns()

import ast
import hashlib
import json
import sys
from pathlib import Path

# Escapement fe7e50f483a07088a682935e83b6d7095760e1cb. Changing the native
# worker requires reviewing these boundaries before updating this digest.
WORKER_SHA256 = "b26f608086f40e74f6763f4ade6f0e34662601a835ca135e45467bf5a1028d34"


def timed(nodes, field):
    return (
        ast.parse("_acq_started = _acq_clock()").body
        + nodes
        + ast.parse(f"_acq_metrics[{field!r}] += _acq_clock() - _acq_started").body
    )


def instrument(source):
    tree = ast.parse(source)
    body = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            name = node.names[0].name
            field = (
                "polars_import_ns"
                if name == "polars"
                else "pyarrow_import_ns"
                if name.startswith("pyarrow")
                else "stdlib_import_ns"
            )
            body.extend(timed([node], field))
        elif isinstance(node, ast.FunctionDef) and node.name == "main":
            indices = [
                index
                for index, statement in enumerate(node.body)
                if isinstance(statement, ast.For)
            ]
            if len(indices) != 1:
                raise RuntimeError("native worker group loop changed")
            index = indices[0]
            loop = node.body[index]
            calls = [
                i
                for i, statement in enumerate(loop.body)
                if isinstance(statement, ast.Assign)
                and isinstance(statement.value, ast.Call)
                and isinstance(statement.value.func, ast.Name)
                and statement.value.func.id == "function"
            ]
            if len(calls) != 1:
                raise RuntimeError("native worker function invocation changed")
            call = calls[0]
            loop.body = (
                timed(loop.body[:call], "input_ns")
                + ast.parse('_acq_metrics["input_rows"] += frame.height').body
                + timed([loop.body[call]], "function_ns")
                + timed(loop.body[call + 1 :], "output_ns")
            )
            node.body = (
                timed(node.body[:index], "function_setup_ns")
                + [loop]
                + timed(node.body[index + 1 :], "finalize_ns")
            )
            body.append(node)
        else:
            body.append(node)
    tree.body = body
    return compile(ast.fix_missing_locations(tree), "<profiled-native-worker>", "exec")


def main():
    worker = Path(sys.argv[1])
    source = worker.read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    if digest != WORKER_SHA256:
        raise RuntimeError(
            f"native worker changed ({digest}); review profiling boundaries"
        )
    directory = Path(sys.argv[2])
    groups = int(sys.argv[4])
    metrics = dict.fromkeys(
        (
            "stdlib_import_ns",
            "polars_import_ns",
            "pyarrow_import_ns",
            "function_setup_ns",
            "input_ns",
            "function_ns",
            "output_ns",
            "finalize_ns",
            "input_rows",
        ),
        0,
    )
    code = instrument(source)
    prepared_ns = time.perf_counter_ns()
    sys.argv = sys.argv[1:]
    exec(  # noqa: S102 -- execute only the hash-checked, timing-instrumented native worker.
        code,
        {
            "__name__": "__main__",
            "__file__": str(worker),
            "_acq_clock": time.perf_counter_ns,
            "_acq_metrics": metrics,
        },
    )
    finished_ns = time.perf_counter_ns()
    context = json.loads((directory / "context-0.json").read_text())
    metrics.update(
        epoch=context["epoch"],
        operator=context["operator"]["id"],
        groups=groups,
        wrapper_prepare_ns=prepared_ns - _entry_ns,
        worker_body_ns=finished_ns - prepared_ns,
    )
    output = Path(__file__).parent / "workers"
    output.mkdir(exist_ok=True)
    tag = hashlib.sha256(metrics["operator"].encode()).hexdigest()[:12]
    (output / f"{context['epoch']}-{tag}-{_entry_ns}.json").write_text(
        json.dumps(metrics)
    )


if __name__ == "__main__":
    main()
