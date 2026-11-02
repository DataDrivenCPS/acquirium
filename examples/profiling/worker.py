"""Diagnostic interpreter launcher; production workers are never modified.

Escapement invokes this executable with its actual worker script and arguments.
Only timing calls are injected into that script's reviewed execution boundaries.
"""

import time

_entry_ns = time.perf_counter_ns()

import ast
import hashlib
import json
import os
import sys
from pathlib import Path

# Escapement's python/worker.py with sequential runs. Changing the native
# worker requires reviewing these boundaries before updating this digest. A
# window job's function call is function_ns and its output checks output_ns;
# a sequential run calls the function once per window between checks, so
# the whole run counts as function_ns.
WORKER_SHA256 = "259b4a02dc5f2b12dde62f3957182771e5803527f0327e822bce6d86c8fffc7f"


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
            # The loop branches on a sequential run; the other branch calls
            # the function once for the job.
            branches = [
                i for i, statement in enumerate(loop.body) if isinstance(statement, ast.If)
            ]
            if len(branches) != 1:
                raise RuntimeError("native worker job branch changed")
            branch = loop.body[branches[0]]
            calls = [
                i
                for i, statement in enumerate(branch.orelse)
                if isinstance(statement, ast.Assign)
                and isinstance(statement.value, ast.Call)
                and isinstance(statement.value.func, ast.Name)
                and statement.value.func.id == "function"
            ]
            if len(calls) != 1:
                raise RuntimeError("native worker function invocation changed")
            call = calls[0]
            branch.body = timed(branch.body, "function_ns")
            branch.orelse = (
                timed(branch.orelse[: call + 1], "function_ns")
                + timed(branch.orelse[call + 1 :], "output_ns")
            )
            loop.body = (
                timed(loop.body[: branches[0]], "input_ns")
                + ast.parse('_acq_metrics["input_rows"] += frame.height').body
                + [branch]
                + timed(loop.body[branches[0] + 1 :], "output_ns")
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
    pooled = sys.argv[2] == "--pool"
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
    namespace = {
        "__name__": "escapement_profiled_worker",
        "__file__": str(worker),
        "_acq_clock": time.perf_counter_ns,
        "_acq_metrics": metrics,
    }
    exec(  # noqa: S102 -- execute only the hash-checked, timing-instrumented native worker.
        code,
        namespace,
    )
    output = Path(__file__).parent / "workers"
    output.mkdir(exist_ok=True)
    original = namespace["main"]
    first = True

    def measured(*args):
        nonlocal first
        cold = first
        first = False
        if not cold:
            for key in metrics:
                metrics[key] = 0
        started_ns = prepared_ns if cold else time.perf_counter_ns()
        original(*args)
        finished_ns = time.perf_counter_ns()
        directory = Path(sys.argv[1])
        context = json.loads((directory / "context-0.json").read_text())
        record = dict(
            metrics,
            epoch=context["epoch"],
            operator=context["operator"]["id"],
            groups=int(sys.argv[3]),
            wrapper_prepare_ns=prepared_ns - _entry_ns if cold else 0,
            worker_body_ns=finished_ns - started_ns,
            cold_worker=cold,
            pid=os.getpid(),
        )
        tag = hashlib.sha256(record["operator"].encode()).hexdigest()[:12]
        (output / f"{context['epoch']}-{tag}-{started_ns}.json").write_text(
            json.dumps(record)
        )

    namespace["main"] = measured
    if pooled:
        load = namespace["load_module"]

        def measured_load(directory):
            started = time.perf_counter_ns()
            result = load(directory)
            metrics["function_setup_ns"] += time.perf_counter_ns() - started
            return result

        namespace["load_module"] = measured_load
        namespace["serve"]()
    else:
        measured()


if __name__ == "__main__":
    main()
