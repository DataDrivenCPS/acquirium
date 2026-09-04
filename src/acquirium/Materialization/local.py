"""Run an app in the caller's process against a server's stored data.

A check normally executes on the server, because that is where the data is.
That also puts the running app out of reach: a ``breakpoint()`` opens on the
server's stdin, and tracebacks land in the server log rather than the
terminal that asked for the check.

This module compiles the same app and runs it here instead, pulling its
inputs over the client API. The app executes under the caller's own
interpreter, so debuggers, tracebacks, and profilers all work normally, and
the server never has to import the app at all.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from acquirium.Materialization.incremental import (
    InputBatch, OutputBuilder, StreamDescriptor, StreamSet, TimeWindow,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class _ServerGraph:
    """The narrow graph capability the planner needs, served over HTTP."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def graph_status(self) -> dict:
        return self._client.graph_status()

    def sparql_query(self, query: str, include_dependencies: bool = True,
                     *, wait_for_fresh: bool = False) -> dict:
        return self._client.sparql_query(
            query, include_dependencies=include_dependencies, wait_for_fresh=wait_for_fresh
        )


class _ServerUnitConverter:
    """``StreamSet.in_unit`` support, using the server's QUDT graph."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def convert(self, value: float, from_unit: str, to_unit: str) -> float:
        factors = self._client.get_conversion_factors(str(from_unit), str(to_unit))
        if not factors.get("compatible", False):
            raise ValueError(f"cannot convert {from_unit!r} to {to_unit!r}: incompatible units")
        si = (value + factors["from_offset"]) * factors["from_multiplier"]
        return si / factors["to_multiplier"] - factors["to_offset"]


def _stream_set(client: Any, alias: str, descriptors: tuple[StreamDescriptor, ...],
                converter: Any) -> StreamSet:
    """Fetch every bound stream's retained rows and shape them like the server does."""
    refs, times, values = [], [], []
    numeric = True
    for descriptor in descriptors:
        frame = client.timeseries_df(descriptor.ref_uri)
        if frame.is_empty():
            continue
        column = frame["value"]
        numeric = numeric and column.dtype.is_numeric()
        refs.extend([descriptor.ref_uri] * frame.height)
        times.extend(frame["ts"].to_list())
        values.extend(column.to_list())
    value_type = pa.float64() if numeric else pa.string()
    if not numeric:
        values = [None if value is None else str(value) for value in values]
    table = pa.table({
        "ref_uri": pa.array(refs, pa.string()),
        "time": pa.array(times, pa.timestamp("us", tz="UTC")),
        "value": pa.array(values, value_type),
    })
    window = (TimeWindow(min(times), max(times)) if times
              else TimeWindow(_EPOCH, _EPOCH))
    # Everything retained is in play, exactly as a backfilling first run sees
    # it, so the read window and the changed rows are the same thing.
    return StreamSet(alias, window, descriptors, table, table, converter=converter)


def check_app(client: Any, target: type, *, parameters: dict | None = None,
              limit: int | None = None) -> dict[str, Any]:
    """Compile and run ``target`` here, against ``client``'s server data.

    Returns the same document as a server-side check, so callers can render
    either the same way. Two things differ, both deliberate: the app runs in
    this process, and a failing ``transform`` raises here instead of being
    reported per binding — the traceback is the point.
    """
    if limit is not None and limit < 0:
        raise ValueError("limit must not be negative")
    deployment = Deployment.from_class(target, parameters=parameters)
    # Accept the Acquirium facade or the low-level client underneath it; the
    # query, timeseries and unit calls all live on the latter.
    api = getattr(client, "client", client)
    graph = _ServerGraph(api)
    converter = _ServerUnitConverter(api)
    revision = int(graph.graph_status().get("published_version", 0))
    planner = BindingPlanner(graph, query_resolver=api.resolve, record_resolver=api.resolve)
    dag, applications = planner.compile((deployment,), revision)

    bindings = []
    for binding in dag.bindings:
        entry: dict[str, Any] = {
            "inputs": {alias: [{"ref_uri": item.ref_uri, "label": item.label, "unit": item.unit}
                               for item in streams]
                       for alias, streams in binding.inputs.items()},
            "row": dict(binding.row) if binding.row else None,
            "outputs": {}, "error": None,
        }
        bindings.append(entry)
        inputs = {alias: _stream_set(api, alias, descriptors, converter)
                  for alias, descriptors in binding.inputs.items()}
        entry["input_rows"] = {alias: value.collect().num_rows for alias, value in inputs.items()}
        if not any(entry["input_rows"].values()):
            entry["error"] = "no stored data for these inputs"
            continue
        extent = [value.window for value in inputs.values() if value.collect().num_rows]
        window = TimeWindow(min(w.start for w in extent), max(w.end for w in extent))
        entry["read_window"] = [window.start.isoformat(), window.end.isoformat()]
        context = InputBatch(binding.signature, revision, 0, revision, window, window,
                             binding.row, binding.result)
        builder = OutputBuilder(binding.outputs)
        # No try/except: a breakpoint stops here and a traceback reaches the
        # caller, which is the whole reason to run locally.
        applications[binding.signature].transform(inputs, builder, context)
        for port, table in builder.values.items():
            shown = table if limit is None else table.slice(0, limit)
            entry["outputs"][port] = {
                "stream": binding.outputs[port][0],
                "ref_name": binding.output_ref_name(port),
                "value_kind": binding.outputs[port][1].value_kind,
                "rows": table.num_rows,
                "truncated": shown.num_rows < table.num_rows,
                "values": [{"time": time.isoformat(), "value": value}
                           for time, value in zip(shown["time"].to_pylist(),
                                                  shown["value"].to_pylist())],
            }
        for port in binding.outputs:
            entry["outputs"].setdefault(port, {"stream": binding.outputs[port][0],
                                               "ref_name": binding.output_ref_name(port),
                                               "value_kind": binding.outputs[port][1].value_kind,
                                               "rows": 0, "truncated": False, "values": []})
    return {"app": deployment.name, "graph_revision": revision, "bindings": bindings}
