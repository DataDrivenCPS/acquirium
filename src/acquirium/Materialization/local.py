"""Run an app in the caller's process against a server's stored data.

A check normally executes on the server, because that is where the data is.
That also puts the running app out of reach: a ``breakpoint()`` opens on the
server's stdin, while transform errors are returned as per-binding messages
instead of raising a traceback in the caller's terminal.

This module compiles the same app and runs it here instead, pulling its
inputs over the client API. The app executes under the caller's own
interpreter, so debuggers, tracebacks, and profilers all work normally, and
the server never has to import the app at all.

Unlike RevisionStore's server-side check, separate HTTP reads do not share a
single database snapshot. This path is useful for debugging calculations, not
for establishing a consistent publication frontier while ingestion continues.
"""
from __future__ import annotations

from acquirium.Materialization.checks import check_entry, check_outputs

from datetime import datetime, timezone
from dataclasses import replace
from acquirium.Materialization.revision_store import RevisionStore
from typing import Any

import pyarrow as pa

from acquirium.Materialization.models import (
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
    # Local checks fetch retained values without row revision metadata. Expose
    # the full table as changes to exercise the calculation over that history;
    # this does not reproduce an incremental invocation's changed-row subset.
    return StreamSet(alias, window, descriptors, table, table, converter=converter)


def check_app(client: Any, target: type, *, parameters: dict | None = None,
              limit: int | None = None) -> dict[str, Any]:
    """Compile and run ``target`` here, against ``client``'s server data.

    Returns the same document as a server-side check, so callers can render
    either the same way. The app executes in this process and transform errors
    propagate to the caller for debugging. Inputs are fetched through separate
    HTTP requests, so they do not have the shared snapshot guarantee of a
    server-side check. Revision fields here are diagnostic placeholders derived
    from the graph version; they must not be used as timeseries progress.
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
        entry = check_entry(binding)
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
        owned = RevisionStore._output_window(binding, window)
        read = window if binding.lookback is None else TimeWindow(owned.start - binding.lookback, owned.end + binding.lookahead)
        inputs = {alias: replace(value, window=read, every=binding.every, _scheduled=True) for alias, value in inputs.items()}
        entry["read_window"] = [read.start.isoformat(), read.end.isoformat()]
        context = InputBatch(binding.signature, revision, 0, revision, window, read,
                             binding.row, binding.result, output_window=owned)
        builder = OutputBuilder(binding.outputs)
        # No try/except: a breakpoint stops here and a traceback reaches the
        # caller, which is the whole reason to run locally.
        applications[binding.signature].transform(inputs, builder, context)
        entry["outputs"] = check_outputs(binding, context, builder.values, limit)
    return {"app": deployment.name, "graph_revision": revision, "bindings": bindings}
