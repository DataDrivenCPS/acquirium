from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol

from acquirium.Apps.base import App, Output, app_source_id
from acquirium.Apps.mapped import StreamMapping
from acquirium.internals.models import AppContext, AppOutputSpec


class AppContractError(ValueError):
    """Raised when an app returns values outside its declared contract."""


class DryRunMutationError(RuntimeError):
    """Raised when an app attempts an Acquirium mutation during preview."""


_MUTATING_METHODS = {
    "delete_app",
    "delete_logs",
    "delete_timeseries",
    "generate_grafana_dashboard",
    "insert_graph",
    "insert_graph_file",
    "insert_log",
    "insert_timeseries",
    "insert_timeseries_arrow",
    "insert_timeseries_batch",
    "register_app",
    "register_datasource",
    "register_stream",
    "register_streams",
    "run_app",
    "sparql_update",
    "stop_app",
}


class ReadOnlyAcquirium:
    """Delegate reads to an Acquirium API while rejecting known mutations.

    Query objects returned by the high-level API remain bound to the real
    low-level client, so normal query/data reads behave exactly as they do in
    production. The nested ``client`` attribute is wrapped too, preventing an
    app from accidentally bypassing the high-level guard.
    """

    def __init__(self, target: Any):
        self._target = target

    def __getattr__(self, name: str) -> Any:
        if name in _MUTATING_METHODS or name.startswith(("insert_", "delete_", "register_")):
            def blocked(*args: Any, **kwargs: Any) -> Any:
                raise DryRunMutationError(
                    f"{name}() is disabled while previewing an app"
                )

            return blocked
        value = getattr(self._target, name)
        if name == "client":
            return ReadOnlyAcquirium(value)
        return value


class OutputSink(Protocol):
    def emit(self, source_id: str, outputs: list[Output]) -> list[dict[str, Any]]:
        """Deliver outputs and return a structured description of each effect."""


@dataclass
class AppExecutionResult:
    app_id: str
    outputs: list[Output]
    effects: list[dict[str, Any]]
    queries: dict[str, dict[str, Any]] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "outputs": [_summarize_output(out) for out in self.outputs],
            "effects": _json_safe(self.effects),
            "queries": _json_safe(self.queries),
            "timings": self.timings,
            "warnings": self.warnings,
        }


@dataclass
class AppDebugSession:
    """Prepared, read-only app state exposed by ``acquirium app debug``."""

    app: App
    aq: ReadOnlyAcquirium
    queries: dict[str, Any]
    ctx: AppContext
    state: Any
    streams: list[Any]
    declarations: list[AppOutputSpec]

    def transform(self, stream: Any | None = None) -> Any:
        """Call a mapped app's transform for one stream (the first by default)."""
        method = getattr(self.app, "transform", None)
        if not callable(method):
            raise TypeError(f"{type(self.app).__name__} does not define transform()")
        if stream is None:
            if not self.streams:
                raise ValueError("the app selector matched no input streams")
            stream = self.streams[0]
        return method(stream, self.ctx)

    def run(self) -> list[Output]:
        """Run the app contract without persisting any returned outputs."""
        return validate_outputs(self.app.run(self.ctx), self.declarations)

    def namespace(self) -> dict[str, Any]:
        """Names made available in the interactive debugging shell."""
        return {
            "app": self.app,
            "aq": self.aq,
            "ctx": self.ctx,
            "queries": self.queries,
            "query": self.ctx.query,
            "state": self.state,
            "streams": self.streams,
            "stream": self.streams[0] if self.streams else None,
            "transform": self.transform,
            "run": self.run,
        }


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _summarize_output(output: Output) -> dict[str, Any]:
    payload = output.payload
    if output.kind == "timeseries":
        return {
            "kind": output.kind,
            "point_uri": payload.get("point_uri"),
            "row_count": len(payload.get("rows") or []),
        }
    return {"kind": output.kind, "payload": _json_safe(payload)}


def normalize_output_specs(items: Iterable[Any]) -> list[AppOutputSpec]:
    """Normalize and validate an iterable of output declarations."""
    specs: list[AppOutputSpec] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, AppOutputSpec):
            spec = item
        elif isinstance(item, dict):
            spec = AppOutputSpec(**item)
        else:
            raise AppContractError("outputs must contain AppOutputSpec objects or dictionaries")
        if not spec.point_uri:
            raise AppContractError("output declarations require a non-empty point_uri")
        if spec.point_uri in seen:
            raise AppContractError(f"duplicate output declaration for {spec.point_uri!r}")
        seen.add(spec.point_uri)
        specs.append(spec)
    return specs


def output_specs(app: App) -> list[AppOutputSpec]:
    """Normalize one app's class-level output declarations."""
    return normalize_output_specs(list(getattr(app, "outputs", []) or []))


def resolved_output_specs(app: App, queries: dict[str, Any]) -> list[AppOutputSpec]:
    """Return static declarations or app-resolved dynamic declarations."""
    resolver = getattr(app, "resolve_output_specs", None)
    if callable(resolver):
        return normalize_output_specs(resolver(queries))
    return output_specs(app)


def validate_outputs(
    outputs: Any,
    declarations: Iterable[AppOutputSpec] = (),
) -> list[Output]:
    """Validate output structure and, when present, declared destinations."""
    if not isinstance(outputs, list):
        raise AppContractError(
            f"App.run() must return list[Output], got {type(outputs).__name__}"
        )

    declared = {spec.point_uri: spec for spec in declarations}
    validated: list[Output] = []
    for index, output in enumerate(outputs, start=1):
        if not isinstance(output, Output):
            raise AppContractError(
                f"output {index} must be Output, got {type(output).__name__}"
            )
        if output.kind not in {"timeseries", "event", "trigger"}:
            raise AppContractError(f"output {index} has unsupported kind {output.kind!r}")

        point_uri = output.payload.get("point_uri")
        if output.kind in {"timeseries", "event"} and not point_uri:
            raise AppContractError(f"{output.kind} output {index} requires point_uri")
        if output.kind == "timeseries":
            rows = output.payload.get("rows")
            if not isinstance(rows, list):
                raise AppContractError(f"timeseries output {index} requires a rows list")
            for row_index, row in enumerate(rows, start=1):
                if not isinstance(row, (list, tuple)) or len(row) != 2:
                    raise AppContractError(
                        f"timeseries output {index} row {row_index} must be (timestamp, value)"
                    )
        if output.kind == "trigger" and not output.payload.get("url"):
            raise AppContractError(f"trigger output {index} requires url")

        # Existing trigger apps may intentionally omit point_uri. If supplied,
        # it participates in the same declaration validation as stored output.
        if declared and point_uri:
            spec = declared.get(point_uri)
            if spec is None:
                raise AppContractError(
                    f"output {index} targets undeclared point_uri {point_uri!r}"
                )
            if spec.kind != output.kind:
                raise AppContractError(
                    f"output {index} kind {output.kind!r} does not match declared "
                    f"kind {spec.kind!r} for {point_uri!r}"
                )
            output_ref_name = output.payload.get("ref_name")
            if spec.ref_name and output_ref_name and spec.ref_name != output_ref_name:
                raise AppContractError(
                    f"output {index} ref_name {output_ref_name!r} does not match "
                    f"declared ref_name {spec.ref_name!r}"
                )
        validated.append(output)
    return validated


def describe_queries(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Describe resolved query inputs without fetching timeseries rows."""
    descriptions: dict[str, dict[str, Any]] = {}
    for name, query in bundle.items():
        item: dict[str, Any] = {}
        if hasattr(query, "to_dict"):
            item["query"] = query.to_dict()
        if hasattr(query, "resolved_nodes"):
            try:
                item["matched_streams"] = query.resolved_nodes(only_data_nodes=True)
            except TypeError:
                item["matched_streams"] = query.resolved_nodes()
        descriptions[name] = item
    return descriptions


def preview_app(
    app: App,
    aq: Any,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    build_params: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    sink: OutputSink,
) -> AppExecutionResult:
    """Run an app once against live reads while recording all output effects."""
    read_only = ReadOnlyAcquirium(aq)
    app._bind_graph_api(read_only, app_source_id(app.name))
    timings: dict[str, float] = {}

    before = time.perf_counter()
    bundle = app.build_query(read_only)
    if not isinstance(bundle, dict):
        bundle = {"default": bundle}
    timings["query_build_seconds"] = time.perf_counter() - before

    queries = dict(bundle)
    query = queries.get("default") or (next(iter(queries.values())) if queries else None)
    descriptions = describe_queries(queries)

    before = time.perf_counter()
    build_ctx = AppContext(
        app_id=app.name,
        started_at=datetime.now(timezone.utc),
        start=start,
        end=end,
        query=query,
        queries=queries,
        params=build_params or {},
    )
    state = app.build_app(build_ctx)
    timings["build_seconds"] = time.perf_counter() - before

    before = time.perf_counter()
    run_ctx = AppContext(
        app_id=app.name,
        started_at=datetime.now(timezone.utc),
        start=start,
        end=end,
        query=query,
        queries=queries,
        params=params or {},
        state=state,
    )
    declarations = resolved_output_specs(app, queries)
    outputs = validate_outputs(app.run(run_ctx), declarations)
    timings["run_seconds"] = time.perf_counter() - before

    before = time.perf_counter()
    effects = sink.emit(app_source_id(app.name), outputs)
    timings["emit_seconds"] = time.perf_counter() - before
    warnings: list[str] = []
    if not declarations:
        warnings.append("app declares no outputs; destination validation was skipped")
    return AppExecutionResult(
        app_id=app.name,
        outputs=outputs,
        effects=effects,
        queries=descriptions,
        timings=timings,
        warnings=warnings,
    )


def prepare_app_debug(
    app: App,
    aq: Any,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    build_params: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> AppDebugSession:
    """Prepare live app inputs and state without executing or emitting outputs."""
    app.validate_definition()
    read_only = ReadOnlyAcquirium(aq)
    app._bind_graph_api(read_only, app_source_id(app.name))
    bundle = app.build_query(read_only)
    if not isinstance(bundle, dict):
        bundle = {"default": bundle}
    queries = dict(bundle)
    query = queries.get("default") or (next(iter(queries.values())) if queries else None)
    build_ctx = AppContext(
        app_id=app.name,
        started_at=datetime.now(timezone.utc),
        start=start,
        end=end,
        query=query,
        queries=queries,
        params=build_params or {},
    )
    state = app.build_app(build_ctx)
    ctx = AppContext(
        app_id=app.name,
        started_at=datetime.now(timezone.utc),
        start=start,
        end=end,
        query=query,
        queries=queries,
        params=params or {},
        state=state,
    )
    stream_loader = getattr(app, "streams", None)
    streams = list(stream_loader(ctx)) if callable(stream_loader) else []
    return AppDebugSession(
        app=app,
        aq=read_only,
        queries=queries,
        ctx=ctx,
        state=state,
        streams=streams,
        declarations=resolved_output_specs(app, queries),
    )


def resolve_stream_mappings(app: App, aq: Any) -> list[StreamMapping]:
    """Resolve an app definition into input/output stream pairs without running it."""
    app.validate_definition()
    read_only = ReadOnlyAcquirium(aq)
    app._bind_graph_api(read_only, app_source_id(app.name))
    bundle = app.build_query(read_only)
    if not isinstance(bundle, dict):
        bundle = {"default": bundle}
    queries = dict(bundle)

    resolver = getattr(app, "resolve_mappings", None)
    if callable(resolver):
        return list(resolver(queries))

    mappings: list[StreamMapping] = []
    for output in resolved_output_specs(app, queries):
        for input_point_uri in output.depends_on:
            mappings.append(StreamMapping(
                input_point_uri=input_point_uri,
                input_ref_uri=None,
                output_point_uri=output.point_uri,
                output_ref_name=output.ref_name,
            ))
    return mappings
