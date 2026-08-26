"""Compile graph-resolved transformation declarations into a validated DAG."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from hashlib import sha256
import json
from typing import Any, Callable, Iterable, Mapping

from acquirium.Client.explore.core import Query
from acquirium.Materialization.incremental import (
    AllAvailable, ApplicationGraph, AroundChange, Binding, Changed, Current, Every,
    OnChange, OutputSpec, StreamDescriptor, Transformation,
)
from acquirium.Materialization.worker import load_entrypoint
from acquirium.Materialization.definitions import source_digest
from acquirium.internals.models import looks_like_uri


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class _GraphQueryClient:
    """The deliberately narrow graph capability available while planning."""
    def __init__(self, graph: object, resolver=None, record_resolver=None) -> None:
        self._graph, self._resolver, self._record_resolver = graph, resolver, record_resolver
    def sparql_query(self, query: str, include_dependencies: bool = True, *, wait_for_fresh: bool = False) -> dict:
        return self._graph.sparql_query(query, include_dependencies=include_dependencies, wait_for_fresh=wait_for_fresh)
    @staticmethod
    def _best_uri(value: Any) -> str | None:
        if isinstance(value, str): return value
        if isinstance(value, (tuple, list)) and value and isinstance(value[0], Mapping):
            return str(value[0]["uri"]) if value[0].get("uri") is not None else None
        return None
    def resolve(self, value: str | Mapping[str, tuple[Any, str | None]], kind: str | None = None, **kwargs: Any):
        if isinstance(value, Mapping):
            pending = {key: item for key, item in value.items() if item[0] is not None and not looks_like_uri(item[0])}
            result = {key: (str(item[0]) if item[0] is not None and looks_like_uri(item[0]) else None) for key,item in value.items()}
            if pending and self._record_resolver is not None:
                matches = self._record_resolver(pending, **kwargs) or {}
                result.update({key: self._best_uri(matches.get(key)) for key in pending})
            elif pending and self._resolver is not None:
                result.update({key: self._best_uri(self._resolver(text, field_kind, **kwargs)) for key,(text,field_kind) in pending.items()})
            elif pending: raise ValueError("query text requires a resolver")
            return result
        return value if looks_like_uri(value) else self._best_uri(self._resolver(value, kind, **kwargs))
    def expand_uri(self, value: str) -> str: return value
    def graph_version(self) -> int: return int(self._graph.graph_status().get("published_version", 0))


@dataclass(frozen=True)
class _QueryFacade:
    client: _GraphQueryClient
    def query(self) -> Query: return Query(client=self.client)


def _query_rows(query: Query) -> tuple[dict[str, list[dict[str, str | None]]], ...]:
    if not isinstance(query, Query): raise TypeError("build_query() must return an Acquirium query")
    result, indices, graph = query.execute(include_dependencies=True), {}, query.query_graph
    indices = {column: index for index, column in enumerate(result.get("columns", ()))}
    rows, seen = [], set()
    for raw in result.get("rows", ()):
        cell = lambda name: raw[indices[name]] if name in indices and indices[name] < len(raw) else None
        streams: dict[str, list[dict[str, str | None]]] = {}
        for node in graph.data_nodes:
            ref = cell(f"ext{node}")
            if ref is None: continue
            unit = cell(f"unit{node}") or cell(f"extunit{node}")
            streams.setdefault(graph.aliases_reverse.get(node, f"data_{node}"), []).append({"ref_uri": str(ref), "point_uri": str(cell(f"v{node}")) if cell(f"v{node}") is not None else None, "unit": str(unit) if unit is not None else None})
        # SPARQL joins can repeat a stream through unrelated graph triples.
        # Bind each distinct alias-to-stream set once, deterministically.
        key = _json({alias: sorted(item["ref_uri"] for item in values) for alias,values in streams.items()})
        if streams and key not in seen: seen.add(key); rows.append(streams)
    return tuple(rows)


@dataclass(frozen=True)
class Deployment:
    name: str
    entrypoint: str
    executable_digest: str
    outputs: Mapping[str, OutputSpec]
    window: object
    trigger: object
    start: object
    parameters: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_class(cls, target: type[Transformation], *, parameters: Mapping[str, Any] | None = None) -> "Deployment":
        if not issubclass(target, Transformation):
            raise TypeError("deployment target must be a Transformation class")
        if not target.outputs:
            raise ValueError("a transformation requires at least one named output")
        params = dict(parameters or {})
        app = target(**params)
        name = target.name or target.__name__
        outputs = {str(key): value if isinstance(value, OutputSpec) else OutputSpec(**value)
                   for key, value in target.outputs.items()}
        return cls(name, f"{target.__module__}:{target.__qualname__}", source_digest(target),
                   outputs, app.window, app.trigger, app.start, params)

    def to_json(self) -> str:
        return _json({"name": self.name, "entrypoint": self.entrypoint,
            "executable_digest": self.executable_digest, "outputs": {k: asdict(v) for k,v in self.outputs.items()},
            "window": _policy_json(self.window), "trigger": _policy_json(self.trigger), "start": type(self.start).__name__,
            "parameters": dict(self.parameters)})

    @classmethod
    def from_json(cls, text: str) -> "Deployment":
        data = json.loads(text)
        start_name = data.get("start")
        if start_name not in {"Current", "AllAvailable"}: raise ValueError("unknown start policy")
        return cls(data["name"], data["entrypoint"], data["executable_digest"],
            {key: OutputSpec(**value) for key, value in data["outputs"].items()},
            _window(data["window"]), _trigger(data["trigger"]),
            Current() if start_name == "Current" else AllAvailable(),
            dict(data.get("parameters") or {}))


def _policy_json(value: object) -> dict[str, object]:
    # JSON has no duration type. Microseconds preserve exact scheduler units.
    fields = {}
    for key, item in getattr(value, "__dict__", {}).items():
        fields[key] = int(item.total_seconds() * 1_000_000) if hasattr(item, "total_seconds") else item
    return {"kind": type(value).__name__, **fields}


def _window(value: Mapping[str, Any]) -> Changed | AroundChange | AllAvailable:
    if value["kind"] == "Changed": return Changed()
    if value["kind"] == "AllAvailable": return AllAvailable()
    if value["kind"] == "AroundChange":
        from datetime import timedelta
        return AroundChange(timedelta(microseconds=int(value.get("before", 0))), timedelta(microseconds=int(value.get("after", 0))))
    raise ValueError("unknown window policy")


def _trigger(value: Mapping[str, Any]) -> OnChange | Every:
    from datetime import timedelta
    if value["kind"] == "OnChange":
        return OnChange(timedelta(microseconds=int(value.get("coalesce", 0))), timedelta(microseconds=int(value["max_delay"])) if value.get("max_delay") is not None else None)
    if value["kind"] == "Every": return Every(timedelta(microseconds=int(value["interval"])))
    raise ValueError("unknown trigger policy")


def _resolved_output(spec: OutputSpec, inputs: Mapping[str, tuple[StreamDescriptor, ...]]) -> OutputSpec:
    """Apply the deliberately conservative inheritance rule at compile time."""
    if not spec.inherit:
        return spec
    streams = [stream for values in inputs.values() for stream in values]
    if not streams:
        return spec
    inherited: dict[str, Any] = {}
    for field in ("unit", "quantity_kind", "medium", "substance"):
        values = {getattr(stream, field) for stream in streams}
        if len(values) == 1 and None not in values and getattr(spec, field) is None:
            inherited[field] = values.pop()
    properties = dict(spec.properties or {})
    for predicate in spec.inherit_properties:
        common = set(streams[0].properties.get(predicate, ()))
        for stream in streams[1:]: common.intersection_update(stream.properties.get(predicate, ()))
        if common and predicate not in properties: properties[predicate] = tuple(sorted(common))
    return replace(spec, properties=properties or None, **inherited)


class BindingPlanner:
    """One deep operation: resolve applications against one pinned graph view."""
    def __init__(self, graph: object, *, query_resolver: Callable[..., Any] | None = None,
                 record_resolver: Callable[..., Any] | None = None) -> None:
        self.graph, self.query_resolver, self.record_resolver = graph, query_resolver, record_resolver

    def compile(self, deployments: Iterable[Deployment], graph_revision: int) -> tuple[ApplicationGraph, dict[str, Transformation]]:
        before = int(self.graph.graph_status().get("published_version", 0))
        if before != graph_revision:
            raise RuntimeError("graph changed before materialization planning began")
        bindings, applications = [], {}
        for deployment in deployments:
            target = load_entrypoint(deployment.entrypoint, deployment.executable_digest)
            if not isinstance(target, type) or not issubclass(target, Transformation):
                raise TypeError(f"{deployment.entrypoint!r} is not a Transformation")
            app = target(**deployment.parameters)
            query = app.build_query(_QueryFacade(_GraphQueryClient(self.graph, self.query_resolver, self.record_resolver)))
            rows = _query_rows(query)
            if app.binding_mode == "full_query":
                # A full-query app consumes one combined stream set; per-row
                # apps below receive independent semantic query bindings.
                grouped: dict[str, dict[str, dict[str, str | None]]] = {}
                for row in rows:
                    for alias, matches in row.items():
                        for item in matches:
                            grouped.setdefault(alias, {})[str(item["ref_uri"])] = item
                binding_rows = ({
                    alias: tuple(items.values()) for alias, items in sorted(grouped.items())
                },) if grouped else ()
            elif app.binding_mode == "per_row":
                binding_rows = rows
            else:
                raise ValueError(f"unknown transformation binding mode {app.binding_mode!r}")
            for row in binding_rows:
                inputs = {
                    alias: tuple(StreamDescriptor(
                        ref_uri=str(item["ref_uri"]), point_uri=item.get("point_uri"), unit=item.get("unit")
                    ) for item in matches)
                    for alias, matches in sorted(row.items())
                }
                ports = {key: (Binding.derive_output_uri(deployment.name, key, inputs), _resolved_output(spec, inputs))
                         for key, spec in deployment.outputs.items()}
                binding = Binding(deployment.name, deployment.executable_digest, inputs, ports, app.window, graph_revision, deployment.parameters)
                bindings.append(binding); applications[binding.signature] = app
        # Planning is optimistic: discard a plan from a mixed graph view.
        if int(self.graph.graph_status().get("published_version", 0)) != graph_revision:
            raise RuntimeError("graph changed while materialization plan was being compiled")
        return ApplicationGraph(bindings), applications
