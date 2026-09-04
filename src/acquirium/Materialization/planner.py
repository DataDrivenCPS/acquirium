"""Compile graph-resolved app declarations into a validated DAG."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import timedelta
import json
from typing import Any, Callable, Iterable, Mapping

from acquirium.Client.explore.core import Query
from acquirium.Materialization.incremental import (
    App, ApplicationGraph, Binding, OutputSpec, StreamDescriptor, _duration, parse_lookback,
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


@dataclass(frozen=True)
class _MatchRow:
    """One deduplicated query-result row: its streams and its entity bindings."""
    streams: Mapping[str, tuple[dict[str, str | None], ...]]
    entities: Mapping[str, str]


def _query_rows(query: Query) -> tuple[_MatchRow, ...]:
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
            label = cell(f"lbl{node}")
            streams.setdefault(graph.aliases_reverse.get(node, f"data_{node}"), []).append({
                "ref_uri": str(ref),
                "point_uri": str(cell(f"v{node}")) if cell(f"v{node}") is not None else None,
                "unit": str(unit) if unit is not None else None,
                "label": str(label) if label is not None else None,
            })
        # Entity aliases resolve the semantic side of the row: which heat
        # exchanger, which unit process. transform() sees them as context.
        entities = {alias: str(cell(f"v{node}"))
                    for node, alias in sorted(graph.aliases_reverse.items())
                    if node not in graph.data_nodes and cell(f"v{node}") is not None}
        # SPARQL joins can repeat a stream through unrelated graph triples.
        # Bind each distinct alias-to-stream set once, deterministically.
        key = _json({alias: sorted(item["ref_uri"] for item in values) for alias,values in streams.items()})
        if streams and key not in seen:
            seen.add(key)
            rows.append(_MatchRow({alias: tuple(values) for alias, values in streams.items()}, entities))
    return tuple(rows)


def _validated_outputs(name: str, declared: Mapping[str, Any]) -> dict[str, OutputSpec]:
    """Check an app's output schema before anything is deployed or run."""
    outputs: dict[str, OutputSpec] = {}
    for key, value in declared.items():
        if not isinstance(key, str) or not key or not key.strip():
            raise ValueError(f"app {name!r}: output port names must be non-empty strings, got {key!r}")
        if isinstance(value, OutputSpec):
            outputs[key] = value
            continue
        if not isinstance(value, Mapping):
            raise TypeError(
                f"app {name!r}: output {key!r} must be declared with aq.output.per_input(...) "
                f"or aq.output.named(...), got {type(value).__name__}"
            )
        try:
            outputs[key] = OutputSpec(**value)
        except TypeError as error:
            raise ValueError(f"app {name!r}: output {key!r}: {error}") from None
    claimed: dict[str, str] = {}
    for key, spec in outputs.items():
        if spec.stream_name is None: continue
        if spec.stream_name in claimed:
            raise ValueError(
                f"app {name!r}: outputs {claimed[spec.stream_name]!r} and {key!r} both claim the "
                f"stream name {spec.stream_name!r}; a named stream has one owner"
            )
        claimed[spec.stream_name] = key
    return outputs


@dataclass(frozen=True)
class Deployment:
    """The durable record of one deployed app: identity, outputs, scheduling.

    Durations are stored as whole microseconds; ``lookback`` may be ``"all"``.
    """
    name: str
    entrypoint: str
    executable_digest: str
    outputs: Mapping[str, OutputSpec]
    lookback: timedelta | None = timedelta()
    lookback_after: timedelta = timedelta()
    backfill: bool = False
    coalesce: timedelta = timedelta()
    max_delay: timedelta | None = None
    min_interval: timedelta | None = None
    parameters: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_class(cls, target: type[App], *, parameters: Mapping[str, Any] | None = None) -> "Deployment":
        if not issubclass(target, App):
            raise TypeError("deployment target must be an App class")
        if not target.outputs:
            raise ValueError("an app requires at least one declared output")
        if target.__module__ == "__main__":
            # The deployment ships only ``module:qualname`` plus a digest; the
            # server must import the same file. ``__main__`` can never resolve.
            raise ValueError(
                "an app class defined in a script's __main__ module cannot be deployed; "
                "move it into an importable module the server can load"
            )
        params = dict(parameters or {})
        app = target(**params)
        name = target.name or target.__name__
        outputs = _validated_outputs(name, target.outputs)
        return cls(name, f"{target.__module__}:{target.__qualname__}", source_digest(target),
                   outputs, parse_lookback(app.lookback), _duration(app.lookback_after), bool(app.backfill),
                   _duration(app.coalesce),
                   _duration(app.max_delay) if app.max_delay is not None else None,
                   _duration(app.min_interval) if app.min_interval is not None else None,
                   params)

    def to_json(self) -> str:
        micros = lambda value: None if value is None else int(value.total_seconds() * 1_000_000)
        return _json({"name": self.name, "entrypoint": self.entrypoint,
            "executable_digest": self.executable_digest, "outputs": {k: asdict(v) for k,v in self.outputs.items()},
            "lookback": "all" if self.lookback is None else micros(self.lookback),
            "lookback_after": micros(self.lookback_after), "backfill": self.backfill,
            "coalesce": micros(self.coalesce), "max_delay": micros(self.max_delay),
            "min_interval": micros(self.min_interval), "parameters": dict(self.parameters)})

    @classmethod
    def from_json(cls, text: str) -> "Deployment":
        data = json.loads(text)
        duration = lambda value: None if value is None else timedelta(microseconds=int(value))
        lookback = None if data["lookback"] == "all" else duration(data["lookback"])
        return cls(data["name"], data["entrypoint"], data["executable_digest"],
            {key: OutputSpec(**value) for key, value in data["outputs"].items()},
            lookback, duration(data.get("lookback_after")) or timedelta(), bool(data.get("backfill")),
            duration(data.get("coalesce")) or timedelta(), duration(data.get("max_delay")),
            duration(data.get("min_interval")), dict(data.get("parameters") or {}))


class BindingPlanner:
    """One deep operation: resolve applications against one pinned graph view."""
    def __init__(self, graph: object, *, query_resolver: Callable[..., Any] | None = None,
                 record_resolver: Callable[..., Any] | None = None) -> None:
        self.graph, self.query_resolver, self.record_resolver = graph, query_resolver, record_resolver

    def compile(self, deployments: Iterable[Deployment], graph_revision: int) -> tuple[ApplicationGraph, dict[str, App]]:
        before = int(self.graph.graph_status().get("published_version", 0))
        if before != graph_revision:
            raise RuntimeError("graph changed before materialization planning began")
        bindings, applications = [], {}
        for deployment in deployments:
            target = load_entrypoint(deployment.entrypoint, deployment.executable_digest)
            if not isinstance(target, type) or not issubclass(target, App):
                raise TypeError(f"{deployment.entrypoint!r} is not an App")
            app = target(**deployment.parameters)
            query = app.build_query(_QueryFacade(_GraphQueryClient(self.graph, self.query_resolver, self.record_resolver)))
            rows = _query_rows(query)
            # The output declaration decides the grouping. A per_input output
            # fans out: one binding per query-result row. All-named outputs
            # aggregate: one binding over the combined result (which keeps a
            # lone row's entity bindings, since it *is* that row).
            named = sorted(key for key, spec in deployment.outputs.items() if spec.stream_name is not None)
            fans_out = any(spec.stream_name is None for spec in deployment.outputs.values())
            if fans_out or len(rows) == 1:
                binding_rows = rows
            else:
                grouped: dict[str, dict[str, dict[str, str | None]]] = {}
                for row in rows:
                    for alias, matches in row.streams.items():
                        for item in matches:
                            grouped.setdefault(alias, {})[str(item["ref_uri"])] = item
                binding_rows = (_MatchRow({
                    alias: tuple(items.values()) for alias, items in sorted(grouped.items())
                }, {}),) if grouped else ()
            if named and len(binding_rows) > 1:
                # An absolute stream has one owner, so a named output cannot
                # ride along with fan-out. The aggregate belongs downstream.
                raise ValueError(
                    f"app {deployment.name!r} declares named output(s) {named!r} alongside per-input "
                    f"fan-out over {len(binding_rows)} input groups; compute the aggregate in a second "
                    f"app whose query selects this app's derived streams"
                )
            for row in binding_rows:
                inputs = {
                    # The planner records the metadata the compiled query
                    # exposes: reference, point, unit, and label.
                    alias: tuple(StreamDescriptor(
                        ref_uri=str(item["ref_uri"]), point_uri=item.get("point_uri"),
                        unit=item.get("unit"), label=item.get("label"),
                    ) for item in matches)
                    for alias, matches in sorted(row.streams.items())
                }
                ports = {key: (Binding.derive_output_uri(deployment.name, key, inputs, spec), spec)
                         for key, spec in deployment.outputs.items()}
                binding = Binding(deployment.name, deployment.executable_digest, inputs, ports,
                                  deployment.lookback, deployment.lookback_after,
                                  graph_revision, deployment.parameters, row.entities)
                bindings.append(binding); applications[binding.signature] = app
        # Planning is optimistic: discard a plan from a mixed graph view.
        if int(self.graph.graph_status().get("published_version", 0)) != graph_revision:
            raise RuntimeError("graph changed while materialization plan was being compiled")
        return ApplicationGraph(bindings), applications
