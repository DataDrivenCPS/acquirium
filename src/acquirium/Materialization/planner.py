"""Resolve deployment declarations into concrete input/output bindings.

Compilation loads verified app code, runs build_query against the graph,
normalizes its matches, and assigns each output a stable identity. The result
is an ApplicationGraph plus App instances keyed by binding signature. No
lineage or timeseries output is published here, so deployment validation and
dry runs can use the same compiler without activating the result.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field, fields
from datetime import timedelta
from hashlib import sha256
import json
from typing import Any, Callable, Iterable, Mapping

from rdflib import Graph

from acquirium.Client.explore.core import Query
from acquirium.Client.explore.expr import AttrProxy
from acquirium.Materialization.models import (
    App, ApplicationGraph, Binding, OutputPort, OutputSpec, StreamDescriptor,
    _duration, parse_lookback,
)
from acquirium.Materialization.worker import load_entrypoint
from acquirium.Materialization.definitions import source_digest
from acquirium.internals.intervals import POS_INF, Interval, intersect, lives, merge
from acquirium.Materialization.admission import AdmissionError, Schedule, admit
from acquirium.Materialization.deployments import DeploymentChange, deployment, names as deployment_names
from acquirium.Materialization.publication import output_triples
from acquirium.Materialization.temporal import Row, ViewDefinition, view_graph, writer_conflicts
from acquirium.Storage.model_history import nt
from acquirium.internals.models import compute_ref_uri, looks_like_uri


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


class _VersionGraph:
    """One model version, with the view outputs published at that time,
    behind the surface planning queries use.

    The version key is derived from both contents, so query-builder caches
    keyed by graph version never mix two versions.
    """
    def __init__(self, store: object, version: object, publications: Graph | None = None) -> None:
        self._store, self._version, self._publications = store, version, publications
        published = "" if publications is None else _graph_digest(publications)
        self.key = sha256(f"{version.digest}:{published}".encode()).hexdigest()
    def sparql_query(self, query: str, include_dependencies: bool = True, *, wait_for_fresh: bool = False) -> dict:
        return self._store.query_version(self._version, query, include_dependencies, publications=self._publications)
    def graph_status(self) -> dict:
        return {"published_version": int(self.key[:15], 16)}


def _graph_digest(graph: Graph) -> str:
    return sha256("\n".join(sorted(nt(t) for t in graph)).encode()).hexdigest()


@dataclass(frozen=True)
class _QueryFacade:
    client: _GraphQueryClient
    def query(self) -> Query: return Query(client=self.client)
    @property
    def attr(self) -> AttrProxy: return AttrProxy(self.client)


def output_port(application_name: str, port: str,
                inputs: Mapping[str, Iterable[StreamDescriptor]],
                spec: OutputSpec) -> OutputPort:
    """Resolve one output port's durable identity, once, at planning time.

    A ``named`` output owns the exact reference name its author chose. A
    generated name follows the app, the port, and the bound inputs — not the
    deployment instance — so recompiling the same graph, or editing the app's
    code, keeps writing the same derived stream. The point that carries the
    output's metadata is named after that stream unless the author attached
    the output to a point of their own, which keeps it stable against edits
    to anything that is not the stream's identity.
    """
    if spec.stream_name is not None:
        ref_name = spec.stream_name
    else:
        pairs = sorted((alias, item.ref_uri) for alias, values in inputs.items() for item in values)
        ref_name = f"{application_name}:{port}:{sha256(_json([port, pairs]).encode()).hexdigest()}"
    ref_uri = str(compute_ref_uri(f"derived:{application_name}", ref_name))
    point_uri = spec.point_uri or f"urn:acquirium:derived-point:{ref_uri.rsplit('#', 1)[-1]}"
    return OutputPort(ref_uri, ref_name, point_uri, spec)


@dataclass(frozen=True)
class _MatchRow:
    """One deduplicated query-result row: its streams and its flat columns.

    ``columns`` is the row as ``Query.metadata()`` would name it — an alias
    holding the matched URI, plus ``<alias>_ref``, ``<alias>.label`` and
    ``<alias>.unit`` where the alias resolves to a stream. It is what
    reaches ``transform`` as ``context.result``.
    """
    streams: Mapping[str, tuple[dict[str, str | None], ...]]
    columns: Mapping[str, str | None]


def _query_rows(query: Query) -> tuple[_MatchRow, ...]:
    if not isinstance(query, Query): raise TypeError("build_query() must return an Acquirium query")
    # The query compiler's extN/unitN columns are an internal representation.
    # Convert them once here so transforms see stable alias-based columns.
    result, indices, graph = query.execute(include_dependencies=True), {}, query.query_graph
    indices = {column: index for index, column in enumerate(result.get("columns", ()))}
    rows, seen = [], {}
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
        # Every alias becomes a column, named the way Query.metadata() names
        # them, and reaches transform as context.result. Aliases bound to a
        # stream get their reference and metadata alongside.
        columns: dict[str, str | None] = {
            alias: str(cell(f"v{node}")) if cell(f"v{node}") is not None else None
            for node, alias in sorted(graph.aliases_reverse.items())
            if node not in graph.data_nodes
        }
        for alias, values in sorted(streams.items()):
            item = values[0]
            columns[alias] = item["point_uri"]
            columns[f"{alias}_ref"] = item["ref_uri"]
            columns[f"{alias}.label"] = item["label"]
            columns[f"{alias}.unit"] = item["unit"]
        # SPARQL joins can repeat a stream through unrelated graph triples.
        # Bind each distinct alias-to-stream set once, deterministically.
        key = _json({alias: sorted(item["ref_uri"] for item in values) for alias,values in streams.items()})
        if key in seen and seen[key] != columns:
            raise ValueError("query matches the same streams with different entity bindings; disambiguate the query")
        if streams and key not in seen:
            seen[key] = columns
            rows.append(_MatchRow({alias: tuple(values) for alias, values in streams.items()}, columns))
    return tuple(sorted(rows, key=lambda row: _json(row.columns)))


def _validated_outputs(name: str, declared: Mapping[str, Any]) -> dict[str, OutputSpec]:
    """Check an app's output schema before anything is deployed or run."""
    outputs: dict[str, OutputSpec] = {}
    for key, value in declared.items():
        if not isinstance(key, str) or not key or not key.strip():
            raise ValueError(f"app {name!r}: output port names must be non-empty strings, got {key!r}")
        if isinstance(value, OutputSpec):
            outputs[key] = value
            continue
        raise TypeError(
            f"app {name!r}: output {key!r} must be declared with aq.output.stream(...) "
            f"or aq.output.named(...), got {type(value).__name__}"
        )
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

    The wire format uses whole microseconds and ``"all"`` for unbounded
    lookback; Python instances use timedeltas and None respectively. Keeping
    conversion at this boundary lets scheduling use one duration representation.
    Entrypoint and digest identify code to import, not serialized App instances.
    Parameters are persisted so the planner can reconstruct those instances.
    """
    name: str
    entrypoint: str
    executable_digest: str
    outputs: Mapping[str, OutputSpec]
    lookback: timedelta | None = timedelta()
    lookahead: timedelta = timedelta()
    backfill: bool = False
    batch_delay: timedelta = timedelta()
    min_interval: timedelta | None = None
    parameters: Mapping[str, Any] = field(default_factory=dict)
    every: timedelta | None = None
    grouping: str | None = None

    def __post_init__(self):
        for value in (self.lookback, self.lookahead, self.batch_delay, self.min_interval):
            if value is not None:
                _duration(value)
        if self.grouping not in ("per_match", "all_matches"):
            raise ValueError(
                "grouping must be explicitly set to 'per_match' or 'all_matches'; "
                f"got {self.grouping!r}"
            )
        if self.every is not None and self.every <= timedelta():
            raise ValueError("every must be positive")

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
        outputs = _validated_outputs(name, app.outputs)
        return cls(name, f"{target.__module__}:{target.__qualname__}", source_digest(target),
                   outputs, parse_lookback(app.lookback), _duration(app.lookahead), bool(app.backfill),
                   _duration(app.batch_delay),
                   _duration(app.min_interval) if app.min_interval is not None else None,
                   params, _duration(app.every) if app.every is not None else None, app.grouping)

    def to_json(self) -> str:
        micros = lambda value: None if value is None else int(value.total_seconds() * 1_000_000)
        return _json({"name": self.name, "entrypoint": self.entrypoint,
            "executable_digest": self.executable_digest, "outputs": {k: asdict(v) for k,v in self.outputs.items()},
            "lookback": "all" if self.lookback is None else micros(self.lookback),
            "lookahead": micros(self.lookahead), "backfill": self.backfill,
            "batch_delay": micros(self.batch_delay),
            "min_interval": micros(self.min_interval), "parameters": dict(self.parameters),
            "every": micros(self.every), "grouping": self.grouping})

    @classmethod
    def from_json(cls, text: str) -> "Deployment":
        """Decode the deployment wire format, including its required grouping."""
        data = json.loads(text)
        unknown = data.keys() - {item.name for item in fields(cls)}
        if unknown:
            raise ValueError(f"unknown deployment fields: {', '.join(sorted(unknown))}")
        duration = lambda value: None if value is None else timedelta(microseconds=int(value))
        lookback = None if data["lookback"] == "all" else duration(data["lookback"])
        return cls(data["name"], data["entrypoint"], data["executable_digest"],
            {key: OutputSpec(**value) for key, value in data["outputs"].items()},
            lookback, duration(data.get("lookahead")) or timedelta(), bool(data.get("backfill")),
            duration(data.get("batch_delay", 0)) or timedelta(),
            duration(data.get("min_interval")), dict(data.get("parameters") or {}),
            duration(data.get("every")), data.get("grouping"))


class BindingPlanner:
    """Compile a plan with optimistic graph-version validation.

    Query execution does not hold a graph snapshot across all deployments.
    Checking the published version before and after compilation rejects a plan
    assembled while that version changes. The caller can retry against a fresh
    version instead of publishing bindings from inconsistent graph reads.
    """
    def __init__(self, graph: object, *, query_resolver: Callable[..., Any] | None = None,
                 record_resolver: Callable[..., Any] | None = None) -> None:
        self.graph, self.query_resolver, self.record_resolver = graph, query_resolver, record_resolver

    def _load(self, deployment: Deployment, search_path: str | None = None) -> App:
        target = load_entrypoint(deployment.entrypoint, deployment.executable_digest, search_path)
        if not isinstance(target, type) or not issubclass(target, App):
            raise TypeError(f"{deployment.entrypoint!r} is not an App")
        app = target(**deployment.parameters)
        app.backfill, app.batch_delay, app.min_interval = deployment.backfill, deployment.batch_delay, deployment.min_interval
        return app

    def _client(self, graph: object) -> _QueryFacade:
        return _QueryFacade(_GraphQueryClient(graph, self.query_resolver, self.record_resolver))

    def match_history(self, deployment: Deployment, *, search_path: str | None = None) -> list[tuple[_MatchRow, list[Interval]]]:
        """Every row the deployment's query has at some time, with its life:
        where it is a row, over all of time.

        The query runs once per model version, on the inferred model of that
        version alone (Acquirium's own graph is not part of the model).
        """
        app = self._load(deployment, search_path)
        found = []
        for start, end in self.graph.model_versions():
            version = self.graph.model_version(start)
            found.append((start, end, _query_rows(app.build_query(self._client(_VersionGraph(self.graph, version))))))
        return _row_lives(found)

    def temporal_plan(self, changes: Iterable[DeploymentChange], *, search_path: str | None = None) -> "TemporalPlan":
        """Plan every deployment of the log over all of time.

        Each definition's query runs on every model version, together with
        the output nodes the other views publish at that time: the named
        outputs of every view deployed then, and the generated outputs of the
        instances present then. A query never sees its own view's outputs. Generated outputs depend on the rows found, so planning
        repeats until the rows stop changing; with stratified selection that
        takes at most one round per view plus one. Raises ``AdmissionError``
        when the view set is not admitted.
        """
        changes = list(changes)
        definitions = []
        for name in deployment_names(changes):
            spans: dict[str, list[Interval]] = defaultdict(list)
            for start, end, text in deployment(changes, name):
                spans[text].append((start, end))
            for text, deployed in sorted(spans.items()):
                definitions.append(_PlannedDefinition(name, text, Deployment.from_json(text), tuple(merge(deployed))))
        apps = {d.key: self._load(d.deployment, search_path) for d in definitions}
        named = [(d.name, d.deployed, output_triples(d.name, {}, port, _port(d, port, {})))
                 for d in definitions for port, spec in d.deployment.outputs.items() if spec.stream_name is not None]
        cache: dict[tuple, tuple[_MatchRow, ...]] = {}
        rows: dict[tuple, list[tuple[_MatchRow, list[Interval]]]] = {}
        generated: list[tuple[str, tuple[Interval, ...], Graph]] = []
        for _ in range(len(definitions) + 2):
            published = named + generated
            pieces = _refine(self.graph.model_versions(), [span for _, spans, _ in published for span in spans])
            current = {}
            for definition in definitions:
                found = []
                for start, end in pieces:
                    version = self.graph.model_version(start)
                    graph = Graph()
                    for owner, spans, triples in published:
                        if owner != definition.name and any(left <= start < right for left, right in spans):
                            graph += triples
                    view = _VersionGraph(self.graph, version, graph)
                    key = (definition.key, view.key)
                    if key not in cache:
                        cache[key] = _query_rows(apps[definition.key].build_query(self._client(view)))
                    found.append((start, end, cache[key]))
                current[definition.key] = _row_lives(found)
            if current == rows:
                break
            rows = current
            generated = [item for definition in definitions for item in _generated(definition, rows[definition.key])]
        else:
            raise AdmissionError("queries select each other's generated outputs without end; "
                                 "read one of them through a named output instead")
        temporal = tuple(_temporal_definition(d, rows[d.key]) for d in definitions)
        graph = view_graph(temporal)
        starts: dict[str, int] = {}
        for d in definitions:
            starts[d.name] = min(starts.get(d.name, POS_INF), d.deployed[0][0])
        schedule = admit(starts, graph.reads, graph.selects)
        conflicts = writer_conflicts(temporal)
        if conflicts:
            raise AdmissionError("; ".join(str(conflict) for conflict in conflicts))
        return TemporalPlan(temporal, {d.text: d.deployment for d in definitions}, rows, schedule)

    def compile(self, deployments: Iterable[Deployment], graph_revision: int,
                *, search_path: str | None = None) -> tuple[ApplicationGraph, dict[str, App]]:
        before = int(self.graph.graph_status().get("published_version", 0))
        if before != graph_revision:
            raise RuntimeError("graph changed before materialization planning began")
        bindings, applications = [], {}
        for deployment in deployments:
            app = self._load(deployment, search_path)
            query = app.build_query(self._client(self.graph))
            rows = _query_rows(query)
            # Choose which matches share a call before assigning output names.
            # Inferring grouping from named outputs would silently change the
            # calculation when an author only intended to rename its result.
            named = sorted(key for key, spec in deployment.outputs.items() if spec.stream_name is not None)
            fans_out = deployment.grouping == "per_match"
            # Every binding carries the whole query result for context; only
            # the row it computes differs.
            result = tuple(row.columns for row in rows)
            if fans_out or len(rows) == 1:
                binding_rows = [(row.streams, row.columns) for row in rows]
            else:
                # One binding over the whole table: it keeps every row, so an
                # aggregate can group on the match itself.
                grouped: dict[str, dict[str, dict[str, str | None]]] = {}
                for row in rows:
                    for alias, matches in row.streams.items():
                        for item in matches:
                            grouped.setdefault(alias, {})[str(item["ref_uri"])] = item
                # A named aggregate still owns its stream when its last input
                # disappears. Keep empty aliases so retained outputs can retract.
                if not grouped:
                    for node in query.query_graph.data_nodes:
                        grouped[query.query_graph.aliases_reverse.get(node, f"data_{node}")] = {}
                binding_rows = [(
                    {alias: tuple(items.values()) for alias, items in sorted(grouped.items())},
                    None,   # an aggregate is about every row, so it has no single one
                )] if grouped else []
            if named and len(binding_rows) > 1:
                # An absolute stream has one owner, so a named output cannot
                # ride along with fan-out. The aggregate belongs downstream.
                raise ValueError(
                    f"app {deployment.name!r} declares named output(s) {named!r} alongside per-row "
                    f"fan-out over {len(binding_rows)} input groups; compute the aggregate in a second "
                    f"app whose query selects this app's derived streams"
                )
            for row_streams, row_columns in binding_rows:
                inputs = {
                    # The planner records the metadata the compiled query
                    # exposes: reference, point, unit, and label.
                    alias: tuple(StreamDescriptor(
                        ref_uri=str(item["ref_uri"]), point_uri=item.get("point_uri"),
                        unit=item.get("unit"), label=item.get("label"),
                    ) for item in items)
                    for alias, items in sorted(row_streams.items())
                }
                ports = {key: output_port(deployment.name, key, inputs, spec)
                         for key, spec in deployment.outputs.items()}
                binding = Binding(deployment.name, deployment.executable_digest, inputs, ports,
                                  deployment.lookback, deployment.lookahead,
                                  graph_revision, deployment.parameters, row_columns, result,
                                  every=deployment.every,
                                  custom_window=app.output_window if type(app).output_window is not App.output_window else None)
                # One instance serves all bindings of this deployment. Those
                # bindings may run concurrently, so App instances must not keep
                # mutable per-invocation state on self.
                bindings.append(binding); applications[binding.signature] = app
        # Planning is optimistic: discard a plan from a mixed graph view.
        if int(self.graph.graph_status().get("published_version", 0)) != graph_revision:
            raise RuntimeError("graph changed while materialization plan was being compiled")
        return ApplicationGraph(bindings), applications


@dataclass(frozen=True)
class _PlannedDefinition:
    name: str
    text: str
    deployment: Deployment
    deployed: tuple[Interval, ...]

    @property
    def key(self) -> tuple[str, str]:
        return (self.name, self.text)


@dataclass(frozen=True)
class TemporalPlan:
    """Every definition of every view, planned over all of time.

    ``definitions`` are the admission-level descriptions (instances, reads,
    writes); ``deployments`` maps definition texts to their deployments;
    ``rows`` holds each definition's query rows with their lives.
    """
    definitions: tuple[ViewDefinition, ...]
    deployments: Mapping[str, Deployment]
    rows: Mapping[tuple[str, str], list[tuple[_MatchRow, list[Interval]]]]
    schedule: Schedule


def _refine(pieces: Iterable[tuple[int, int]], spans: Iterable[Interval]) -> list[tuple[int, int]]:
    """``pieces`` split at every endpoint of ``spans`` that falls inside one."""
    cuts = sorted({p for span in spans for p in span})
    out = []
    for start, end in pieces:
        points = [start, *[p for p in cuts if start < p < end], end]
        out.extend(zip(points, points[1:]))
    return out


def _row_key(row: _MatchRow) -> str:
    return _json({"streams": row.streams, "columns": row.columns})


def _row_lives(found: Iterable[tuple[int, int, tuple[_MatchRow, ...]]]) -> list[tuple[_MatchRow, list[Interval]]]:
    by_key: dict[str, _MatchRow] = {}
    pieces = []
    for start, end, rows in found:
        keys = []
        for row in rows:
            key = _row_key(row)
            by_key[key] = row
            keys.append(key)
        pieces.append((start, end, keys))
    return sorted(((by_key[key], life) for key, life in lives(pieces).items()), key=lambda item: _row_key(item[0]))


def _descriptors(row: _MatchRow) -> dict[str, tuple[StreamDescriptor, ...]]:
    return {alias: tuple(StreamDescriptor(ref_uri=str(item["ref_uri"]), point_uri=item.get("point_uri"),
                                          unit=item.get("unit"), label=item.get("label")) for item in items)
            for alias, items in sorted(row.streams.items())}


def _port(definition: _PlannedDefinition, port: str, inputs: Mapping[str, Iterable[StreamDescriptor]]) -> OutputPort:
    return output_port(definition.name, port, inputs, definition.deployment.outputs[port])


def _generated(definition: _PlannedDefinition, rows: list[tuple[_MatchRow, list[Interval]]]):
    """``(view, times, triples)`` for the generated outputs of a definition."""
    ports = [port for port, spec in definition.deployment.outputs.items() if spec.stream_name is None]
    if definition.deployment.grouping == "per_match":
        for row, life in rows:
            present = tuple(intersect(life, definition.deployed))
            if present:
                inputs = _descriptors(row)
                for port in ports:
                    yield definition.name, present, output_triples(definition.name, inputs, port, _port(definition, port, inputs))
    else:
        for port in ports:
            yield definition.name, definition.deployed, output_triples(definition.name, {}, port, _port(definition, port, {}))


def _temporal_definition(definition: _PlannedDefinition, rows) -> ViewDefinition:
    spec = definition.deployment
    generated = frozenset(port for port, item in spec.outputs.items() if item.stream_name is None)
    per_match = spec.grouping == "per_match"
    single = {port: _port(definition, port, {}).ref_uri for port in spec.outputs
              if port not in generated or not per_match}
    lookahead = int(spec.lookahead.total_seconds() * 1_000_000)
    temporal_rows = []
    aliases = set()
    for row, life in rows:
        streams = {alias: tuple(str(item["ref_uri"]) for item in items) for alias, items in row.streams.items()}
        aliases.update(streams)
        outputs = {port: _port(definition, port, _descriptors(row)).ref_uri for port in generated} if per_match else {}
        temporal_rows.append(Row(_row_key(row), streams, tuple(life), outputs))
    return ViewDefinition(definition.name, definition.text, spec.grouping, definition.deployed,
                          tuple(temporal_rows), lookahead={alias: lookahead for alias in aliases},
                          outputs=single, generated=generated)
