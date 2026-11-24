"""Semantic selections compiled into complete Escapement plans.

Acquirium owns declarations and RDF discovery. Escapement owns validation,
execution, history, publication, and recovery of accepted computation.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
import functools
from hashlib import sha256
import json
import os
import re
from uuid import uuid4
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, Sequence

from acquirium.app_spec import FixedBucket, Native, Pointwise, PythonFunction, Rolling
from acquirium.functions import WINDOW_BUILTINS
from escapement import Receipt
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium import replacements
from acquirium.replacements import app_of
from acquirium.internals.models import compute_stream_uri, looks_like_uri
from acquirium.Storage.graph_registry import DERIVED_SOURCE_PREFIX
from acquirium.Storage.values import normalize_value_kind
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_VALUE_KIND,
    INFERRED_VALUE_KIND,
    HAS_EXTERNAL_REFERENCE,
    HAS_UNIT,
    HAS_QUANTITY_KIND,
    VIRTUAL_POINT,
    PRODUCED_BY,
    OUTPUT_PORT,
    OUTPUT_GROUP,
    S223,
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return sha256(_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class Selection:
    """SPARQL selection with explicit stream and group columns.

    An app names each of its inputs by its key in ``App.inputs``; the
    selection says which streams that input reads.
    """

    query: str
    stream: str = "stream"
    group: str | None = "group"
    unit: str | None = "unit"
    point_unit: str | None = None
    target_unit: str | None = None
    exclude_app: str | None = None
    upstream: str | None = None
    upstream_port: str | None = None
    # Per-group operator parameters (JSON values), keyed by group. When set,
    # only the listed groups are selected; inspect warns about the groups skipped.
    parameters: Mapping[str, Mapping[str, Any]] | None = None
    # A maximum age in nanoseconds: read this input as of each window, its
    # latest reading at or before the window's timestamp and no older, instead
    # of the readings in the window. Such an input creates no windows.
    as_of: int | None = None
    # Which replacement stage each selected identity binds: "current" (the
    # chain's final stage), "raw" (its original input), or "stored" (exactly
    # the stream selected). Unset means "current", or "stored" for outputs_of.
    view: str | None = None

    @classmethod
    def outputs_of(cls, app, port, *, combine=False, parameters=None, as_of=None):
        """Select one output port of another app.

        By default each upstream group feeds its own operator, under the same
        group. ``combine=True`` feeds every upstream group into one operator.
        ``parameters`` maps groups to values the function reads from
        ``context["parameters"]``. ``as_of`` reads the port as of each window,
        no older than that many nanoseconds, as for any selection.
        Deployment rejects the selection while ``app`` is not deployed with
        this port.
        """
        if port not in app.outputs:
            raise ValueError(f"app {app.name!r} has no output port {port!r}")
        query = f"""
            SELECT * WHERE {{
                ?point <{PRODUCED_BY}> {Literal(app.name).n3()} ;
                       <{OUTPUT_PORT}> {Literal(port).n3()} ;
                       <{OUTPUT_GROUP}> ?group ;
                       <{HAS_EXTERNAL_REFERENCE}> ?stream .
            }}"""
        return cls(
            query,
            group=None if combine else "group",
            upstream=app.name,
            upstream_port=port,
            parameters=parameters,
            as_of=as_of,
        )

    @classmethod
    def from_query(
        cls,
        query,
        *,
        measurement: str,
        group: str | None = None,
        target_unit: str | None = None,
        exclude_app: str | None = None,
        parameters: Mapping[str, Mapping[str, Any]] | None = None,
        view: str | None = None,
    ):
        """Freeze the query DSL as SPARQL; resolve its inputs again on graph edits.

        ``measurement`` and ``group`` are aliases in the query: the
        measurement node whose streams this input reads, and the entity whose
        streams share one calculation. ``parameters`` maps groups to values
        the function reads from ``context["parameters"]``, as for
        ``outputs_of``. ``view`` chooses the replacement stage each matched
        measurement binds; by default it is the chain's final stage, as for a
        normal read.
        """
        node = query.query_graph.resolve_alias(measurement)
        if node is None or node not in query.query_graph.data_nodes:
            raise ValueError(f"{measurement!r} must name a measurement alias")
        group_node = query.query_graph.resolve_alias(group) if group else None
        if group and group_node is None:
            raise ValueError(f"unknown group alias {group!r}")
        return cls(
            query.to_sparql(),
            stream=f"ext{node}",
            group=f"v{group_node}" if group_node is not None else f"ext{node}",
            unit=f"extunit{node}",
            point_unit=f"unit{node}",
            target_unit=target_unit,
            exclude_app=exclude_app,
            parameters=parameters,
            view=view,
        )


def _selection_dict(selection):
    # Omitted when unset, so declarations saved before as-of inputs keep their form.
    data = asdict(selection)
    for key in ("as_of", "view"):
        if data[key] is None:
            del data[key]
    return data


@dataclass(frozen=True)
class Parameter:
    """A native function argument taken from each group's ``parameters``,
    e.g. ``aq.Native.threshold("alarm", above=aq.Parameter("limit"))``."""

    name: str


def _function_dict(function):
    if isinstance(function, (PythonFunction, Native)):
        function = function.to_dict()
    return {
        key: {"parameter": value.name} if isinstance(value, Parameter) else value
        for key, value in dict(function).items()
    }


def _bind_parameters(function, values, app, group):
    """The function with each ``{"parameter": name}`` argument replaced by the group's value."""
    bound = {}
    for key, value in function.items():
        if isinstance(value, Mapping) and set(value) == {"parameter"}:
            if value["parameter"] not in values:
                raise ValueError(
                    f"app {app!r} group {group} has no parameter {value['parameter']!r} "
                    f"for its function's {key!r}"
                )
            value = values[value["parameter"]]
        bound[key] = value
    return bound


@dataclass(frozen=True)
class Output:
    unit: str | None = None
    quantity_kind: str | None = None
    label: str | None = None
    on_group: bool = False
    # "text" for a port whose function emits text; reads default to it.
    value_kind: str = "numeric"


@dataclass(frozen=True)
class ReplacementOutput:
    """An output that continues one input's measurement as its authoritative stage.

    ``of`` names the input it replaces; an app with one input may leave it
    out. The quantity kind and value kind come from that input, and so does
    the unit unless ``unit`` names a compatible one. Reads of the input's
    measurement, and other apps that select it, follow the replacement.
    """

    of: str | None = None
    unit: str | None = None
    label: str | None = None


def _input_items(inputs):
    """``(name, selection)`` pairs for an app's inputs mapping."""
    if not isinstance(inputs, Mapping):
        raise ValueError("app inputs map input names to selections")
    items = []
    for name, value in inputs.items():
        if not isinstance(name, str) or not name:
            raise ValueError("input names must be nonempty strings")
        selections = [value] if isinstance(value, Selection) else value
        if (not isinstance(selections, (list, tuple)) or not selections
                or any(not isinstance(item, Selection) for item in selections)):
            raise ValueError(f"input {name!r} must be a Selection or a list of them")
        items.extend((name, item) for item in selections)
    return items


@dataclass(frozen=True)
class App:
    name: str
    # Each input's name keys one selection, or a list of selections that
    # feed the same input. The function's frame labels rows with the name.
    inputs: Mapping[str, Selection | Sequence[Selection]]
    outputs: Mapping[str, Output | ReplacementOutput]
    function: PythonFunction | Native | Mapping[str, Any]
    window: Pointwise | FixedBucket | Rolling = field(default_factory=Pointwise)

    def to_dict(self):
        if (
            not isinstance(self.name, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", self.name)
            or not self.outputs
        ):
            raise ValueError(
                "apps require a simple name (letters, digits, _, ., -) and declared outputs"
            )
        if not isinstance(self.window, (Pointwise, FixedBucket, Rolling)):
            raise ValueError("only pointwise, fixed-bucket and rolling windows are supported")
        function = _function_dict(self.function)
        items = _input_items(self.inputs)
        if any(not key for key in self.outputs):
            raise ValueError("output port names must be nonempty")
        outputs = {}
        for port, spec in self.outputs.items():
            if isinstance(spec, ReplacementOutput):
                of = spec.of
                if of is None:
                    if len(self.inputs) != 1:
                        raise ValueError(
                            f"replacement output {port!r} must name the input it replaces "
                            "with of=, because the app has several inputs"
                        )
                    of = next(iter(self.inputs))
                if of not in self.inputs:
                    raise ValueError(f"replacement output {port!r} names unknown input {of!r}")
                outputs[port] = {"replaces": of, "unit": spec.unit, "label": spec.label}
            elif isinstance(spec, Output):
                if type(spec.on_group) is not bool:
                    raise ValueError("output on_group must be a boolean")
                if spec.value_kind != normalize_value_kind(spec.value_kind):
                    raise ValueError("output value_kind must be 'numeric' or 'text'")
                outputs[port] = asdict(spec)
            else:
                raise ValueError(f"output {port!r} must be an Output or a ReplacementOutput")
        ages = {}
        for name, selection in items:
            if selection.as_of is not None and (
                type(selection.as_of) is not int or selection.as_of <= 0
            ):
                raise ValueError("as_of must be a positive number of nanoseconds")
            if ages.setdefault(name, selection.as_of) != selection.as_of:
                raise ValueError(f"input {name!r} is read as of different ages")
            if selection.view is not None and selection.view not in replacements.VIEWS:
                raise ValueError("selection view must be 'current', 'raw', or 'stored'")
            for values in (selection.parameters or {}).values():
                if not isinstance(values, Mapping) or any(
                    not isinstance(key, str) or not key for key in values
                ):
                    raise ValueError("parameters map groups to named values")
                try:
                    _json(dict(values))
                except (TypeError, ValueError) as error:
                    raise ValueError("parameter values must be JSON") from error
        if function.get("kind") == "python":
            sizes = [value for key, value in self.window.to_dict().items() if key.endswith("_ns")]
            sizes.extend(selection.as_of for _, selection in items if selection.as_of is not None)
            if any(type(value) is int and value % 1000 for value in sizes):
                raise ValueError("Python soft sensor windows and as-of ages must have microsecond precision")
        return dict(
            name=self.name,
            inputs=[{"input": name, **_selection_dict(selection)} for name, selection in items],
            outputs=outputs,
            function=function,
            window=self.window.to_dict(),
        )

    @classmethod
    def from_dict(cls, data):
        allowed = {"name", "inputs", "outputs", "function", "window"}
        if set(data) != allowed:
            raise ValueError(f"app fields must be {sorted(allowed)}")
        window = data["window"]
        if window["kind"] == "pointwise":
            value = Pointwise()
        elif window["kind"] == "fixed":
            value = FixedBucket(window["width_ns"], window.get("origin_ns", 0))
        elif window["kind"] == "rolling":
            value = Rolling(
                window["lookback_ns"],
                window["step_ns"],
                window.get("lookahead_ns", 0),
                window.get("origin_ns", 0),
            )
        else:
            raise ValueError("only pointwise, fixed-bucket and rolling windows are supported")
        inputs = {}
        for item in data["inputs"]:
            item = dict(item)
            name = item.pop("input")
            inputs.setdefault(name, []).append(Selection(**item))
        app = cls(
            data["name"],
            {name: items[0] if len(items) == 1 else items for name, items in inputs.items()},
            {key: _output_from_dict(item) for key, item in data["outputs"].items()},
            data["function"],
            value,
        )
        app.to_dict()
        return app

    def input_items(self):
        """``(name, selection)`` pairs, one per selection."""
        return _input_items(self.inputs)


def _output_from_dict(item):
    if "replaces" in item:
        return ReplacementOutput(of=item["replaces"], unit=item.get("unit"), label=item.get("label"))
    return Output(**item)


def _output_metadata(rdf, name, group, port, reference, spec, resolved=None):
    """Graph metadata for one output stream and its point.

    A replacement's point carries the unit, quantity kind and value kind it
    inherited, and is not attached to its group: reads and selections of
    the measurement it replaces already reach it.
    """
    if isinstance(spec, ReplacementOutput):
        resolved = resolved or {}
        unit, quantity_kind = resolved.get("unit"), resolved.get("quantity_kind")
        value_kind, on_group = resolved.get("value_kind", "numeric"), False
    else:
        unit, quantity_kind = spec.unit, spec.quantity_kind
        value_kind, on_group = spec.value_kind, spec.on_group
    ref, point = URIRef(reference), URIRef(f"{reference}__point")
    rdf.add((ref, ACQUIRIUM_SOURCE_ID, Literal(_source_id(name))))
    rdf.add((ref, ACQUIRIUM_REF_NAME, Literal(_json([group, port]))))
    rdf.add((ref, ACQUIRIUM_VALUE_KIND, Literal(value_kind)))
    rdf.add((point, RDF.type, VIRTUAL_POINT))
    rdf.add((point, RDF.type, S223.Property))
    if value_kind == "numeric" and unit and quantity_kind:
        rdf.add((point, RDF.type, S223.QuantifiableProperty))
    rdf.add((point, HAS_EXTERNAL_REFERENCE, ref))
    rdf.add((point, PRODUCED_BY, Literal(name)))
    rdf.add((point, OUTPUT_PORT, Literal(port)))
    rdf.add((point, OUTPUT_GROUP, URIRef(group) if looks_like_uri(group) else Literal(group)))
    if on_group:
        if not looks_like_uri(group):
            raise ValueError("output on_group requires a group URI")
        rdf.add((URIRef(group), S223.hasProperty, point))
    rdf.add((point, RDFS.label, Literal(spec.label or f"{name} {port}")))
    if unit:
        rdf.add((point, HAS_UNIT, URIRef(unit)))
    if quantity_kind:
        rdf.add((point, HAS_QUANTITY_KIND, URIRef(quantity_kind)))


def _warnings(report):
    # An empty selection is valid (e.g. before a driver registers its streams)
    # but is usually a typo, so report it instead of failing.
    return [
        f"input {role!r} matches no streams"
        for role, count in sorted(report["matches"].items())
        if not count
    ] + [
        f"input {role!r} skips groups without parameters: {', '.join(groups)}"
        for role, groups in sorted(report["skipped"].items())
    ]


def output_reference(app: str, group: str, port: str) -> str:
    return str(compute_stream_uri(_source_id(app), _json([group, port])))


# An app's operators are "<app>:<group digest>", one per selected group.
def _operator_id(app: str, group: str) -> str:
    return f"{app}:{_digest(group)}"


def _definition(app) -> dict[str, Any]:
    """An app's validated declaration, from an ``App`` or its dict form."""
    return app.to_dict() if isinstance(app, App) else App.from_dict(app).to_dict()


def _window_and_roles(declaration):
    spec = declaration["window"]
    if spec["kind"] == "pointwise":
        window = {"anchors": {"instant": {}}, "body": "point"}
    elif spec["kind"] == "fixed":
        window = {
            "anchors": {"grid": {"origin": spec.get("origin_ns", 0), "step": spec["width_ns"]}},
            "body": "bucket",
        }
    else:
        window = {
            "anchors": {"grid": {"origin": spec.get("origin_ns", 0), "step": spec["step_ns"]}},
            "body": "point",
        }
    roles = {}
    for item in declaration["inputs"]:
        age = item.get("as_of")
        reach = {"latest": {"max_age": age}} if age is not None else {
            "span": {"before": spec.get("lookback_ns", 0), "after": spec.get("lookahead_ns", 0)}
        }
        roles[item["input"]] = {"reach": reach, "driving": age is None, "reads": "bound"}
    return window, roles


_RATE_PORT = "__acquirium_rate"


def _ports(declaration):
    ports = set(declaration["outputs"])
    if declaration["function"]["kind"] == "integral":
        if _RATE_PORT in ports:
            raise ValueError(f"output port {_RATE_PORT!r} is reserved for integration")
        ports.add(_RATE_PORT)
    return sorted(ports)


def _plan_id(plan):
    return f"acquirium:{_digest(plan)}"


# Activation saves each Python module here by content hash, and the engine's
# workers import them from it.
MODULE_DIRECTORY = "app-functions"


def _source_id(name):
    return f"{DERIVED_SOURCE_PREFIX}{name}"


class Apps:
    def __init__(self, core, graph, directory, *, converter=None, lock=None):
        self.core, self.graph, self.converter = core, graph, converter
        self.lock = lock if lock is not None else RLock()
        self.directory = Path(directory)
        self._revision = -1
        self.error = None
        # Streams matched and groups skipped per app and input by the
        # installed compilation.
        self.report = {}
        self.state = {"apps": {}, "plan": None}
        self._pending = None
        # The adopted plan, the epoch it is known published from, and whether
        # an activation is under way. Replaced whole, so reads need no lock.
        self._published = (None, 0, False)

    def _artifact(self, code, *, prefix=""):
        """Durably retain immutable module bytes under their content hash."""
        digest = sha256(code).hexdigest()
        directory = self.directory / MODULE_DIRECTORY
        directory.mkdir(exist_ok=True)
        path = directory / f"{prefix}{digest}.py"
        if not path.exists() or path.read_bytes() != code:
            temporary = path.with_suffix(".tmp")
            with temporary.open("wb") as handle:
                handle.write(code)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _source(self, function):
        """Read the declared source at its content hash."""
        digest = function["sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Python functions require a SHA-256 content hash")
        source = self.directory / MODULE_DIRECTORY / f"{digest}.py"
        if source.exists():
            code = source.read_bytes()
            if sha256(code).hexdigest() != digest:
                raise ValueError("cached Python calculation content hash mismatch")
        else:
            try:
                code = Path(function["path"]).read_bytes()
            except OSError as error:
                raise ValueError(f"cannot read Python calculation {function['path']!r}: {error}") from error
            if sha256(code).hexdigest() != digest:
                raise ValueError(
                    "Python calculation changed since PythonFunction.from_file(); "
                    "rebuild its declaration before deploying"
                )
        return code

    def _compile(self, declarations, *, graph=None):
        graph = self.graph if graph is None else graph
        for name, declaration in declarations.items():
            for item in declaration["inputs"]:
                upstream, port = item.get("upstream"), item.get("upstream_port")
                if upstream and port not in declarations.get(upstream, {}).get("outputs", {}):
                    raise ValueError(
                        f"app {name!r} input {item['input']!r} needs output {port!r} "
                        f"of app {upstream!r}, which is not deployed"
                    )
        operators, matches, skipped, ports = [], {}, {}, []
        # The selection behind each bound input, so a rebound input can
        # describe the stream it ends up reading.
        selected = {}
        for name, declaration in sorted(declarations.items()):
            app = App.from_dict(declaration)
            items = app.input_items()
            as_of = {role: s.as_of for role, s in items if s.as_of is not None}
            groups, parameters = {}, {}
            matches[name], skipped[name] = {}, {}
            for role, selection in items:
                conversion = (
                    self._conversions(selection.target_unit) if selection.target_unit else None
                )
                matches[name].setdefault(role, 0)
                result = graph.sparql_query(selection.query, wait_for_fresh=True)
                columns = result.get("columns", [])
                if selection.stream not in columns or (
                    selection.group and selection.group not in columns
                ):
                    raise ValueError(
                        "selection must return its declared stream and group columns"
                    )
                excluded = set()
                if selection.exclude_app:
                    excluded = {
                        str(subject)
                        for subject in graph.data_graph(
                            _source_id(selection.exclude_app)
                        ).subjects(ACQUIRIUM_SOURCE_ID, None)
                    }
                for values in result.get("rows", []):
                    row = dict(zip(columns, values))
                    stream = row[selection.stream]
                    if str(stream) in excluded:
                        continue
                    group = row.get(selection.group) if selection.group else name
                    if not stream or not group:
                        raise ValueError("selected stream/group must be bound")
                    context = {
                        str(key): str(value)
                        for key, value in row.items()
                        if value is not None
                    }
                    if selection.parameters is not None:
                        values = selection.parameters.get(str(group))
                        if values is None:
                            skipped[name].setdefault(role, set()).add(str(group))
                            continue
                        merged = parameters.setdefault(str(group), {})
                        for key, value in values.items():
                            if merged.setdefault(key, value) != value:
                                raise ValueError(
                                    f"app {name!r} sets parameter {key!r} of group "
                                    f"{group} to different values"
                                )
                    unit = row.get(selection.unit) if selection.unit else None
                    unit = unit or (
                        row.get(selection.point_unit) if selection.point_unit else None
                    )
                    if conversion is not None:
                        if not unit or self.converter is None:
                            raise ValueError("input unit is required for conversion")
                        context.update(conversion(str(unit)))
                    groups.setdefault(str(group), []).append(
                        {"stream": str(stream), "role": role, "context": context}
                    )
                    selected.setdefault((_operator_id(name, str(group)), role, str(stream)), (selection, conversion))
                    matches[name][role] += 1
            for group, inputs in sorted(groups.items()):
                group_parameters = dict(parameters.get(group, {}))
                group_key = [group]
                if group_parameters.setdefault("group", group_key) != group_key:
                    raise ValueError(
                        f"app {name!r} parameter 'group' is reserved for the group key {group!r}"
                    )
                outputs = {
                    port: output_reference(name, group, port)
                    for port in sorted(app.outputs)
                }
                # Refuse a group missing a parameter the function names.
                _bind_parameters(declaration["function"], group_parameters, name, group)
                operators.append({
                    "id": _operator_id(name, group),
                    "inputs": inputs, "outputs": outputs, "as_of": as_of,
                    "parameters": group_parameters, "annotations": None,
                })
                ports.extend((name, group, port, reference, app.outputs[port])
                             for port, reference in outputs.items())
        if any(map(replacements.claimed_inputs, declarations.values())):
            streams = self._replacement_metadata(operators, declarations, graph)
        else:
            streams = {}
        def rebind(operator, item, target, metadata):
            # Units and conversion factors describe the values the input reads.
            selection, conversion = selected[(operator["id"], item["role"], item["stream"])]
            unit = metadata.get("unit")
            context = dict(item["context"])
            if unit:
                for column in (selection.unit, selection.point_unit):
                    if column and column in context:
                        context[column] = unit
                if conversion is not None:
                    context.update(conversion(unit))
            return dict(item, stream=target, context=context)

        resolved = replacements.compile(
            operators, declarations, streams=streams, previous_plan=self.state.get("plan"),
            compatible=self._compatible_units, rebind=rebind,
        )
        bound = Counter()
        for operator in operators:
            # Bindings have a reproducible order after following replacement links.
            operator["inputs"].sort(key=_json)
            bound.update((app_of(operator["id"]), item["role"]) for item in operator["inputs"])
        # An input that follows replacements counts each identity once,
        # however many of its stages the query matched.
        for (name, role), view in replacements.input_views(declarations).items():
            if view != "stored":
                matches[name][role] = bound[(name, role)]
        metadata = {name: Graph() for name in declarations}
        for name, group, port, reference, spec in ports:
            _output_metadata(metadata[name], name, group, port, reference, spec, resolved.get(reference))
        report = {
            name: {
                "matches": matches[name],
                "skipped": {role: sorted(groups) for role, groups in skipped[name].items()},
            }
            for name in matches
        }
        return operators, metadata, report

    def _compatible_units(self, before, after):
        """Whether a replacement may express its input's values in another unit."""
        if self.converter is None:
            raise ValueError("changing a replacement's unit needs unit conversion support")
        converter = self.converter()
        return converter.check_compatible(converter.resolve_unit(before), converter.resolve_unit(after))

    def _replacement_metadata(self, operators, declarations, graph):
        # Candidate ordinary outputs need no graph lookup; replacement outputs
        # inherit theirs in replacements.compile. Read missing source metadata
        # in one batch, without inspecting unrelated registrations.
        streams, sources = {}, set()
        for operator in operators:
            declaration = declarations[app_of(operator["id"])]
            claimed = replacements.claimed_inputs(declaration)
            for port, ref in operator["outputs"].items():
                if port not in claimed:
                    streams[ref] = declaration["outputs"][port]
            roles = set(claimed.values())
            sources.update(item["stream"] for item in operator["inputs"] if item["role"] in roles)
        missing = sources - streams.keys()
        if not missing:
            return streams
        refs = " ".join(URIRef(ref).n3() for ref in sorted(missing))
        result = graph.sparql_query(f"""
            SELECT ?stream ?value_kind ?unit ?quantity_kind WHERE {{
                VALUES ?stream {{ {refs} }}
                ?stream <{ACQUIRIUM_SOURCE_ID}> ?source .
                OPTIONAL {{ ?stream <{ACQUIRIUM_VALUE_KIND}> ?declared_kind . }}
                OPTIONAL {{ ?stream <{INFERRED_VALUE_KIND}> ?inferred_kind . }}
                BIND(COALESCE(?declared_kind, ?inferred_kind) AS ?value_kind)
                OPTIONAL {{ ?point <{HAS_EXTERNAL_REFERENCE}> ?stream .
                    OPTIONAL {{ ?point <{HAS_UNIT}> ?point_unit . }}
                    OPTIONAL {{ ?point <{HAS_QUANTITY_KIND}> ?quantity_kind . }}
                }}
                OPTIONAL {{ ?stream <{HAS_UNIT}> ?ref_unit . }}
                BIND(COALESCE(?ref_unit, ?point_unit) AS ?unit)
            }}
        """, wait_for_fresh=True)
        for row in result.get("rows", []):
            values = dict(zip(result["columns"], row))
            ref = str(values.pop("stream"))
            spec = {key: str(value) if value is not None else None for key, value in values.items()}
            spec["value_kind"] = normalize_value_kind(spec.get("value_kind"))
            if ref in streams and streams[ref] != spec:
                raise ValueError(f"replacement input {ref!r} has conflicting metadata")
            streams[ref] = spec
        return streams

    def _conversions(self, target_unit):
        """The conversion context an input in each unit carries to
        ``target_unit``. Each unit resolves once, on first use, however many
        streams a selection matches."""

        @functools.cache
        def target():
            return self.converter().resolve_unit(target_unit)

        @functools.cache
        def context(unit):
            converter = self.converter()
            source = converter.resolve_unit(unit)
            if not converter.check_compatible(source, target()):
                raise ValueError("incompatible input/output units")
            return dict(
                from_multiplier=str(source.multiplier),
                from_offset=str(source.offset),
                to_multiplier=str(target().multiplier),
                to_offset=str(target().offset),
            )

        return context

    def _function(self, declaration, previous):
        function = declaration["function"]
        kind = function["kind"]
        ports = _ports(declaration)
        batch = function.get("batch", False)
        if batch and function.get("sequential"):
            raise ValueError("a Python function can be batch or sequential")
        replaced = sorted(replacements.claimed_inputs(declaration))
        window = declaration["window"]
        if previous is not None:
            old = previous["annotations"]["app"]
            identity = lambda spec: {key: value for key, value in spec.items() if key != "path"}
            if (identity(old["function"]) == identity(function)
                    and all(old[field] == declaration[field] for field in ("outputs", "window"))):
                return previous["definition"]["function"], []

        artifacts = []
        if kind == "python":
            source = self._source(function)
            artifacts.append(("", source))
            adapter = "_epoch_batch_function" if batch else "_epoch_function"
            code = (
                f"from acquirium.functions import {adapter} as __acquirium_adapt\n"
                f"__file__ = {function['path']!r}\n"
                f"exec(compile({source!r}, __file__, 'exec'), globals())\n"
                f"__acquirium_calculate = __acquirium_adapt(globals()[{function['entrypoint']!r}], "
                f"{window!r}, {ports!r}, {replaced!r})\n"
            )
        else:
            if kind not in WINDOW_BUILTINS | {"integral", "cumulative_sum"}:
                raise ValueError(f"unknown calculation {kind!r}")
            if function.get("port") not in declaration["outputs"]:
                raise ValueError("the built-in calculation must name a declared output port")
            if kind == "integral" and (
                window["kind"] != "pointwise" or type(function.get("per_ns")) is not int
                or function["per_ns"] <= 0
            ):
                raise ValueError("integral requires a pointwise window and a positive per_ns")
            batch = kind in WINDOW_BUILTINS
            adapter = "_builtin_batch_function" if batch else "_builtin_function"
            code = (
                f"from acquirium.functions import {adapter}\n"
                f"__acquirium_calculate = {adapter}({function!r}, {ports!r})\n"
            )
        code = code.encode()
        digest = sha256(code).hexdigest()
        artifacts.append(("aq_app_", code))
        return {
            "module": f"aq_app_{digest}", "name": "__acquirium_calculate",
            "digest": digest, "mode": "vectorized" if batch else "per_window",
        }, artifacts

    def _candidate(self, declarations):
        plan = {"templates": [], "instances": [], "bindings": []}
        if not declarations:
            return plan, [], self.graph.graph_status()["source_version"]
        desired = {_source_id(name) for name in declarations}
        sources = self.graph.app_sources() | desired
        exclusions = {
            _source_id(item["exclude_app"])
            for declaration in declarations.values()
            for item in declaration["inputs"]
            if item.get("exclude_app")
        }
        with self.graph.selection_snapshot(sources | exclusions) as view:
            view.stage({source: Graph() for source in sources - desired})
            for _ in range(8):
                operators, metadata, report = self._compile(declarations, graph=view)
                if not view.stage({_source_id(name): graph for name, graph in metadata.items()}):
                    break
            else:
                raise ValueError("derived semantic selections did not stabilize")

        old = {row["id"]: row for row in (self.state["plan"] or {}).get("templates", [])}
        modules = []
        for name, declaration in sorted(declarations.items()):
            function, artifacts = self._function(declaration, old.get(name))
            modules.extend(artifacts)
            window, roles = _window_and_roles(declaration)
            ports = _ports(declaration)
            spec = declaration["function"]
            if spec.get("sequential") or spec["kind"] in {"integral", "cumulative_sum"}:
                previous_role = "__acquirium_previous"
                while previous_role in roles:
                    previous_role += "_"
                roles[previous_role] = {
                    "reach": {"latest": {"max_age": None}}, "driving": False,
                    "reads": {"own_ports": ports},
                }
            plan["templates"].append({
                "id": name,
                "definition": {
                    "function": function, "window": window, "roles": roles,
                    "ports": ports, "freshness": "synchronous",
                },
                "annotations": {
                    "app": declaration,
                    "graph": sorted(metadata[name].serialize(format="nt").splitlines()),
                    "report": report[name],
                },
            })
        for operator in operators:
            name = replacements.app_of(operator["id"])
            key = operator["parameters"]["group"][0]
            outputs = operator["outputs"]
            if declarations[name]["function"]["kind"] == "integral":
                outputs = {**outputs, _RATE_PORT: output_reference(name, key, _RATE_PORT)}
            plan["instances"].append({
                "template": name, "key": key, "parameters": operator["parameters"],
                "outputs": outputs, "annotations": operator["annotations"],
            })
            for item in operator["inputs"]:
                plan["bindings"].append({
                    "template": name, "instance": key, "role": item["role"], "stream": item["stream"],
                    "valid_from": item.get("valid_from"), "valid_to": item.get("valid_to"),
                    "context": _json(item["context"]),
                })
        plan["bindings"] = sorted(
            plan["bindings"], key=lambda row: (
                row["template"], row["instance"], row["role"], row["stream"],
                row["valid_from"] is not None, row["valid_from"],
                row["valid_to"] is not None, row["valid_to"], row["context"],
            )
        )
        return plan, modules, view.revision

    def _adopt(self, plan, revision=None, *, epoch=None):
        declarations, sources, report = {}, {}, {}
        for template in plan["templates"]:
            name, annotation = template["id"], template["annotations"]
            if not annotation or "app" not in annotation:
                raise ValueError(f"template {name!r} has no Acquirium declaration")
            declarations[name] = annotation["app"]
            sources[_source_id(name)] = Graph().parse(data="\n".join(annotation["graph"]), format="nt")
            report[name] = annotation["report"]
        before, after = self.graph.project_app_sources(sources)
        self.state = {"apps": declarations, "plan": plan}
        if epoch is not None:
            self._published = (plan, epoch, False)
        self.report = report
        self._revision = after if revision == before else -1
        self.error = None

    def _settle(self, *, wait=True):
        """Adopt the plan of the activation under way once it publishes;
        without ``wait``, only if it already has. Changes to the app set
        wait, since they compile against the adopted plan; reads do not."""
        if self._pending is None:
            return
        receipt, revision = self._pending
        if wait:
            self.core.wait(receipt)
        (epoch,) = self.core.engine.epochs_of([receipt])
        if epoch is None:
            return
        self._adopt(self.core.engine.snapshot().plan(), revision, epoch=epoch)
        self._pending = None

    def recover(self):
        """Adopt the published plan at startup, without waiting for requests
        accepted before the restart. While those are pending, the plan they
        may publish is adopted, and the graph recompiled against it, by the
        next change or snapshot read, which waits for them."""
        with self.lock:
            status = self.core.engine.status()
            snapshot = self.core.engine.snapshot()
            self._adopt(snapshot.plan(), epoch=snapshot.epoch)
            if status["pending"]:
                self._pending = Receipt(status["accepted"]), None
                self._published = (*self._published[:2], True)
            else:
                self.refresh()

    def _install(self, declarations):
        plan, modules, revision = self._candidate(declarations)
        if plan != self.state["plan"]:
            for prefix, code in modules:
                self._artifact(code, prefix=prefix)
            self._published = (*self._published[:2], True)
            try:
                self._pending = self.core.activate(uuid4().hex, plan), revision
            except BaseException:
                self._published = (*self._published[:2], False)
                raise
            self._settle()
        else:
            self._adopt(plan, revision)

    def plan_at(self, epoch=None):
        """The plan published at ``epoch``, by default the latest. The adopted
        plan covers each epoch from its publication while no activation is
        under way; any other epoch loads its plan from the engine."""
        plan, since, activating = self._published
        if plan is not None and not activating and (
            epoch is None or since <= epoch <= self.core.engine.status()["published"]
        ):
            return plan
        return self.core.plan_at(epoch)

    def refresh(self):
        with self.lock:
            self._settle()
            if self._revision == self.graph.graph_status()["source_version"]:
                return
            try:
                self._install(dict(self.state["apps"]))
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"
                raise

    def catch_up(self, timeout: float = 30.0) -> bool:
        """Wait until the published plan reflects the graph as of this call,
        and return whether it does.

        A graph write lands before the plan compiled from it publishes, so a
        reader that resolves streams in the graph and then reads the latest
        epoch can find outputs the plan does not hold yet. An install under
        way finishes first; a write that nothing has installed yet is
        installed here. Gives up after waiting ``timeout`` seconds for an
        install, or when installing fails, whose error ``inspect`` reports.
        """
        revision = self.graph.graph_status()["source_version"]
        if self._revision >= revision or not self.state["apps"]:
            return True
        if not self.lock.acquire(timeout=timeout):
            return False
        try:
            if self._revision < revision:
                self.refresh()
        except Exception:
            return False
        finally:
            self.lock.release()
        return self._revision >= revision

    def deploy(self, app):
        with self.lock:
            self._settle()
            definition = _definition(app)
            declarations = dict(self.state["apps"])
            declarations[definition["name"]] = definition
            self._install(declarations)
            return self.inspect(definition["name"])

    def remove(self, name):
        with self.lock:
            self._settle()
            declarations = dict(self.state["apps"])
            del declarations[name]
            self._install(declarations)
            return {"name": name, "status": "removed", "active_plan": _plan_id(self.state["plan"])}

    def preview(self, app):
        with self.lock:
            definition = _definition(app)
            name = definition["name"]
            plan, _, _ = self._candidate({**self.state["apps"], name: definition})
            preview = self.core.engine.preview(plan)
            report = next(t["annotations"]["report"] for t in plan["templates"] if t["id"] == name)
            return {
                "name": name, "dry_run": True, "plan": _plan_id(plan),
                "active_plan": _plan_id(self.state["plan"]), "epoch": preview["base"],
                "matches": report["matches"], "warnings": _warnings(report),
                "within_limits": all(item["within_limits"] for item in preview["instances"].values()),
                "instances": [
                    {"template": template, "key": key, **counts}
                    for (template, key), counts in preview["instances"].items()
                ],
            }

    def inspect(self, name):
        with self.lock:
            self._settle(wait=False)
            definition = self.state["apps"][name]
            # The adopted plan; while an activation is pending, the one it replaces.
            snapshot, plan = self.core.engine.snapshot(), self.state["plan"]
            instances = [row for row in plan["instances"] if row["template"] == name]
            streams = [stream for row in instances for stream in row["outputs"].values()]
            failures = {
                _operator_id(*stream["instance"]): stream["behind"]
                for stream in self.core.failures(snapshot, streams)
            }
            report = self.report[name]
            template = next(row for row in plan["templates"] if row["id"] == name)
            spec = template["definition"]
            inputs = {}
            for binding in plan["bindings"]:
                if binding["template"] == name:
                    key = binding["instance"]
                    inputs.setdefault(key, []).append({
                        "stream": binding["stream"], "role": binding["role"],
                        "valid_from": binding["valid_from"], "valid_to": binding["valid_to"],
                        "context": json.loads(binding["context"]),
                    })
            operators = []
            for instance in instances:
                group = instance["key"]
                outputs = {key: value for key, value in instance["outputs"].items() if key in definition["outputs"]}
                operator = {
                    "id": _operator_id(name, group), "inputs": inputs.get(group, []),
                    "outputs": outputs, "parameters": instance["parameters"],
                    "function": _bind_parameters(definition["function"], instance["parameters"], name, group),
                    "window": definition["window"],
                }
                as_of = {role: {"max_age_ns": row["reach"]["latest"]["max_age"]}
                         for role, row in spec["roles"].items() if row["reads"] == "bound" and "latest" in row["reach"]}
                if as_of:
                    operator["as_of"] = as_of
                operator["version"] = _digest([spec, instance["parameters"], outputs, instance["annotations"]])
                operators.append(operator)
            errors = [item["message"] for item in failures.values() if "message" in item]
            status = ("failed" if errors else "pending" if self._pending is not None
                      else "blocked" if failures else "active")
            return {
                "name": name, "definition": definition, "operators": operators, "status": status,
                "active_plan": _plan_id(plan),
                "error": self.error or "\n".join(dict.fromkeys(errors)) or None,
                "stale": bool(failures), "failures": failures,
                "matches": report["matches"], "warnings": _warnings(report),
            }
    def list(self):
        return [self.inspect(name) for name in sorted(self.state["apps"])]
