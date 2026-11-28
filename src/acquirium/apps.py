"""Semantic selections compiled into complete Escapement plans.

Acquirium owns declarations and RDF discovery. Escapement owns validation,
execution, history, publication, and recovery of accepted computation.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import timedelta
import functools
from hashlib import sha256
import json
import logging
import os
import re
from uuid import uuid4
from pathlib import Path
from threading import RLock
from typing import Any, Mapping, Sequence
from typing import Literal as TypeLiteral

from acquirium.app_spec import FixedBucket, Pointwise, PythonFunction, Rolling, duration, duration_ns
from escapement import Receipt
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium import replacements
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
    # A maximum age, such as pd.Timedelta("2h"): read this input as of each
    # window, its latest reading at or before the window's timestamp and no
    # older, instead of the readings in the window. Such an input creates no windows.
    as_of: timedelta | None = None
    # Which replacement stage each selected identity binds: "current" (the
    # chain's final stage), "raw" (its original input), or "stored" (exactly
    # the stream selected). Unset means "current", or "stored" for outputs_of.
    view: str | None = None

    @classmethod
    def from_stream(cls, stream, *, as_of=None, view=None):
        """Bind a stream handle or URI to the app's shared input group."""
        uri = str(stream)
        if not looks_like_uri(uri):
            raise ValueError("stream must be an Acquirium stream handle or URI")
        return cls(
            f"SELECT ?stream WHERE {{ BIND({URIRef(uri).n3()} AS ?stream) }}",
            group=None, as_of=as_of, view=view,
        )

    @classmethod
    def outputs_of(cls, app, port, *, combine=False, parameters=None, as_of=None):
        """Select one output port of another app.

        By default each upstream group feeds its own operator, under the same
        group. ``combine=True`` feeds every upstream group into one operator.
        ``parameters`` maps groups to values the function reads from
        ``context["parameters"]``. ``as_of`` reads the port as of each window,
        no older than that duration, as for any selection.
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
    if "as_of" in data:
        data["as_of"] = duration_ns(data["as_of"], "as_of")
    return data


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
    function: PythonFunction | Mapping[str, Any]
    window: Pointwise | FixedBucket | Rolling = field(default_factory=Pointwise)
    freshness: TypeLiteral["synchronous", "asynchronous"] = "synchronous"

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
        if self.freshness not in ("synchronous", "asynchronous"):
            raise ValueError("app freshness must be 'synchronous' or 'asynchronous'")
        function = self.function.to_dict() if isinstance(self.function, PythonFunction) else dict(self.function)
        if function.get("kind") != "python":
            raise ValueError(f"app {self.name!r} requires a PythonFunction. Redeploy built-in apps before upgrading.")
        if function.get("batch") and function.get("sequential"):
            raise ValueError("a Python function can be batch or sequential")
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
            if selection.as_of is not None and duration_ns(selection.as_of, "as_of") <= 0:
                raise ValueError("as_of must be a positive duration")
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
        window = self.window.to_dict()
        sizes = [value for key, value in window.items() if key.endswith("_ns")]
        sizes.extend(duration_ns(selection.as_of, "as_of") for _, selection in items
                     if selection.as_of is not None)
        if any(value % 1000 for value in sizes):
            raise ValueError("Python soft sensor windows and as-of ages must have microsecond precision")
        data = dict(
            name=self.name,
            inputs=[{"input": name, **_selection_dict(selection)} for name, selection in items],
            outputs=outputs,
            function=function,
            window=window,
        )
        # Keep existing synchronous declarations and plans in their stored form.
        if self.freshness != "synchronous":
            data["freshness"] = self.freshness
        return data

    @classmethod
    def from_dict(cls, data):
        required = {"name", "inputs", "outputs", "function", "window"}
        if not required <= set(data) <= (required | {"freshness"}):
            raise ValueError(f"app fields must be {sorted(required)}, with optional 'freshness'")
        window = data["window"]
        if window["kind"] == "pointwise":
            value = Pointwise()
        elif window["kind"] == "fixed":
            value = FixedBucket(duration(window["width_ns"]), window.get("origin_ns", 0))
        elif window["kind"] == "rolling":
            value = Rolling(
                duration(window["lookback_ns"]),
                duration(window["step_ns"]),
                duration(window.get("lookahead_ns", 0)),
                window.get("origin_ns", 0),
            )
        else:
            raise ValueError("only pointwise, fixed-bucket and rolling windows are supported")
        inputs = {}
        for item in data["inputs"]:
            item = dict(item)
            name = item.pop("input")
            if item.get("as_of") is not None:
                item["as_of"] = duration(item["as_of"])
            inputs.setdefault(name, []).append(Selection(**item))
        app = cls(
            data["name"],
            {name: items[0] if len(items) == 1 else items for name, items in inputs.items()},
            {key: _output_from_dict(item) for key, item in data["outputs"].items()},
            data["function"],
            value,
            freshness=data.get("freshness", "synchronous"),
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


def output_value_kind(plan, stream: str) -> str | None:
    """Read an output's declared kind from its plan. Replacements inherit theirs."""
    for instance in plan["instances"]:
        for port, output in instance["outputs"].items():
            if output == stream:
                template = next(item for item in plan["templates"] if item["id"] == instance["template"])
                declaration = (template["annotations"] or {}).get("app", {})
                return declaration.get("outputs", {}).get(port, {}).get("value_kind")
    return None


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

    def _compile(self, declarations, apps, graph):
        """Resolve declared inputs against the graph and build concrete instances and bindings."""
        for name, declaration in declarations.items():
            for item in declaration["inputs"]:
                upstream, port = item.get("upstream"), item.get("upstream_port")
                if upstream and port not in declarations.get(upstream, {}).get("outputs", {}):
                    raise ValueError(
                        f"app {name!r} input {item['input']!r} needs output {port!r} "
                        f"of app {upstream!r}, which is not deployed"
                    )
        plan = {"templates": [], "instances": [], "bindings": []}
        bindings, matches, skipped = {}, {}, {}
        # The selection behind each bound input, so a rebound input can
        # describe the stream it ends up reading.
        selected = {}
        for name, declaration in sorted(declarations.items()):
            app = apps[name]
            groups, parameters = {}, {}
            matches[name], skipped[name] = {}, {}
            for role, selection in app.input_items():
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
                    group, stream = str(group), str(stream)
                    context = {
                        str(key): str(value)
                        for key, value in row.items()
                        if value is not None
                    }
                    if selection.parameters is not None:
                        values = selection.parameters.get(group)
                        if values is None:
                            skipped[name].setdefault(role, set()).add(group)
                            continue
                        merged = parameters.setdefault(group, {})
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
                    groups.setdefault(group, []).append({
                        "template": name, "instance": group, "role": role, "stream": stream,
                        "valid_from": None, "valid_to": None, "context": _json(context),
                    })
                    selected.setdefault((name, group, role, stream), (selection, conversion))
                    matches[name][role] += 1
            for group, rows in sorted(groups.items()):
                bindings[name, group] = rows
                group_parameters = dict(parameters.get(group, {}))
                group_key = [group]
                if group_parameters.setdefault("group", group_key) != group_key:
                    raise ValueError(
                        f"app {name!r} parameter 'group' is reserved for the group key {group!r}"
                    )
                plan["instances"].append({
                    "template": name, "key": group,
                    "outputs": {port: output_reference(name, group, port) for port in sorted(app.outputs)},
                    "parameters": group_parameters, "annotations": None,
                })

        streams = {}
        if any(map(replacements.claimed_inputs, declarations.values())):
            streams = self._replacement_metadata(plan["instances"], bindings, declarations, graph)
        views = replacements.input_views(declarations)

        def rebind(binding, target, metadata):
            # Units and conversion factors describe the values the input reads.
            unit = metadata.get("unit")
            if not unit:
                return dict(binding, stream=target)
            identity = (binding["template"], binding["instance"], binding["role"], binding["stream"])
            selection, conversion = selected[identity]
            context = json.loads(binding["context"])
            for column in (selection.unit, selection.point_unit):
                if column and column in context:
                    context[column] = unit
            if conversion is not None:
                context.update(conversion(unit))
            return dict(binding, stream=target, context=_json(context))

        resolved = replacements.compile(
            plan, bindings, declarations, views, streams=streams, previous_plan=self.state["plan"],
            compatible=self._compatible_units, rebind=rebind,
        )
        # Semantic selections are unbounded, so all bindings share the same span.
        plan["bindings"] = sorted(
            (row for rows in bindings.values() for row in rows),
            key=lambda row: (row["template"], row["instance"], row["role"], row["stream"], row["context"]),
        )
        bound = Counter((row["template"], row["role"]) for row in plan["bindings"])
        # An input that follows replacements counts each identity once,
        # however many of its stages the query matched.
        for (name, role), view in views.items():
            if view != "stored":
                matches[name][role] = bound[(name, role)]
        metadata = {name: Graph() for name in declarations}
        for instance in plan["instances"]:
            name, group = instance["template"], instance["key"]
            for port, spec in apps[name].outputs.items():
                ref = instance["outputs"][port]
                _output_metadata(metadata[name], name, group, port, ref, spec, resolved.get(ref))
        report = {
            name: {
                "matches": matches[name],
                "skipped": {role: sorted(groups) for role, groups in skipped[name].items()},
            }
            for name in matches
        }
        return plan, metadata, report

    def _compatible_units(self, before, after):
        """Whether a replacement may express its input's values in another unit."""
        if self.converter is None:
            raise ValueError("changing a replacement's unit needs unit conversion support")
        converter = self.converter()
        return converter.check_compatible(converter.resolve_unit(before), converter.resolve_unit(after))

    def _replacement_metadata(self, instances, bindings, declarations, graph):
        # Candidate ordinary outputs need no graph lookup; replacement outputs
        # inherit theirs in replacements.compile. Read missing source metadata
        # in one batch, without inspecting unrelated registrations.
        streams, sources = {}, set()
        for instance in instances:
            name, group = instance["template"], instance["key"]
            declaration = declarations[name]
            claimed = replacements.claimed_inputs(declaration)
            for port, spec in declaration["outputs"].items():
                if port not in claimed:
                    streams[instance["outputs"][port]] = spec
            roles = set(claimed.values())
            sources.update(row["stream"] for row in bindings[name, group] if row["role"] in roles)
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
        ports = sorted(declaration["outputs"])
        batch = function.get("batch", False)
        replaced = sorted(replacements.claimed_inputs(declaration))
        window = declaration["window"]
        if previous is not None:
            old = previous["annotations"]["app"]
            identity = lambda spec: {key: value for key, value in spec.items() if key != "path"}
            if (identity(old["function"]) == identity(function)
                    and all(old[field] == declaration[field] for field in ("outputs", "window"))):
                return previous["definition"]["function"], []

        source = self._source(function)
        adapter = "_epoch_batch_function" if batch else "_epoch_function"
        code = (
            f"from acquirium.functions import {adapter} as __acquirium_adapt\n"
            f"__file__ = {function['path']!r}\n"
            f"exec(compile({source!r}, __file__, 'exec'), globals())\n"
            f"__acquirium_calculate = __acquirium_adapt(globals()[{function['entrypoint']!r}], "
            f"{window!r}, {ports!r}, {replaced!r})\n"
        ).encode()
        digest = sha256(code).hexdigest()
        return {
            "module": f"aq_app_{digest}", "name": "__acquirium_calculate",
            "digest": digest, "mode": "vectorized" if batch else "per_window",
        }, [("", source), ("aq_app_", code)]

    def _candidate(self, declarations):
        """Build a candidate plan and the module artifacts needed to activate it."""
        if not declarations:
            return {"templates": [], "instances": [], "bindings": []}, [], self.graph.graph_status()["source_version"]
        apps = {name: App.from_dict(declaration) for name, declaration in declarations.items()}
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
            # Output metadata can change other apps' input selections. Repeat
            # against the staged graph until those selections settle.
            for _ in range(8):
                plan, metadata, report = self._compile(declarations, apps, view)
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
            ports = sorted(declaration["outputs"])
            if declaration["function"].get("sequential"):
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
                    "ports": ports, "freshness": apps[name].freshness,
                },
                "annotations": {
                    "app": declaration,
                    "graph": sorted(metadata[name].serialize(format="nt").splitlines()),
                    "report": report[name],
                },
            })
        return plan, modules, view.revision

    def _adopt(self, plan, revision=None, *, epoch=None):
        """Project a published plan's declarations and metadata back into Acquirium."""
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

    def _settle(self, *, snapshot=None):
        """Adopt the pending plan once it publishes. A reader supplies its
        snapshot so adoption and inspection use the same publication cut."""
        if self._pending is None:
            return
        receipt, revision = self._pending
        if snapshot is None:
            snapshot = self.core.wait(receipt)
        (epoch,) = self.core.engine.epochs_of([receipt])
        if epoch is None or epoch > snapshot.epoch:
            return
        self._adopt(snapshot.plan(), revision, epoch=epoch)
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
                self.reconcile()

    def _install(self, declarations):
        """Activate a changed plan and adopt it after the engine publishes it."""
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
        """Recompile the adopted declarations when the source graph has changed."""
        with self.lock:
            self._settle()
            if self._revision == self.graph.graph_status()["source_version"]:
                return
            try:
                self._install(dict(self.state["apps"]))
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"
                raise

    def reconcile(self) -> dict[str, str]:
        """Refresh after a committed graph change or startup and report any error.

        The published plan remains the recovery point when compilation fails.
        """
        with self.lock:
            try:
                self.refresh()
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"
                logging.getLogger(__name__).warning("App reconciliation failed: %s", self.error)
                return {"app_reconciliation_error": self.error}
        return {}

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
            template = next(t for t in plan["templates"] if t["id"] == name)
            report = template["annotations"]["report"]
            return {
                "name": name, "dry_run": True, "plan": _plan_id(plan),
                "active_plan": _plan_id(self.state["plan"]), "epoch": preview["base"],
                "freshness": template["definition"]["freshness"],
                "matches": report["matches"], "warnings": _warnings(report),
                "within_limits": all(item["within_limits"] for item in preview["instances"].values()),
                "instances": [
                    {"template": template, "key": key, **counts}
                    for (template, key), counts in preview["instances"].items()
                ],
            }

    def inspect(self, name):
        return self._inspect([name])[0]

    def list(self):
        return self._inspect()

    def _inspect(self, names=None):
        with self.lock:
            snapshot = self.core.engine.snapshot()
            self._settle(snapshot=snapshot)
            declarations, plan = self.state["apps"], self.state["plan"]
            report, pending, error = self.report, self._pending is not None, self.error
        names = sorted(declarations) if names is None else names
        if not names:
            return []
        for name in names:
            if name not in declarations:
                raise KeyError(name)

        selected = set(names)
        specs = {row["id"]: row["definition"] for row in plan["templates"] if row["id"] in selected}
        instances = {}
        streams = []
        for row in plan["instances"]:
            name = row["template"]
            if name in selected:
                instances.setdefault(name, []).append(row)
                streams.extend(row["outputs"].values())
        inputs = {}
        for binding in plan["bindings"]:
            name = binding["template"]
            if name in selected:
                key = (name, binding["instance"])
                inputs.setdefault(key, []).append({
                    "stream": binding["stream"], "role": binding["role"],
                    "valid_from": binding["valid_from"], "valid_to": binding["valid_to"],
                    "context": json.loads(binding["context"]),
                })
        failures_by_app = {}
        failed_streams = self.core.failures(snapshot, streams) if streams else []
        for stream in failed_streams:
            name, group = stream["instance"]
            failures_by_app.setdefault(name, {})[_operator_id(name, group)] = stream["behind"]

        active_plan = _plan_id(plan)
        result = []
        for name in names:
            definition, spec = declarations[name], specs[name]
            failures = failures_by_app.get(name, {})
            as_of = {role: {"max_age_ns": row["reach"]["latest"]["max_age"]}
                     for role, row in spec["roles"].items() if row["reads"] == "bound" and "latest" in row["reach"]}
            operators = []
            for instance in instances.get(name, []):
                group = instance["key"]
                outputs = {key: value for key, value in instance["outputs"].items() if key in definition["outputs"]}
                operator = {
                    "id": _operator_id(name, group), "inputs": inputs.get((name, group), []),
                    "outputs": outputs, "parameters": instance["parameters"],
                    "function": definition["function"],
                    "window": definition["window"],
                }
                if as_of:
                    operator["as_of"] = as_of
                operator["version"] = _digest([spec, instance["parameters"], outputs, instance["annotations"]])
                operators.append(operator)
            errors = [item["message"] for item in failures.values() if "message" in item]
            if errors:
                status = "failed"
            elif pending:
                status = "pending"
            elif failures:
                status = (
                    "refreshing" if all(item["reason"] == "refreshing" for item in failures.values())
                    else "blocked"
                )
            else:
                status = "active"
            result.append({
                "name": name, "definition": definition, "operators": operators, "status": status,
                "active_plan": active_plan, "freshness": spec["freshness"],
                "error": error or "\n".join(dict.fromkeys(errors)) or None,
                "stale": bool(failures), "failures": failures,
                "matches": report[name]["matches"], "warnings": _warnings(report[name]),
            })
        return result
