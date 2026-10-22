"""Semantic selections compiled into complete Escapement plans.

Acquirium owns declarations and RDF discovery. Escapement owns validation,
execution, history, publication, and recovery of accepted computation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
import os
import re
from uuid import uuid4
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from escapement import (
    FixedBucket,
    Input,
    Native,
    Operator,
    Pointwise,
    PythonFunction,
    PendingError,
    Rolling,
    ExecutionError,
    ValidationError,
)
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import XSD

from acquirium.internals.models import compute_ref_uri, looks_like_uri
from acquirium.Storage.values import normalize_value_kind
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_VALUE_KIND,
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
    """SPARQL selection with explicit stream/group columns and a function role."""

    query: str
    role: str = "input"
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

    @classmethod
    def outputs_of(cls, app, port, *, role, combine=False, parameters=None):
        """Select one output port of another app.

        By default each upstream group feeds its own operator, under the same
        group. ``combine=True`` feeds every upstream group into one operator.
        ``parameters`` maps groups to values the function reads from
        ``context["parameters"]``.
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
            role=role,
            group=None if combine else "group",
            upstream=app.name,
            upstream_port=port,
            parameters=parameters,
        )

    @classmethod
    def from_query(
        cls,
        query,
        *,
        alias: str,
        group: str | None = None,
        target_unit: str | None = None,
        exclude_app: str | None = None,
    ):
        """Freeze the query DSL as SPARQL; resolve its inputs again on graph edits."""
        node = query.query_graph.resolve_alias(alias)
        if node is None or node not in query.query_graph.data_nodes:
            raise ValueError(f"{alias!r} must name a measurement alias")
        group_node = query.query_graph.resolve_alias(group) if group else None
        if group and group_node is None:
            raise ValueError(f"unknown group alias {group!r}")
        return cls(
            query.to_sparql(),
            role=alias,
            stream=f"ext{node}",
            group=f"v{group_node}" if group_node is not None else f"ext{node}",
            unit=f"extunit{node}",
            point_unit=f"unit{node}",
            target_unit=target_unit,
            exclude_app=exclude_app,
        )


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
class App:
    name: str
    inputs: tuple[Selection, ...] | list[Selection]
    outputs: Mapping[str, Output]
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
        if any(not key for key in self.outputs):
            raise ValueError("output port names must be nonempty")
        if any(type(spec.on_group) is not bool for spec in self.outputs.values()):
            raise ValueError("output on_group must be a boolean")
        for spec in self.outputs.values():
            if spec.value_kind != normalize_value_kind(spec.value_kind):
                raise ValueError("output value_kind must be 'numeric' or 'text'")
        for selection in self.inputs:
            for values in (selection.parameters or {}).values():
                if not isinstance(values, Mapping) or any(
                    not isinstance(key, str) or not key for key in values
                ):
                    raise ValueError("parameters map groups to named values")
                try:
                    _json(dict(values))
                except (TypeError, ValueError) as error:
                    raise ValueError("parameter values must be JSON") from error
        return dict(
            name=self.name,
            inputs=[asdict(item) for item in self.inputs],
            outputs={key: asdict(value) for key, value in self.outputs.items()},
            function=_function_dict(self.function),
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
        app = cls(
            data["name"],
            tuple(Selection(**item) for item in data["inputs"]),
            {key: Output(**item) for key, item in data["outputs"].items()},
            data["function"],
            value,
        )
        app.to_dict()
        return app


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
    return str(compute_ref_uri(f"derived:{app}", _json([group, port])))


class Apps:
    def __init__(self, core, graph, directory, *, converter=None):
        self.core, self.graph, self.converter = core, graph, converter
        self.lock = RLock()
        self.path = Path(directory) / "apps.json"
        self._revision = -1
        self.error = None
        # Streams matched and groups skipped per app and input role by the
        # installed compilation.
        self.report = {}
        self.state = (
            json.loads(self.path.read_text())
            if self.path.exists()
            else {"apps": {}, "plan": None}
        )

    def _save(self, state):
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as handle:
            handle.write(_json(state))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def recover(self):
        with self.lock:
            plan = self.state.get("plan")
            active = self.core.engine.snapshot().plan
            if not plan and active and active["operators"]:
                # Deploying nothing would retract every derived output.
                raise RuntimeError(
                    f"the engine store runs plan {active['id']} with "
                    f"{len(active['operators'])} operators, but {self.path} records no "
                    "apps. Restore apps.json, or start from an empty engine store."
                )
            if plan:
                # Reusing the exact declaration also works after retry-ledger expiry.
                if not active or active["id"] != plan["id"]:
                    try:
                        self.core.wait(
                            self.core.activate(self.state["activation_id"], plan)
                        )
                    except ValidationError:
                        previous = self.state.get("previous")
                        if previous is None:
                            raise
                        self.state = previous
                        self._save(previous)
                    except (ExecutionError, PendingError):
                        self.error = None
                        return
            self.refresh()

    def _compile(self, declarations):
        for name, declaration in declarations.items():
            for item in declaration["inputs"]:
                upstream, port = item.get("upstream"), item.get("upstream_port")
                if upstream and port not in declarations.get(upstream, {}).get("outputs", {}):
                    raise ValueError(
                        f"app {name!r} input {item['role']!r} needs output {port!r} "
                        f"of app {upstream!r}, which is not deployed"
                    )
        operators, metadata, matches, skipped = [], {}, {}, {}
        for name, declaration in sorted(declarations.items()):
            app = App.from_dict(declaration)
            groups, parameters = {}, {}
            matches[name], skipped[name] = {}, {}
            for selection in app.inputs:
                if not selection.role:
                    raise ValueError("input roles must be nonempty")
                matches[name].setdefault(selection.role, 0)
                result = self.graph.sparql_query(selection.query, wait_for_fresh=True)
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
                        for subject in self.graph.data_graph(
                            f"derived:{selection.exclude_app}"
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
                            skipped[name].setdefault(selection.role, set()).add(str(group))
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
                    if selection.target_unit:
                        if not unit or self.converter is None:
                            raise ValueError("input unit is required for conversion")
                        converter = self.converter()
                        source, target = (
                            converter.resolve_unit(str(unit)),
                            converter.resolve_unit(selection.target_unit),
                        )
                        if not converter._check_compatible(source, target):
                            raise ValueError("incompatible input/output units")
                        context.update(
                            from_multiplier=str(source.multiplier),
                            from_offset=str(source.offset),
                            to_multiplier=str(target.multiplier),
                            to_offset=str(target.offset),
                        )
                    groups.setdefault(str(group), []).append(
                        Input(str(stream), selection.role, context=context)
                    )
                    matches[name][selection.role] += 1
            rdf = Graph()
            for group, inputs in sorted(groups.items()):
                outputs = {
                    port: output_reference(name, group, port)
                    for port in sorted(app.outputs)
                }
                function = _bind_parameters(
                    declaration["function"], parameters.get(group, {}), name, group
                )
                definition = dict(
                    function=function,
                    window=declaration["window"],
                    outputs=outputs,
                )
                if group in parameters:
                    definition["parameters"] = parameters[group]
                operator = Operator(
                    id=f"{name}:{_digest(group)}",
                    version=_digest(definition),
                    inputs=sorted(inputs, key=lambda item: _json(item.to_dict())),
                    outputs=outputs,
                    window=app.window,
                    function=function,
                    parameters=parameters.get(group, {}),
                )
                operators.append(operator.to_dict())
                for port, reference in outputs.items():
                    spec = app.outputs[port]
                    ref, point = URIRef(reference), URIRef(f"{reference}__point")
                    rdf.add((ref, ACQUIRIUM_SOURCE_ID, Literal(f"derived:{name}")))
                    rdf.add((ref, ACQUIRIUM_REF_NAME, Literal(_json([group, port]))))
                    rdf.add((ref, ACQUIRIUM_VALUE_KIND, Literal(spec.value_kind)))
                    rdf.add((point, RDF.type, VIRTUAL_POINT))
                    rdf.add((point, HAS_EXTERNAL_REFERENCE, ref))
                    rdf.add((point, PRODUCED_BY, Literal(name)))
                    rdf.add((point, OUTPUT_PORT, Literal(port)))
                    rdf.add(
                        (
                            point,
                            OUTPUT_GROUP,
                            URIRef(group) if looks_like_uri(group) else Literal(group),
                        )
                    )
                    if spec.on_group:
                        if not looks_like_uri(group):
                            raise ValueError("output on_group requires a group URI")
                        rdf.add((URIRef(group), S223.hasProperty, point))
                    rdf.add(
                        (point, RDFS.label, Literal(spec.label or f"{name} {port}"))
                    )
                    if spec.unit:
                        rdf.add((point, HAS_UNIT, URIRef(spec.unit)))
                    if spec.quantity_kind:
                        rdf.add((point, HAS_QUANTITY_KIND, URIRef(spec.quantity_kind)))
            metadata[name] = rdf
        report = {
            name: {
                "matches": matches[name],
                "skipped": {role: sorted(groups) for role, groups in skipped[name].items()},
            }
            for name in matches
        }
        return operators, metadata, report

    def _metadata_changes(self, metadata):
        """Apps whose output metadata differs from the published graph."""

        def terms(graph):
            return {
                (
                    str(subject),
                    str(predicate),
                    (str(obj), str(obj.datatype or XSD.string), obj.language)
                    if isinstance(obj, Literal)
                    else str(obj),
                )
                for subject, predicate, obj in graph
            }

        return sorted(
            name
            for name in self.state["apps"].keys() | metadata.keys()
            if terms(self.graph.data_graph(f"derived:{name}"))
            != terms(metadata.get(name, Graph()))
        )

    def _publish_metadata(self, metadata):
        changed = False
        for name in self._metadata_changes(metadata):
            graph_uri = self.graph.source_graph_uri(f"derived:{name}")
            desired = metadata.get(name, Graph())
            self.graph.insert_graph(desired, replace=True, graph_uri=graph_uri)
            changed = True
        return changed

    def _install(self, declarations):
        previous_metadata = {
            name: self.graph.data_graph(f"derived:{name}")
            for name in self.state["apps"].keys() | declarations.keys()
        }
        accepted = False
        previous = self.state
        try:
            for _ in range(8):
                operators, metadata, report = self._compile(declarations)
                if not self._publish_metadata(metadata):
                    break
            else:
                raise ValueError("derived semantic selections did not stabilize")
            revision = self.graph.graph_status()["source_version"]
            # A complete-plan identity includes semantic context and all retained apps.
            plan = {"id": f"acquirium:{_digest(operators)}", "operators": operators}
            active = self.core.engine.snapshot().plan
            pending = self.state.get("plan")
            activation_id = (
                self.state.get("activation_id")
                if pending
                and pending["id"] == plan["id"]
                and (not active or active["id"] != plan["id"])
                else None
            )
            state = {
                "apps": declarations,
                "plan": plan,
                "graph_revision": revision,
                "activation_id": activation_id or uuid4().hex,
            }
            self._save(dict(state, previous=previous))
            self.report = report
            if not active or active["id"] != plan["id"]:
                receipt = self.core.activate(state["activation_id"], plan)
                accepted = True
                self.state = state
                self._save(state)
                try:
                    self.core.wait(receipt)
                except (ExecutionError, PendingError):
                    self.error = None
                    return
            self.state = state
            self._save(state)
            self._revision = revision
            self.error = None
        except BaseException:
            if not accepted:
                self._save(previous)
                self._publish_metadata(previous_metadata)
            raise

    def refresh(self):
        with self.lock:
            if self._revision == self.graph.graph_status()["source_version"]:
                return
            try:
                self._install(dict(self.state["apps"]))
            except Exception as error:
                self.error = f"{type(error).__name__}: {error}"
                raise

    def deploy(self, app):
        with self.lock:
            definition = (
                app.to_dict() if isinstance(app, App) else App.from_dict(app).to_dict()
            )
            declarations = dict(self.state["apps"])
            declarations[definition["name"]] = definition
            self._install(declarations)
            return self.inspect(definition["name"])

    def preview(self, app):
        """What deploying ``app`` would do, without publishing or activating anything.

        Compiles every app once against the current graph and asks Escapement
        what the resulting plan would re-evaluate. Apps that select the outputs
        of an app listed in ``metadata_changes`` are compiled against its
        current outputs, so their own membership changes are not included.
        """
        with self.lock:
            definition = (
                app.to_dict() if isinstance(app, App) else App.from_dict(app).to_dict()
            )
            name = definition["name"]
            declarations = dict(self.state["apps"])
            declarations[name] = definition
            operators, metadata, report = self._compile(declarations)
            plan = {"id": f"acquirium:{_digest(operators)}", "operators": operators}
            preview = self.core.engine.preview(plan)
            apps = {}
            for item in preview["operators"]:
                totals = apps.setdefault(
                    item["id"].split(":", 1)[0],
                    {
                        "operators": 0,
                        "changes": {},
                        "groups": 0,
                        "input_rows": 0,
                        "cleared_rows": 0,
                        "downstream": False,
                    },
                )
                totals["operators"] += 1
                totals["changes"][item["change"]] = totals["changes"].get(item["change"], 0) + 1
                for key in ("groups", "input_rows", "cleared_rows"):
                    totals[key] += item[key]
                totals["downstream"] |= item["downstream"]
            removed = {}
            for item in preview["removed"]:
                app_name = item["id"].split(":", 1)[0]
                removed[app_name] = removed.get(app_name, 0) + item["retracted_rows"]
            return {
                "name": name,
                "dry_run": True,
                "plan": plan["id"],
                "active_plan": (self.core.engine.snapshot().plan or {}).get("id"),
                "epoch": preview["epoch"],
                "within_limits": preview["within_limits"],
                "matches": report[name]["matches"],
                "warnings": _warnings(report[name]),
                "apps": apps,
                "removed": [
                    {"app": key, "retracted_rows": rows} for key, rows in sorted(removed.items())
                ],
                "metadata_changes": self._metadata_changes(metadata),
            }

    def remove(self, name):
        with self.lock:
            if name not in self.state["apps"]:
                raise KeyError(name)
            declarations = dict(self.state["apps"])
            del declarations[name]
            self._install(declarations)
            active = self.core.engine.snapshot().plan
            desired = self.state["plan"]
            return {
                "name": name,
                "status": "removed"
                if active and active["id"] == desired["id"]
                else "pending",
                "active_plan": (active or {}).get("id"),
                "desired_plan": desired["id"],
                "error": self.core.processor.status()["error"],
            }

    def inspect(self, name):
        with self.lock:
            definition = self.state["apps"][name]
            plan = self.core.engine.snapshot().plan
            operators = [
                item
                for item in (plan or {}).get("operators", [])
                if item["id"].startswith(f"{name}:")
            ]
            report = self.report.get(name, {"matches": {}, "skipped": {}})
            return {
                "name": name,
                "definition": definition,
                "operators": operators,
                "status": "active"
                if plan and plan["id"] == (self.state.get("plan") or {}).get("id")
                else "pending",
                "active_plan": (plan or {}).get("id"),
                "desired_plan": (self.state.get("plan") or {}).get("id"),
                "graph_revision": self.state.get("graph_revision"),
                "error": self.error or self.core.processor.status()["error"],
                "matches": report["matches"],
                "warnings": _warnings(report),
            }

    def list(self):
        return [self.inspect(name) for name in sorted(self.state["apps"])]
