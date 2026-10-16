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
    Operator,
    Pointwise,
    PythonFunction,
    PendingError,
    ExecutionError,
    ValidationError,
)
from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.namespace import XSD

from acquirium.internals.models import compute_ref_uri
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_SOURCE_ID,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_VALUE_KIND,
    HAS_EXTERNAL_REFERENCE,
    HAS_UNIT,
    HAS_QUANTITY_KIND,
    VIRTUAL_POINT,
    PRODUCED_BY,
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
class Output:
    unit: str | None = None
    quantity_kind: str | None = None
    label: str | None = None


@dataclass(frozen=True)
class App:
    name: str
    inputs: tuple[Selection, ...] | list[Selection]
    outputs: Mapping[str, Output]
    function: PythonFunction | Mapping[str, Any]
    window: Pointwise | FixedBucket = field(default_factory=Pointwise)

    def to_dict(self):
        if (
            not isinstance(self.name, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", self.name)
            or not self.outputs
        ):
            raise ValueError(
                "apps require a simple name (letters, digits, _, ., -) and declared outputs"
            )
        if not isinstance(self.window, (Pointwise, FixedBucket)):
            raise ValueError("only pointwise and fixed-bucket windows are supported")
        if any(not key for key in self.outputs):
            raise ValueError("output port names must be nonempty")
        return dict(
            name=self.name,
            inputs=[asdict(item) for item in self.inputs],
            outputs={key: asdict(value) for key, value in self.outputs.items()},
            function=self.function.to_dict()
            if isinstance(self.function, PythonFunction)
            else dict(self.function),
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
        else:
            raise ValueError("only pointwise and fixed-bucket windows are supported")
        app = cls(
            data["name"],
            tuple(Selection(**item) for item in data["inputs"]),
            {key: Output(**item) for key, item in data["outputs"].items()},
            data["function"],
            value,
        )
        app.to_dict()
        return app


def output_reference(app: str, group: str, port: str) -> str:
    return str(compute_ref_uri(f"derived:{app}", _json([group, port])))


class Apps:
    def __init__(self, core, graph, directory, *, converter=None):
        self.core, self.graph, self.converter = core, graph, converter
        self.lock = RLock()
        self.path = Path(directory) / "apps.json"
        self._revision = -1
        self.error = None
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
            if plan:
                # Reusing the exact declaration also works after retry-ledger expiry.
                active = self.core.engine.snapshot().plan
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
        operators, metadata = [], {}
        for name, declaration in sorted(declarations.items()):
            app = App.from_dict(declaration)
            groups = {}
            for selection in app.inputs:
                if not selection.role:
                    raise ValueError("input roles must be nonempty")
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
            rdf = Graph()
            for group, inputs in sorted(groups.items()):
                outputs = {
                    port: output_reference(name, group, port)
                    for port in sorted(app.outputs)
                }
                definition = dict(
                    function=declaration["function"],
                    window=declaration["window"],
                    outputs=outputs,
                )
                operator = Operator(
                    id=f"{name}:{_digest(group)}",
                    version=_digest(definition),
                    inputs=sorted(inputs, key=lambda item: _json(item.to_dict())),
                    outputs=outputs,
                    window=app.window,
                    function=app.function,
                )
                operators.append(operator.to_dict())
                for port, reference in outputs.items():
                    spec = app.outputs[port]
                    ref, point = URIRef(reference), URIRef(f"{reference}__point")
                    rdf.add((ref, ACQUIRIUM_SOURCE_ID, Literal(f"derived:{name}")))
                    rdf.add((ref, ACQUIRIUM_REF_NAME, Literal(_json([group, port]))))
                    rdf.add((ref, ACQUIRIUM_VALUE_KIND, Literal("numeric")))
                    rdf.add((point, RDF.type, VIRTUAL_POINT))
                    rdf.add((point, HAS_EXTERNAL_REFERENCE, ref))
                    rdf.add((point, PRODUCED_BY, Literal(name)))
                    rdf.add(
                        (point, RDFS.label, Literal(spec.label or f"{name} {port}"))
                    )
                    if spec.unit:
                        rdf.add((point, HAS_UNIT, URIRef(spec.unit)))
                    if spec.quantity_kind:
                        rdf.add((point, HAS_QUANTITY_KIND, URIRef(spec.quantity_kind)))
            metadata[name] = rdf
        return operators, metadata

    def _publish_metadata(self, metadata):
        changed = False
        for name in self.state["apps"].keys() | metadata.keys():
            graph_uri = self.graph.source_graph_uri(f"derived:{name}")
            desired = metadata.get(name, Graph())
            existing = self.graph.data_graph(f"derived:{name}")

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

            if terms(existing) != terms(desired):
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
                operators, metadata = self._compile(declarations)
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
            }

    def list(self):
        return [self.inspect(name) for name in sorted(self.state["apps"])]
