"""Deployments over time, admission, and the steps that keep views current.

``Materializer`` owns the deployment log, the plan of every deployment over
all of time, and the engine whose steps apply the inbox and recompute what
changed (``evaluation.ViewEvaluator``). A deployment change and a model write
are checked against the whole plan before they hold: a deployment that fails
admission is not recorded, and a model write that fails it is undone. The
plan is cached per (model history head, deployment log head).

The timeseries database is the recovery authority: the deployment log, the
inbox, the evaluator's frontier and every output survive a restart. The plan
and the loaded app code are rebuilt in the process.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from threading import RLock
from typing import Any

from rdflib import Graph, URIRef

from acquirium.internals.internals_namespaces import IS_CALCULATED_FROM, PRODUCES
from acquirium.internals.intervals import NEG_INF, POS_INF, Interval, from_micros, merge, now_micros, to_micros
from acquirium.Materialization.admission import AdmissionError
from acquirium.Materialization.deployments import DeploymentChange, DeploymentLog, changed, deployment
from acquirium.Materialization.engine import Engine
from acquirium.Materialization.evaluation import EvaluationReport, SAFE_HIGH, SAFE_LOW, ViewEvaluator, _Reader, _Runner
from acquirium.Materialization.planner import BindingPlanner, Deployment, TemporalPlan
from acquirium.Materialization.publication import _default_label, output_triples  # noqa: F401
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI
from acquirium.Storage.metadata_outbox import execute


class Materializer:
    """Deploy, remove and check apps; run steps; describe the plan."""

    def __init__(self, store: Any, graph: Any, *, query_resolver=None, record_resolver=None,
                 unit_converter=None, max_workers: int = 2) -> None:
        self._store, self._graph = store, graph
        self._planner = BindingPlanner(graph, query_resolver=query_resolver, record_resolver=record_resolver)
        self._log = DeploymentLog(store)
        self._unit_converter = unit_converter
        self._lock = RLock()
        self._plan: tuple[TemporalPlan, int, int] | None = None
        self._applied: tuple[TemporalPlan, int, int] | None = None
        self._plan_error: str | None = None
        self._published_lineage: int | None = None
        self.evaluator = ViewEvaluator(store, self, unit_converter)
        self.engine = Engine(store, evaluator=self.evaluator)
        self._admitted_head = self._model_head()

    # -------------------- heads and plans --------------------
    def _model_head(self) -> int:
        history = getattr(self._graph, "history", None)
        return history.head() if history is not None else 0

    def current_plan(self) -> tuple[TemporalPlan, int, int] | None:
        """The plan for the current heads, planned once per pair of heads."""
        with self._lock:
            key = (self._model_head(), self._log.head())
            if self._plan is not None and self._plan[1:] == key:
                return self._plan
            try:
                plan = self._planner.temporal_plan(self._log.changes())
            except Exception as error:
                self._plan_error = f"{type(error).__name__}: {error}"
                return None
            self._plan_error = None
            self._plan = (plan, *key)
            self._publish_lineage(plan)
            return self._plan

    def applied_plan(self) -> tuple[TemporalPlan, int, int] | None:
        return self._applied

    def step_committed(self, applied: tuple[TemporalPlan, int, int]) -> None:
        self._applied = applied

    def changed_region(self, old_model: int, model_head: int, old_deployment: int, deployment_head: int) -> list[Interval]:
        """Where model writes and deployment changes between two pairs of
        heads changed anything, for any view."""
        region: list[Interval] = []
        history = getattr(self._graph, "history", None)
        if history is not None:
            for r in range(old_model + 1, model_head + 1):
                for spans in history.changed(r).values():
                    region.extend(spans)
        changes = self._log.changes()
        for r in range(old_deployment + 1, deployment_head + 1):
            for spans in changed(changes, r).values():
                region.extend(spans)
        return merge(region)

    def refresh(self) -> None:
        self.current_plan()

    def known_views(self) -> set[str]:
        """Every view name any deployment change has named."""
        return {change.name for change in self._log.changes()}

    # -------------------- deployment changes --------------------
    def deploy(self, deployment_: Deployment, *, start: int | None = None) -> None:
        """Deploy from ``start`` (microseconds, ``NEG_INF`` for all history;
        now if omitted) on. A view on a loop that asks for all history starts
        at the earliest raw sample it depends on."""
        start = now_micros() if start is None else start
        text = deployment_.to_json()
        with self._lock:
            changes = self._log.changes()
            head = max((c.r for c in changes), default=0)
            proposed = changes + [DeploymentChange(head + 1, deployment_.name, text, start, POS_INF)]
            try:
                plan = self._planner.temporal_plan(proposed)
            except AdmissionError as error:
                if start != NEG_INF or "needs a finite start" not in str(error):
                    raise
                start = self._earliest_input(changes, deployment_, head)
                proposed[-1] = DeploymentChange(head + 1, deployment_.name, text, start, POS_INF)
                plan = self._planner.temporal_plan(proposed)
            self._log.record([(deployment_.name, text, start, POS_INF)])
            self._plan = (plan, self._model_head(), self._log.head())
            self._publish_lineage(plan)

    def _earliest_input(self, changes: list[DeploymentChange], deployment_: Deployment, head: int) -> int:
        """The earliest raw sample a view depends on, directly or through other
        views; now if there is none."""
        now = now_micros()
        probe = changes + [DeploymentChange(head + 1, deployment_.name, deployment_.to_json(), now, POS_INF)]
        plan = self._planner.temporal_plan(probe)
        writers: dict[str, list] = defaultdict(list)
        for instance in plan.instances:
            for port in instance.outputs.values():
                writers[port.ref_uri].append(instance)
        seen, raw = set(), set()
        frontier = [i for i in plan.instances if i.view == deployment_.name]
        while frontier:
            instance = frontier.pop()
            if id(instance) in seen:
                continue
            seen.add(id(instance))
            refs = [d.ref_uri for row in instance.rows for ds in row.streams.values() for d in ds]
            refs += [d.ref_uri for d in instance.named.values()]
            for ref in refs:
                if ref in writers:
                    frontier.extend(writers[ref])
                else:
                    raw.add(ref)
        with self._store._own_conn() as conn:
            extent = _Reader(self._store, conn).extent(raw)
        return extent[0] if extent is not None else now

    def remove(self, name: str, *, at: int | None = None, purge: bool = False) -> None:
        """Remove an app from ``at`` (now if omitted) on; its outputs before
        that stay. ``purge`` removes it over all time, outputs included."""
        with self._lock:
            changes = self._log.changes()
            known = {c.name for c in changes}
            if name not in (known if purge else self._log.current()):
                raise KeyError(name)
            start = NEG_INF if purge else (now_micros() if at is None else at)
            head = max((c.r for c in changes), default=0)
            plan = self._planner.temporal_plan(changes + [DeploymentChange(head + 1, name, None, start, POS_INF)])
            self._log.record([(name, None, start, POS_INF)])
            self._plan = (plan, self._model_head(), self._log.head())
            self._publish_lineage(plan)

    def model_changed(self) -> None:
        """Admit the model as recorded now, or undo every write since the last
        admitted one and raise."""
        with self._lock:
            head = self._model_head()
            if head == self._admitted_head:
                return
            try:
                plan = self._planner.temporal_plan(self._log.changes())
            except AdmissionError:
                self._graph.undo_writes(self._admitted_head)
                raise
            except Exception as error:
                # A query that fails is the view's problem, not the model's.
                self._plan_error = f"{type(error).__name__}: {error}"
                self._admitted_head = head
                return
            self._admitted_head = head
            self._plan = (plan, head, self._log.head())
            self._plan_error = None
            self._publish_lineage(plan)

    # -------------------- steps --------------------
    def run_once(self) -> bool:
        return self.engine.step() is not None

    def step(self):
        return self.engine.step()

    def configure_workers(self, count: int) -> None:
        """Transforms run inside the step, one after another."""

    def close(self) -> None:
        pass

    def failures(self) -> dict[str, str]:
        errors = {}
        if self._plan_error:
            errors["plan"] = self._plan_error
        errors.update(self.evaluator.errors)
        return errors

    def reprocess(self, name: str, start: datetime, end: datetime) -> dict[str, Any]:
        """Recompute an app's outputs over ``[start, end]`` in the next step."""
        if name not in self._log.current():
            raise KeyError(name)
        if end < start:
            raise ValueError("reprocess end precedes its start")
        with self._store._lock, self._store._write_conn() as conn:
            execute(conn, "INSERT INTO view_pending VALUES (?, ?, ?)", [name, to_micros(start), to_micros(end) + 1])
        return {"name": name, "status": "reprocessing"}

    # -------------------- dry runs --------------------
    def check(self, deployment_: Deployment, *, limit: int | None = None,
              search_path: str | None = None) -> dict[str, Any]:
        """Run an app against stored data and return what it computed.

        Nothing is written: the app is not deployed, no derived stream is
        created and no frontier moves. The app is planned over all of time
        next to the deployed apps (whose outputs its query may select), and
        each of its instances runs over all the data it reads.
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must not be negative")
        changes = [c for c in self._log.changes() if c.name != deployment_.name]
        head = max((c.r for c in self._log.changes()), default=0)
        proposed = changes + [DeploymentChange(head + 1, deployment_.name, deployment_.to_json(), NEG_INF, POS_INF)]
        plan = self._planner.temporal_plan(proposed, search_path=search_path)
        entries = []
        with self._store._own_conn() as conn:
            reader = _DryReader(self._store, conn)
            runner = _Runner(plan, reader, self._unit_converter, EvaluationReport())
            for instance in (i for i in plan.instances if i.view == deployment_.name):
                inputs: dict[str, list] = defaultdict(list)
                for row in instance.rows:
                    for alias, streams in row.streams.items():
                        inputs[alias].extend({"ref_uri": d.ref_uri, "label": d.label, "unit": d.unit} for d in streams)
                for name, d in instance.named.items():
                    inputs[name].append({"ref_uri": d.ref_uri, "label": d.label, "unit": d.unit})
                entry = {"inputs": dict(inputs),
                         "row": dict(instance.rows[0].columns) if deployment_.grouping == "per_match" and instance.rows else None,
                         "outputs": {}, "error": None}
                entries.append(entry)
                extent = runner.extent(instance)
                if extent is None:
                    entry["error"] = "no stored data for these inputs"
                    continue
                entry["read_window"] = [from_micros(max(extent[0], SAFE_LOW)).isoformat(),
                                        from_micros(min(extent[1], SAFE_HIGH) - 1).isoformat()]
                reader.captured.clear()
                try:
                    for window in runner.clamp(instance, list(instance.present), extent):
                        runner.compute(instance, window)
                except BaseException as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    continue
                entry["input_rows"] = dict(reader.rows_read)
                entry["outputs"] = _check_outputs(instance, reader.captured, limit)
        return {"app": deployment_.name, "graph_revision": self._model_head(), "bindings": entries}

    # -------------------- description --------------------
    def _deployed(self) -> dict[str, Deployment]:
        current = self._log.current()
        return {name: Deployment.from_json(text) for name, text in sorted(current.items())}

    def dag(self) -> dict[str, Any]:
        """The current plan as nodes (instances) and edges (stream reads)."""
        prepared = self.current_plan()
        plan = prepared[0] if prepared else None
        instances = plan.instances if plan else ()
        writer: dict[str, str] = {}
        for instance in instances:
            for port in instance.outputs.values():
                writer[port.ref_uri] = instance.id
        errors = self.evaluator.errors
        nodes, edges = [], []
        for instance in instances:
            node = instance.id
            inputs: dict[str, list[str]] = defaultdict(list)
            for row in instance.rows:
                for alias, streams in row.streams.items():
                    inputs[alias].extend(d.ref_uri for d in streams)
            for name, d in instance.named.items():
                inputs[name].append(d.ref_uri)
            error = errors.get(instance.id)
            nodes.append({"binding_signature": node, "application_name": instance.view,
                          "inputs": dict(inputs), "outputs": {p: o.ref_uri for p, o in instance.outputs.items()},
                          "present": [[_render(s), _render(e)] for s, e in instance.present],
                          "status": "failed" if error else "idle", "error": error})
            for refs in inputs.values():
                for ref in refs:
                    if ref in writer:
                        edges.append({"source": writer[ref], "target": node, "ref_uri": ref})
        return {"graph_revision": self._model_head(), "deployment_revision": self._log.head(),
                "nodes": nodes, "edges": edges, "errors": self.failures(),
                "inbox": self.engine.pending(), "last_step": self.engine.last_step(),
                "loops": sorted(plan.schedule.loops) if plan else []}

    def list_apps(self) -> list[dict[str, Any]]:
        return self._inspect()

    def inspect_app(self, name: str) -> dict[str, Any]:
        return self._inspect(name)[0]

    def _inspect(self, name: str | None = None) -> list[dict[str, Any]]:
        deployed = self._deployed()
        if name is not None and name not in deployed:
            raise KeyError(name)
        snapshot = self.dag()
        by_app: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for node in snapshot["nodes"]:
            by_app[node["application_name"]].append(node)
        changes = self._log.changes()
        results = []
        for app_name, definition in deployed.items():
            if name is not None and app_name != name:
                continue
            bindings = by_app.get(app_name, [])
            counts: dict[str, int] = defaultdict(int)
            for binding in bindings:
                counts[binding["status"]] += 1
            error = self._plan_error or next(
                (e for k, e in snapshot["errors"].items() if k.startswith(f"{app_name}:")), None)
            status = "failed" if error else ("idle" if bindings else "no_matches")
            result = {"name": app_name, "entrypoint": definition.entrypoint, "grouping": definition.grouping,
                      "status": status, "binding_count": len(bindings), "binding_statuses": dict(counts),
                      "graph_revision": snapshot["graph_revision"], "plan_current": self._plan_error is None,
                      "error": error,
                      "deployed": [[_render(s), _render(e)] for s, e, _ in deployment(changes, app_name)]}
            if name is not None:
                result["definition"] = json.loads(definition.to_json())
                result["output_schemas"] = {
                    key: {"time": {"type": "timestamp[us, tz=UTC]", "nullable": False},
                          "value": {"type": "float64" if spec.value_kind == "numeric" else "string", "nullable": False}}
                    for key, spec in definition.outputs.items()}
                result["bindings"] = bindings
            results.append(result)
        return results

    # -------------------- lineage for the live graph --------------------
    def _publish_lineage(self, plan: TemporalPlan) -> None:
        """Publish the outputs present now into Acquirium's own graph, where
        live queries find them. Planning never reads this graph."""
        if not hasattr(self._graph, "insert_graph"):
            return
        now = now_micros()
        graph = Graph()
        for instance in plan.instances:
            if not any(s <= now < e for s, e in instance.present):
                continue
            binding_uri = URIRef(f"urn:acquirium:binding:{instance.id.replace(':', '/').replace('*', 'all')}")
            inputs: dict[str, list] = defaultdict(list)
            for row in instance.rows:
                for alias, streams in row.streams.items():
                    inputs[alias].extend(streams)
            for name, d in instance.named.items():
                inputs[name].append(d)
            for streams in inputs.values():
                for d in streams:
                    graph.add((binding_uri, IS_CALCULATED_FROM, URIRef(d.ref_uri)))
            for port_name, port in instance.outputs.items():
                graph.add((binding_uri, PRODUCES, URIRef(port.ref_uri)))
                graph += output_triples(instance.view, dict(inputs), port_name, port)
        digest = hash(frozenset(graph))
        if digest == self._published_lineage:
            return
        self._graph.insert_graph(graph, format="turtle", replace=True, graph_uri=URIRef(ACQUIRIUM_GRAPH_URI))
        self._published_lineage = digest


def _render(value: int) -> str:
    rendered = from_micros(value)
    return rendered if isinstance(rendered, str) else rendered.isoformat()


class _DryReader(_Reader):
    """A reader that captures outputs instead of writing them."""

    def __init__(self, store: Any, conn: Any) -> None:
        super().__init__(store, conn)
        self.captured: dict[str, dict[int, Any]] = defaultdict(dict)
        self.rows_read: dict[str, int] = defaultdict(int)

    def stage(self, ref, window, values, *, port, application):
        self.captured[ref].update(values)
        return []

    def flush(self):
        pass


def _check_outputs(instance, captured: dict[str, dict[int, Any]], limit: int | None) -> dict[str, Any]:
    outputs = {}
    for name, port in instance.outputs.items():
        values = sorted(captured.get(port.ref_uri, {}).items())
        shown = values if limit is None else values[:limit]
        outputs[name] = {"stream": port.ref_uri, "ref_name": port.ref_name, "value_kind": port.spec.value_kind,
                         "rows": len(values), "assigned": port.ref_uri in captured,
                         "truncated": len(shown) < len(values),
                         "values": [{"time": from_micros(t).isoformat(), "value": v} for t, v in shown]}
    return outputs
