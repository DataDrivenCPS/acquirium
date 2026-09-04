"""Durable deployment registry and graph-recompiled materializer service."""
from __future__ import annotations

from typing import Any
from time import monotonic
from threading import Lock, RLock
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium.Materialization.incremental import (
    ApplicationGraph, InProcessExecutor, OutputBuilder, RevisionStore, Scheduler, _duration,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME, ACQUIRIUM_SOURCE_ID, ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE, HAS_EXTERNAL_REFERENCE, HAS_MEDIUM, HAS_QUANTITY_KIND, HAS_UNIT,
    IS_CALCULATED_FROM, OF_SUBSTANCE, PRODUCES,
    TIMESERIES_REFERENCE,
)


class Materializer:
    """Small orchestration facade; all recoverable state remains in DuckDB."""
    def __init__(self, store: Any, graph: Any, *, query_resolver=None, record_resolver=None,
                 unit_converter=None) -> None:
        self._store, self._graph = store, graph
        self._planner = BindingPlanner(graph, query_resolver=query_resolver, record_resolver=record_resolver)
        self._revisions = RevisionStore(store, unit_converter=unit_converter)
        self._scheduler: Scheduler | None = None
        self._scheduler_lock = Lock()
        # The DAG and its application instances are one immutable plan. A
        # graph refresh replaces them together, while workers execute a local
        # snapshot without holding this lock.
        self._plan_lock = RLock()
        self._graph_revision = -1
        self._dag = ApplicationGraph(())
        self._applications: dict[str, Any] = {}
        self._lineage_signatures: frozenset[str] = frozenset()
        self._pending_since: dict[str, float] = {}
        self._last_run: dict[str, float] = {}
        with store._lock, store._write_conn() as conn:
            self._execute(conn, "CREATE TABLE IF NOT EXISTS materialization_deployments (name VARCHAR PRIMARY KEY, deployment_json VARCHAR NOT NULL)")
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_lineage (
                binding_signature VARCHAR NOT NULL, progress_key VARCHAR NOT NULL,
                application_name VARCHAR NOT NULL,
                executable_digest VARCHAR NOT NULL, input_alias VARCHAR NOT NULL,
                input_ref_uri VARCHAR NOT NULL, output_name VARCHAR NOT NULL,
                output_ref_uri VARCHAR NOT NULL,
                PRIMARY KEY (binding_signature, input_alias, input_ref_uri, output_name))""")

    def _execute(self, conn: Any, query: str, params=()):
        if getattr(self._store, "materialization_backend", None) == "postgres":
            query = query.replace("?", "%s")
        return conn.execute(query, list(params))

    def deploy(self, deployment: Deployment) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            self._execute(conn, "INSERT INTO materialization_deployments VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET deployment_json=EXCLUDED.deployment_json", [deployment.name, deployment.to_json()])
        with self._plan_lock:
            # Deployment changes share the graph-refresh path, avoiding a
            # second invalidation mechanism with different semantics.
            self._graph_revision = -1

    def remove(self, name: str) -> None:
        with self._store._lock, self._store._write_conn() as conn:
            if self._execute(conn, "DELETE FROM materialization_deployments WHERE name=? RETURNING name", [name]).fetchone() is None:
                raise KeyError(name)
            # Forget the removed app's frontier so redeploying the same name is
            # a fresh start (its start policy applies again), and so progress
            # rows do not accumulate forever.
            self._execute(conn, """DELETE FROM binding_progress WHERE progress_key IN
                (SELECT progress_key FROM materialization_lineage WHERE application_name=?)""", [name])
        with self._plan_lock:
            self._graph_revision = -1

    def check(self, deployment: Deployment, *, limit: int | None = None,
              search_path: str | None = None) -> dict[str, Any]:
        """Run an app against real stored data and return what it computed.

        Nothing is written: the app is not registered, its derived streams are
        not created, no progress is recorded, and the computed rows are
        returned instead of stored. Each binding reads every retained input
        row, so a check shows what the app would produce from a full backfill.

        Every computed row is returned unless ``limit`` keeps only the first
        few of each output. ``search_path`` is a directory the server looks in
        for the app's module, so a file that is not otherwise importable there
        can still be checked.
        """
        if limit is not None and limit < 0: raise ValueError("limit must not be negative")
        # Compile against the live graph without persisting the deployment or
        # publishing lineage, so a check cannot disturb what is deployed.
        revision = int(self._graph.graph_status().get("published_version", 0))
        dag, applications = self._planner.compile((deployment,), revision, search_path=search_path)
        bindings = []
        for binding in dag.bindings:
            entry: dict[str, Any] = {
                "inputs": {alias: [{"ref_uri": item.ref_uri, "label": item.label, "unit": item.unit}
                                   for item in streams]
                           for alias, streams in binding.inputs.items()},
                "entities": dict(binding.entities),
                "outputs": {}, "error": None,
            }
            bindings.append(entry)
            batch = self._revisions.preview_batch(binding)
            if batch is None:
                entry["error"] = "no stored data for these inputs"
                continue
            entry["read_window"] = [batch.read_window.start.isoformat(), batch.read_window.end.isoformat()]
            entry["input_rows"] = {alias: stream_set.collect().num_rows for alias, stream_set in batch.inputs.items()}
            builder = OutputBuilder(binding.outputs)
            try:
                applications[binding.signature].transform(batch.inputs, builder, batch)
            except BaseException as error:
                # A failing transform is the normal reason to run a check, so
                # report it as a result rather than an unhandled error.
                entry["error"] = f"{type(error).__name__}: {error}"
                continue
            for port, table in builder.values.items():
                shown = table if limit is None else table.slice(0, limit)
                times, values = shown["time"].to_pylist(), shown["value"].to_pylist()
                entry["outputs"][port] = {
                    "stream": binding.outputs[port][0],
                    "ref_name": binding.output_ref_name(port),
                    "value_kind": binding.outputs[port][1].value_kind,
                    "rows": table.num_rows,
                    "truncated": shown.num_rows < table.num_rows,
                    "values": [{"time": time.isoformat(), "value": value}
                               for time, value in zip(times, values)],
                }
            for port in binding.outputs:
                entry["outputs"].setdefault(port, {"stream": binding.outputs[port][0],
                                                   "ref_name": binding.output_ref_name(port),
                                                   "value_kind": binding.outputs[port][1].value_kind,
                                                   "rows": 0, "truncated": False, "values": []})
        return {"app": deployment.name, "graph_revision": revision, "bindings": bindings}

    def _deployments(self) -> tuple[Deployment, ...]:
        with self._store._own_conn() as conn:
            return tuple(Deployment.from_json(row[0]) for row in self._execute(conn, "SELECT deployment_json FROM materialization_deployments ORDER BY name").fetchall())

    def refresh(self) -> None:
        with self._plan_lock:
            revision = int(self._graph.graph_status().get("published_version", 0))
            if revision == self._graph_revision: return
            dag, applications = self._planner.compile(self._deployments(), revision)
            # Materialization-owned provenance is a complete projection of the
            # current DAG. Publish it only when the projection actually changed:
            # lineage writes advance the graph's published_version, so publishing
            # on every refresh would self-trigger a perpetual recompile loop.
            signatures = frozenset(binding.signature for binding in dag.bindings)
            if signatures != self._lineage_signatures:
                self._publish_graph_lineage(dag.bindings)
                self._lineage_signatures = signatures
            with self._store._lock, self._store._write_conn() as conn:
                self._execute(conn, "DELETE FROM materialization_lineage")
                for binding in dag.bindings:
                    rows = [(binding.signature, binding.progress_key, binding.application_name, binding.executable_digest,
                        alias, stream.ref_uri, output_name, output_ref)
                        for alias, streams in binding.inputs.items() for stream in streams
                        for output_name, (output_ref, _) in binding.outputs.items()]
                    if getattr(self._store, "materialization_backend", None) == "postgres":
                        with conn.cursor() as cur:
                            cur.executemany("INSERT INTO materialization_lineage VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
                    else:
                        conn.executemany("INSERT OR REPLACE INTO materialization_lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            self._dag, self._applications, self._graph_revision = dag, applications, revision

    def _plan_snapshot(self) -> tuple[ApplicationGraph, dict[str, Any]]:
        """Return a coherent plan without serializing transformation execution."""
        with self._plan_lock:
            self.refresh()
            return self._dag, dict(self._applications)


    def _publish_graph_lineage(self, bindings) -> None:
        if not hasattr(self._graph, "insert_graph"):
            return
        # Replace the dedicated materialization graph as a whole. Incremental
        # updates would leave provenance behind for removed bindings.
        graph = Graph()
        for binding in bindings:
            binding_uri = URIRef(f"urn:acquirium:binding:{binding.signature}")
            for alias, inputs in binding.inputs.items():
                for item in inputs:
                    graph.add((binding_uri, IS_CALCULATED_FROM, URIRef(item.ref_uri)))
            for name, (ref_uri, spec) in binding.outputs.items():
                ref, point = URIRef(ref_uri), URIRef(spec.point_uri or f"urn:acquirium:derived-point:{binding.signature}:{name}")
                graph.add((binding_uri, PRODUCES, ref))
                graph.add((point, HAS_EXTERNAL_REFERENCE, ref))
                graph.add((ref, RDF.type, TIMESERIES_REFERENCE))
                graph.add((ref, ACQUIRIUM_SOURCE_ID, Literal(f"derived:{binding.application_name}")))
                graph.add((ref, ACQUIRIUM_REF_NAME, Literal(binding.output_ref_name(name))))
                if spec.value_kind: graph.add((ref, ACQUIRIUM_VALUE_KIND, Literal(spec.value_kind)))
                if spec.unit: graph.add((point, HAS_UNIT, URIRef(spec.unit)))
                if spec.label: graph.add((point, RDFS.label, Literal(spec.label)))
                if spec.quantity_kind: graph.add((point, HAS_QUANTITY_KIND, URIRef(spec.quantity_kind)))
                if spec.medium: graph.add((point, HAS_MEDIUM, URIRef(spec.medium)))
                if spec.substance: graph.add((point, OF_SUBSTANCE, URIRef(spec.substance)))
                if spec.data_source: graph.add((ref, DATA_SOURCE, Literal(spec.data_source)))
                for predicate, values in (spec.properties or {}).items():
                    for value in values: graph.add((point, URIRef(predicate), URIRef(value)))
        self._graph.insert_graph(
            graph,
            format="turtle",
            replace=True,
            graph_uri=URIRef(ACQUIRIUM_GRAPH_URI),
        )

    def run_once(self) -> bool:
        dag, applications = self._plan_snapshot()
        if self._scheduler is None:
            with self._scheduler_lock:
                if self._scheduler is None:
                    # Materialization has its own bounded server worker pool.
                    # Keeping its batch execution in-process avoids racing the
                    # Ray driver supervisor during application startup.
                    self._scheduler = Scheduler(self._revisions, InProcessExecutor())
        ran = False
        # A completed wave publishes before a dependent wave reads its next
        # revision, preserving DAG semantics across this scheduler tick.
        for wave in dag.layers():
            now, current = monotonic(), self._revisions.current_revision()
            ready, previous = [], {}
            for binding in wave:
                app = applications[binding.signature]
                consumed = self._revisions.initialise(binding, app.backfill)
                if current <= consumed:
                    self._pending_since.pop(binding.signature, None)
                    continue
                first = self._pending_since.setdefault(binding.signature, now)
                # Three composable throttles instead of trigger modes: at most
                # one run per min_interval; wait coalesce for a quiet gap in a
                # burst, but never longer than max_delay.
                min_interval = _duration(app.min_interval) if app.min_interval is not None else None
                if min_interval is not None and now - self._last_run.get(binding.signature, 0) < min_interval.total_seconds():
                    continue
                elapsed, coalesce = now - first, _duration(app.coalesce)
                max_delay = _duration(app.max_delay) if app.max_delay is not None else None
                if elapsed < coalesce.total_seconds() and (max_delay is None or elapsed < max_delay.total_seconds()):
                    continue
                ready.append(binding)
                previous[binding.signature] = consumed
            if not ready:
                continue
            self._scheduler.run_layer(ready, applications)
            for binding in ready:
                with self._store._own_conn() as conn:
                    row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
                if row is not None and row[0] > previous[binding.signature]:
                    self._last_run[binding.signature] = now
                    self._pending_since.pop(binding.signature, None)
                    ran = True
        return ran

    def dag(self) -> dict[str, Any]:
        dag, applications = self._plan_snapshot()
        current = self._revisions.current_revision()
        nodes = []
        for binding in dag.bindings:
            with self._store._own_conn() as conn:
                row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            nodes.append({"binding_signature": binding.signature, "application_name": binding.application_name,
                "inputs": {key: [item.ref_uri for item in value] for key,value in binding.inputs.items()},
                "outputs": {key: ref for key,(ref,_) in binding.outputs.items()},
                "lookback": "all" if binding.lookback is None else str(binding.lookback),
                "backfill": bool(applications[binding.signature].backfill),
                "consumed_revision": row[0] if row else None, "current_revision": current, "status": "idle"})
        return {"graph_revision": self._graph_revision, "nodes": nodes,
                "edges": [{"source": source, "target": target, "ref_uri": ref} for source,target,ref in dag.edges]}
