"""Durable deployment registry and graph-recompiled materializer service."""
from __future__ import annotations

from acquirium.Materialization.checks import check_entry, check_outputs

import re
from dataclasses import replace
from uuid import uuid4
from hashlib import sha256
from datetime import datetime
from typing import Any
from time import monotonic
from threading import Lock, RLock
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium.Materialization.incremental import (
    ApplicationGraph, InProcessExecutor, OutputBuilder, RevisionStore, Scheduler, TimeWindow, _duration,
)
from acquirium.Materialization.planner import BindingPlanner, Deployment
from acquirium.Storage.graph_registry import ACQUIRIUM_GRAPH_URI
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME, ACQUIRIUM_SOURCE_ID, ACQUIRIUM_VALUE_KIND,
    DATA_SOURCE, HAS_EXTERNAL_REFERENCE, HAS_MEDIUM, HAS_QUANTITY_KIND, HAS_UNIT,
    IS_CALCULATED_FROM, OF_SUBSTANCE, PRODUCED_BY, PRODUCES,
    TIMESERIES_REFERENCE,
)


def _default_label(binding, port: str, spec) -> str:
    """Name a derived stream the most specific way its binding allows.

    The author's own ``label`` wins over this, and a ``named`` output already
    carries a human name. Otherwise the label reads as the thing measured
    followed by what produced it — ``Basin 1 inlet temperature
    (normalize-temperatures[celsius])`` — with any previous parenthetical
    dropped, so a chain of apps stays one hop deep instead of accumulating.
    An output over several streams has no single subject, so it is named for
    its app and port alone.
    """
    if spec.stream_name:
        return spec.stream_name
    tag = f"{binding.application_name}[{port}]"
    streams = [stream for values in binding.inputs.values() for stream in values]
    if len(streams) == 1 and streams[0].label:
        subject = re.sub(r"\s*\([^()]*\)$", "", streams[0].label)
        return f"{subject} ({tag})"
    return tag


class Materializer:
    """Orchestration facade; recoverable state lives in the timeseries store."""
    def __init__(self, store: Any, graph: Any, *, query_resolver=None, record_resolver=None,
                 unit_converter=None, max_workers: int = 2) -> None:
        self._store, self._graph = store, graph
        self._planner = BindingPlanner(graph, query_resolver=query_resolver, record_resolver=record_resolver)
        self._revisions = RevisionStore(store, unit_converter=unit_converter)
        self._scheduler = Scheduler(self._revisions, InProcessExecutor(), max_workers=max_workers)
        self._coordinator_lock = Lock()
        self._generations: dict[str, str] = {}
        self._plan_errors: dict[str, str] = {}
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
            self._execute(conn, "ALTER TABLE materialization_lineage ADD COLUMN IF NOT EXISTS context_hash VARCHAR DEFAULT ''")

    def _execute(self, conn: Any, query: str, params=()):
        if getattr(self._store, "materialization_backend", None) == "postgres":
            query = query.replace("?", "%s")
        return conn.execute(query, list(params))

    def deploy(self, deployment: Deployment) -> None:
        with self._plan_lock:
            deployments = [d for d in self._deployments() if d.name != deployment.name]
            deployments.append(deployment)
            revision = int(self._graph.graph_status().get("published_version", 0))
            self._planner.compile(deployments, revision)
            with self._store._lock, self._store._write_conn() as conn:
                self._execute(conn, "INSERT INTO materialization_deployments VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET deployment_json=EXCLUDED.deployment_json", [deployment.name, deployment.to_json()])
                self._generations[deployment.name] = uuid4().hex
                self._revoke(deployment.name)
            self._graph_revision = -1

    def _revoke(self, name: str) -> None:
        if self._revisions.active_bindings is not None:
            for binding in self._dag.bindings:
                if binding.application_name == name:
                    self._revisions.active_bindings.pop(binding.progress_key, None)

    def remove(self, name: str) -> None:
        with self._plan_lock, self._store._lock, self._store._write_conn() as conn:
            if self._execute(conn, "DELETE FROM materialization_deployments WHERE name=? RETURNING name", [name]).fetchone() is None:
                raise KeyError(name)
            self._execute(conn, """DELETE FROM binding_progress WHERE progress_key IN
                (SELECT progress_key FROM materialization_lineage WHERE application_name=?)""", [name])
            self._execute(conn, """DELETE FROM materialization_work WHERE progress_key IN
                (SELECT progress_key FROM materialization_lineage WHERE application_name=?)""", [name])
            self._revoke(name)
            self._generations.pop(name, None)
            self._graph_revision = -1

    def close(self) -> None:
        with self._coordinator_lock:
            self._scheduler.close()

    def configure_workers(self, count: int) -> None:
        with self._coordinator_lock:
            self._scheduler.close()
            self._scheduler = Scheduler(self._revisions, max_workers=count)

    def failures(self) -> dict[str, str]:
        errors = dict(self._plan_errors)
        for binding in self._dag.bindings:
            error = self._scheduler.errors.get(binding.signature)
            if error:
                errors[f"{binding.application_name}[{binding.signature[:8]}]"] = error
        return errors

    def reprocess(self, name: str, start: datetime, end: datetime) -> dict[str, Any]:
        window = TimeWindow(start, end)
        with self._coordinator_lock, self._plan_lock:
            if name not in {d.name for d in self._deployments()}:
                raise KeyError(name)
            dag, applications = self._plan_snapshot()
            bindings = [b for b in dag.bindings if b.application_name == name]
            for binding in bindings:
                self._revisions.initialise(binding, applications[binding.signature].backfill)
            self._revisions.request_reprocess(bindings, window)
        return {"name": name, "status": "reprocessing", "bindings": len(bindings)}

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
            entry = check_entry(binding)
            bindings.append(entry)
            batch = self._revisions.preview_batch(binding)
            if batch is None:
                entry["error"] = "no stored data for these inputs"
                continue
            entry["read_window"] = [batch.context.read_window.start.isoformat(),
                                    batch.context.read_window.end.isoformat()]
            entry["input_rows"] = {alias: stream_set.collect().num_rows
                                   for alias, stream_set in batch.inputs.items()}
            builder = OutputBuilder(binding.outputs)
            try:
                applications[binding.signature].transform(batch.inputs, builder, batch.context)
            except BaseException as error:
                # A failing transform is the normal reason to run a check, so
                # report it as a result rather than an unhandled error.
                entry["error"] = f"{type(error).__name__}: {error}"
                continue
            entry["outputs"] = check_outputs(binding, batch.context, builder.values, limit)
        return {"app": deployment.name, "graph_revision": revision, "bindings": bindings}

    def _deployments(self) -> tuple[Deployment, ...]:
        with self._store._own_conn() as conn:
            return tuple(Deployment.from_json(row[0]) for row in self._execute(conn, "SELECT deployment_json FROM materialization_deployments ORDER BY name").fetchall())

    def refresh(self) -> None:
        with self._plan_lock:
            revision = int(self._graph.graph_status().get("published_version", 0))
            if revision == self._graph_revision: return
            bindings, applications, self._plan_errors = [], {}, {}
            for deployment in self._deployments():
                try:
                    partial, loaded = self._planner.compile((deployment,), revision)
                    generation = self._generations.setdefault(deployment.name, uuid4().hex)
                    bindings.extend(replace(b, generation=f"{generation}:{b.signature}:{sha256(repr(b.result).encode()).hexdigest()}") for b in partial.bindings)
                    applications.update(loaded)
                except Exception as error:
                    self._plan_errors[deployment.name] = f"{type(error).__name__}: {error}"
                    for binding in self._dag.bindings:
                        if binding.application_name == deployment.name:
                            bindings.append(binding)
                            applications[binding.signature] = self._applications[binding.signature]
            try:
                dag = ApplicationGraph(bindings)
            except ValueError as error:
                self._plan_errors["graph"] = str(error)
                return
            with self._store._own_conn() as conn:
                prior_outputs = dict(self._execute(conn, "SELECT output_ref_uri, progress_key FROM materialization_lineage").fetchall())
                prior_context = dict(self._execute(conn, "SELECT progress_key, context_hash FROM materialization_lineage").fetchall())
            repair = [b for b in dag.bindings if
                      any(p.ref_uri in prior_outputs and prior_outputs[p.ref_uri] != b.progress_key for p in b.outputs.values())
                      or (prior_context.get(b.progress_key) and prior_context[b.progress_key] != sha256(repr(b.result).encode()).hexdigest())]
            # Materialization-owned provenance is a complete projection of the
            # current DAG. Publish it only when the projection actually changed:
            # lineage writes advance the graph's published_version, so publishing
            # on every refresh would self-trigger a perpetual recompile loop.
            signatures = frozenset(binding.signature for binding in dag.bindings)
            if signatures != self._lineage_signatures:
                self._publish_graph_lineage(dag.bindings)
                self._lineage_signatures = signatures
            # Schedule repair before recording the new context fingerprint.
            # A crash can repeat a repair, but cannot forget that it is needed.
            with self._store._lock:
                self._revisions.active_bindings = {b.progress_key: b.generation for b in dag.bindings}
            for binding in repair:
                window = self._revisions.retained_window(binding)
                if window is None:
                    continue
                self._revisions.initialise(binding, applications[binding.signature].backfill)
                with self._store._lock, self._store._write_conn() as conn:
                    keys = {binding.progress_key} | {prior_outputs[p.ref_uri] for p in binding.outputs.values() if p.ref_uri in prior_outputs}
                    for key in keys:
                        self._execute(conn, "DELETE FROM materialization_work WHERE progress_key=?", [key])
                self._revisions.request_reprocess([binding], window)
            with self._store._lock, self._store._write_conn() as conn:
                self._revisions.active_bindings = {b.progress_key: b.generation for b in dag.bindings}
                failed = list(self._plan_errors)
                if failed:
                    marks = ','.join('?' for _ in failed)
                    self._execute(conn, f"DELETE FROM materialization_lineage WHERE application_name NOT IN ({marks})", failed)
                else:
                    self._execute(conn, "DELETE FROM materialization_lineage")
                for binding in dag.bindings:
                    rows = [(binding.signature, binding.progress_key, binding.application_name, binding.executable_digest,
                        alias, stream.ref_uri, output_name, output_ref, sha256(repr(binding.result).encode()).hexdigest())
                        for alias, streams in binding.inputs.items() for stream in streams
                        for output_name, output in binding.outputs.items()
                        for output_ref in (output.ref_uri,)]
                    if not rows:
                        # Empty aggregates retain output ownership across restart.
                        # Empty alias/ref fields represent no input edge in SQL.
                        rows = [(binding.signature, binding.progress_key, binding.application_name,
                                 binding.executable_digest, "", "", name, port.ref_uri,
                                 sha256(repr(binding.result).encode()).hexdigest())
                                for name, port in binding.outputs.items()]
                    if getattr(self._store, "materialization_backend", None) == "postgres":
                        with conn.cursor() as cur:
                            cur.executemany("INSERT INTO materialization_lineage VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
                    else:
                        conn.executemany("INSERT OR REPLACE INTO materialization_lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
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
            for name, output in binding.outputs.items():
                spec = output.spec
                ref, point = URIRef(output.ref_uri), URIRef(output.point_uri)
                graph.add((binding_uri, PRODUCES, ref))
                graph.add((point, HAS_EXTERNAL_REFERENCE, ref))
                # Which app made this. On the point, where query attributes
                # are matched, so `measurement(app="…")` finds one app's work
                # without the author having to tag it by hand.
                graph.add((point, PRODUCED_BY, Literal(binding.application_name)))
                graph.add((ref, RDF.type, TIMESERIES_REFERENCE))
                graph.add((ref, ACQUIRIUM_SOURCE_ID, Literal(f"derived:{binding.application_name}")))
                graph.add((ref, ACQUIRIUM_REF_NAME, Literal(output.ref_name)))
                if spec.value_kind: graph.add((ref, ACQUIRIUM_VALUE_KIND, Literal(spec.value_kind)))
                if spec.unit: graph.add((point, HAS_UNIT, URIRef(spec.unit)))
                # A point the author supplied is theirs and already has a
                # name; only a point this runtime created gets a generated
                # one, so a derived stream never shows up as a bare URI.
                label = spec.label or (None if spec.point_uri else _default_label(binding, name, spec))
                if label: graph.add((point, RDFS.label, Literal(label)))
                if spec.quantity_kind: graph.add((point, HAS_QUANTITY_KIND, URIRef(spec.quantity_kind)))
                if spec.medium: graph.add((point, HAS_MEDIUM, URIRef(spec.medium)))
                if spec.substance: graph.add((point, OF_SUBSTANCE, URIRef(spec.substance)))
                # On the point, not the reference: that is where driver
                # registration records it, and where the query layer looks —
                # measurement(data_source=...) filters the point.
                if spec.data_source: graph.add((point, DATA_SOURCE, Literal(spec.data_source)))
                for predicate, values in (spec.properties or {}).items():
                    for value in values: graph.add((point, URIRef(predicate), URIRef(value)))
        self._graph.insert_graph(
            graph,
            format="turtle",
            replace=True,
            graph_uri=URIRef(ACQUIRIUM_GRAPH_URI),
        )

    def run_once(self) -> bool:
        if not self._coordinator_lock.acquire(blocking=False):
            return False
        try:
            return self._run_once()
        finally:
            self._coordinator_lock.release()

    def _run_once(self) -> bool:
        dag, applications = self._plan_snapshot()
        ran = False
        blocked = set()
        pending_work = self._revisions.pending_keys()
        # A completed wave publishes before a dependent wave reads its next
        # revision, preserving DAG semantics across this scheduler tick.
        for wave in dag.layers():
            blocked.update(target for source, target, _ in dag.edges if source in blocked)
            now = monotonic()
            current, progress = self._revisions.progress_snapshot()
            ready, previous = [], {}
            for binding in wave:
                if binding.signature in blocked:
                    continue
                app = applications[binding.signature]
                consumed = progress.get(binding.progress_key)
                if consumed is None:
                    consumed = self._revisions.initialise(binding, app.backfill)
                if current <= consumed and binding.progress_key not in pending_work:
                    self._pending_since.pop(binding.signature, None)
                    continue
                first = self._pending_since.setdefault(binding.signature, now)
                # Delay from the first pending change, then enforce the rate cap.
                min_interval = _duration(app.min_interval) if app.min_interval is not None else None
                if min_interval is not None and now - self._last_run.get(binding.signature, 0) < min_interval.total_seconds():
                    continue
                if now - first < _duration(app.batch_delay).total_seconds():
                    continue
                ready.append(binding)
                previous[binding.signature] = consumed
            if not ready:
                blocked.update(b.signature for b in wave if b.signature in self._scheduler.errors)
                continue
            successes = {b.signature: self._scheduler.last_success.get(b.signature) for b in ready}
            ran = self._scheduler.run_layer(ready, applications) or ran
            blocked.update(b.signature for b in ready if b.signature in self._scheduler.errors)
            _, progressed = self._revisions.progress_snapshot()
            for binding in ready:
                executed = successes[binding.signature] != self._scheduler.last_success.get(binding.signature)
                if executed:
                    self._last_run[binding.signature] = now
                if executed or progressed.get(binding.progress_key, 0) > previous[binding.signature]:
                    self._pending_since.pop(binding.signature, None)
        return ran

    def dag(self) -> dict[str, Any]:
        dag, applications = self._plan_snapshot()
        current = self._revisions.current_revision()
        pending_work = self._revisions.pending_keys()
        blocked = set(self._scheduler.errors)
        for wave in dag.layers():
            blocked.update(target for source, target, _ in dag.edges if source in blocked)
        nodes = []
        for binding in dag.bindings:
            with self._store._own_conn() as conn:
                row = self._execute(conn, "SELECT consumed_revision FROM binding_progress WHERE progress_key=?", [binding.progress_key]).fetchone()
            nodes.append({"binding_signature": binding.signature, "application_name": binding.application_name,
                "inputs": {key: [item.ref_uri for item in value] for key,value in binding.inputs.items()},
                "outputs": {key: output.ref_uri for key, output in binding.outputs.items()},
                "lookback": "all" if binding.lookback is None else str(binding.lookback),
                "backfill": bool(applications[binding.signature].backfill),
                "consumed_revision": row[0] if row else None, "current_revision": current,
                "status": ("running" if binding.signature in self._scheduler.running else
                           "failed" if binding.signature in self._scheduler.errors else
                           "waiting" if binding.signature in blocked else
                           "reprocessing" if binding.progress_key in pending_work else
                           "pending" if row is None or row[0] < current else "idle"),
                "last_success": self._scheduler.last_success.get(binding.signature),
                "error": self._scheduler.errors.get(binding.signature)})
        return {"graph_revision": self._graph_revision, "nodes": nodes, "errors": dict(self._plan_errors),
                "edges": [{"source": source, "target": target, "ref_uri": ref} for source,target,ref in dag.edges]}
