"""Coordinate deployments, graph changes, and incremental execution.

Materializer owns the currently installed plan and execution cadence. Planning
resolves app queries; RevisionStore owns snapshot reads and atomic publication;
Scheduler executes independent bindings. Keeping these responsibilities separate
lets graph/deployment updates revoke work without waiting for transforms to end.

The SQL registry and lineage survive restart. The DAG, loaded App instances,
generation tokens, and scheduling timers are rebuilt in this process. A graph
refresh reconciles the durable record with current query matches and schedules
repairs before acknowledging changed query context in stored lineage.
"""
from __future__ import annotations

from acquirium.Materialization.checks import check_entry, check_outputs

import re
import json
from dataclasses import replace
from contextlib import contextmanager
from uuid import uuid4
from hashlib import sha256
from datetime import datetime, timezone
from typing import Any
from time import monotonic
from threading import Lock, RLock
from rdflib import Graph, Literal, RDF, RDFS, URIRef

from acquirium.Materialization.changes import ChangeFeed, StepChanges
from acquirium.Materialization.engine import Engine
from acquirium.Materialization.models import App, ApplicationGraph, OutputBuilder, TimeWindow, _duration
from acquirium.Materialization.revision_store import RevisionStore
from acquirium.Materialization.scheduler import InProcessExecutor, Scheduler
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


class _Cadence:
    """batch_delay and min_interval, as the scheduler's readiness policy."""
    def __init__(self) -> None:
        self._pending_since: dict[str, float] = {}
        self._last_run: dict[str, float] = {}

    def ready(self, binding, application, now: float) -> bool:
        first = self._pending_since.setdefault(binding.signature, now)
        # Delay from the first pending change, then enforce the rate cap.
        min_interval = _duration(application.min_interval) if application.min_interval is not None else None
        if min_interval is not None and now - self._last_run.get(binding.signature, 0) < min_interval.total_seconds():
            return False
        return now - first >= _duration(application.batch_delay).total_seconds()

    def ran(self, binding, executed: bool, progressed: bool, now: float) -> None:
        # Skipping unrelated writes must not start the rate-limit timer.
        if executed:
            self._last_run[binding.signature] = now
        if executed or progressed:
            self._pending_since.pop(binding.signature, None)


class Materializer:
    """Manage a plan and evaluate it inside the engine's steps.

    Lock order, when combined, is coordinator -> plan -> backend write lock.
    The coordinator lock serializes steps, reprocessing requests, and executor
    replacement. The reentrant plan lock protects compilation and plan swaps;
    a step takes its plan snapshot before opening the backend transaction.
    Deployment updates only need the plan and backend locks.

    A step holds the backend lock for its whole transaction, so a generation
    cannot be revoked while its transforms run; publication still checks
    active generations and progress in the same transaction as its writes.
    """
    def __init__(self, store: Any, graph: Any, *, query_resolver=None, record_resolver=None,
                 unit_converter=None, max_workers: int = 2) -> None:
        self._store, self._graph = store, graph
        self._planner = BindingPlanner(graph, query_resolver=query_resolver, record_resolver=record_resolver)
        self._revisions = RevisionStore(store, unit_converter=unit_converter)
        self._scheduler = Scheduler(self._revisions, InProcessExecutor(), max_workers=max_workers)
        self._cadence = _Cadence()
        self.changes = ChangeFeed()
        self.engine = Engine(store, evaluator=self)
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
        with store._lock, store._write_conn() as conn:
            self._execute(conn, "CREATE TABLE IF NOT EXISTS materialization_deployments (name VARCHAR PRIMARY KEY, deployment_json VARCHAR NOT NULL)")
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_lineage (
                binding_signature VARCHAR NOT NULL, progress_key VARCHAR NOT NULL,
                application_name VARCHAR NOT NULL,
                executable_digest VARCHAR NOT NULL, input_alias VARCHAR NOT NULL,
                input_ref_uri VARCHAR NOT NULL, output_name VARCHAR NOT NULL,
                output_ref_uri VARCHAR NOT NULL, context_hash VARCHAR NOT NULL,
                PRIMARY KEY (binding_signature, input_alias, input_ref_uri, output_name))""")
            # Bindings as data: one row per interval in which the model said a
            # binding existed. An open row (valid_to NULL) is a current binding.
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_bindings (
                progress_key VARCHAR NOT NULL, binding_signature VARCHAR NOT NULL,
                application_name VARCHAR NOT NULL, context_hash VARCHAR NOT NULL,
                graph_revision BIGINT NOT NULL, valid_from VARCHAR, valid_to VARCHAR,
                definition VARCHAR NOT NULL)""")
            # Graph writes waiting to be planned, with the time the author
            # said the change took effect.
            self._execute(conn, """CREATE TABLE IF NOT EXISTS materialization_graph_changes (
                graph_revision BIGINT NOT NULL, effective_at VARCHAR NOT NULL, recorded_at VARCHAR NOT NULL)""")

    def _execute(self, conn: Any, query: str, params=()):
        if getattr(self._store, "materialization_backend", None) == "postgres":
            query = query.replace("?", "%s")
        return conn.execute(query, list(params))

    def deploy(self, deployment: Deployment) -> None:
        with self._plan_lock:
            deployments = [d for d in self._deployments() if d.name != deployment.name]
            deployments.append(deployment)
            revision = int(self._graph.graph_status().get("published_version", 0))
            # Validate the proposed app together with existing deployments:
            # individually valid apps can still form a cycle or share an output.
            # Persist and revoke only after this succeeds, leaving the active
            # definition intact if import, query, or graph validation fails.
            self._planner.compile(deployments, revision)
            with self._store._lock, self._store._write_conn() as conn:
                self._execute(conn, "INSERT INTO materialization_deployments VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET deployment_json=EXCLUDED.deployment_json", [deployment.name, deployment.to_json()])
                # Renew even if the definition is identical. Redeploying must
                # revoke work from the prior activation without resetting its
                # durable progress or changing its derived stream identities.
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
            # A removed app has no history to maintain; a later deployment
            # under the same name starts over.
            self._execute(conn, "DELETE FROM materialization_bindings WHERE application_name=?", [name])
            self._revoke(name)
            self._generations.pop(name, None)
            self._graph_revision = -1

    def close(self) -> None:
        with self._coordinator_lock:
            self._scheduler.close()
            self.changes.close()

    def configure_workers(self, count: int) -> None:
        with self._coordinator_lock:
            self._scheduler.close()
            self._scheduler = Scheduler(self._revisions, max_workers=count)

    def failures(self) -> dict[str, str]:
        errors = dict(self._plan_errors)
        errors.update({f"trigger[{ref}]": error for ref, error in self.changes.trigger_errors.items()})
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

    def note_graph_change(self, graph_revision: int, effective_at: datetime | None = None) -> datetime:
        """Record that the model changed, effective at ``effective_at`` (default: now).

        The next plan refresh consumes every recorded change and applies the
        earliest effective time to the bindings that appear or disappear.
        Durable, so a crash between the graph write and the refresh keeps
        the author's time.
        """
        effective = (effective_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        with self._store._lock, self._store._write_conn() as conn:
            self._execute(conn, "INSERT INTO materialization_graph_changes VALUES (?, ?, ?)",
                          [int(graph_revision), effective.isoformat(), datetime.now(timezone.utc).isoformat()])
        self._graph_revision = -1
        return effective

    @contextmanager
    def graph_change(self, effective_at: datetime | None = None):
        """Wrap a graph write so its effective time is recorded before any plan sees it.

        The plan lock is held across the write and the record. Without it the
        query cache can publish the write and a refresh can plan the bindings
        it creates before their effective time exists, and they would default
        to covering all retained history. Nothing is recorded if the write
        raises.
        """
        with self._plan_lock:
            yield
            revision = int(self._graph.graph_status().get("source_version", 0))
            self.note_graph_change(revision, effective_at)

    def _deployments(self) -> tuple[Deployment, ...]:
        with self._store._own_conn() as conn:
            return tuple(Deployment.from_json(row[0]) for row in self._execute(conn, "SELECT deployment_json FROM materialization_deployments ORDER BY name").fetchall())

    def refresh(self) -> None:
        """Reconcile the plan with persisted deployments and the graph version.

        Compile each deployment independently so an unavailable module or failed
        query does not remove healthy apps. Validate the combined DAG before
        installing it. SQL lineage records both output ownership and query
        context, which lets restart detect repairs needed by membership changes.
        """
        with self._plan_lock:
            revision = int(self._graph.graph_status().get("published_version", 0))
            if revision == self._graph_revision: return
            bindings, applications, self._plan_errors = [], {}, {}
            for deployment in self._deployments():
                try:
                    partial, loaded = self._planner.compile((deployment,), revision)
                    # Signature covers code and policy; the context hash covers
                    # the match row (or the whole table for an aggregate).
                    # Either changing must invalidate old publications.
                    generation = self._generations.setdefault(deployment.name, uuid4().hex)
                    bindings.extend(replace(b, generation=f"{generation}:{b.signature}:{b.context_hash}") for b in partial.bindings)
                    applications.update(loaded)
                except Exception as error:
                    self._plan_errors[deployment.name] = f"{type(error).__name__}: {error}"
                    # Retain the last compiled plan for this deployment and
                    # expose the failure instead of silently dropping its
                    # bindings when a query or import temporarily fails.
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
                table = self._binding_rows(conn)
                # Only changes this plan can see: the query cache that the
                # plan compiled against covers graph revisions up to
                # ``revision``. A change written after that waits for the
                # refresh that sees it, or its effective time would be lost.
                pending = [datetime.fromisoformat(row[0]) for row in self._execute(
                    conn, "SELECT effective_at FROM materialization_graph_changes WHERE graph_revision<=?", [revision]).fetchall()]
            open_rows = {r["key"]: r for r in table if r["valid_to"] is None}
            # When the model changed, the author said when (default: now).
            # Bindings that appear or disappear exist from, or until, that
            # time; the earliest pending time covers every change planned now.
            # A bucketed view owns whole buckets, so its boundary is the start
            # of the bucket containing the effective time.
            effective = min(pending) if pending else datetime.now(timezone.utc)
            deployments = {d.name: d for d in self._deployments()}
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            def boundary(app: str) -> datetime:
                every = deployments[app].every if app in deployments else None
                return effective if not every else epoch + ((effective - epoch) // every) * every
            apps_with_rows = {r["app"] for r in open_rows.values()}
            # A binding that appears because the model changed exists from the
            # effective time. One that appears because the dataflow changed
            # (a view deployed, an upstream view removed or redeployed) covers
            # retained history, subject to backfill, as does every binding of
            # a newly deployed app. A binding that is still here keeps the
            # validity it was given when it appeared.
            current_bindings = []
            for b in dag.bindings:
                if b.valid_to is not None:
                    continue   # a failed app's retained historical binding; rebuilt below
                if b.progress_key in open_rows:
                    since = open_rows[b.progress_key]["valid_from"]
                elif b.application_name in apps_with_rows and pending:
                    since = boundary(b.application_name)
                else:
                    since = None
                current_bindings.append(replace(b, valid_from=since))
            current = {b.progress_key: b for b in current_bindings}
            failed_apps = set(self._plan_errors)
            # Bindings closed by a model change stay in the plan over their own
            # interval, so a late reading before the change still reaches the
            # result as the model was then. Those closing now close at the
            # boundary. A binding that disappears because the dataflow changed
            # has no interval to maintain and is dropped.
            closing = {key: boundary(row["app"]) for key, row in open_rows.items()
                       if pending and key not in current and row["app"] not in failed_apps}
            dataflow_dropped = self._dataflow_dropped(closing, table, current, deployments)
            closing = {key: until for key, until in closing.items() if key not in dataflow_dropped}
            table = [row for row in table if row["key"] not in dataflow_dropped or row["valid_to"] is None]
            historical = self._historical(table, closing, current, deployments, failed_apps,
                                          applications, revision)
            # A current binding whose interval overlaps a historical owner of
            # the same output appeared through a dataflow change and covers
            # that history itself, as it is repaired over it: it supersedes.
            superseded = {b.progress_key for b in historical for c in current_bindings
                          if b.overlaps(c) and {p.ref_uri for p in b.outputs.values()} & {p.ref_uri for p in c.outputs.values()}}
            historical = [b for b in historical if b.progress_key not in superseded]
            dataflow_dropped = dataflow_dropped | superseded
            for b in historical:
                applications.setdefault(b.signature, self._instance(b.application_name, applications, deployments))
            try:
                dag = ApplicationGraph([*current_bindings, *historical])
            except ValueError as error:
                self._plan_errors["graph"] = str(error)
                return
            applications = {b.signature: applications[b.signature] for b in dag.bindings}
            # A stable named output may now have different inputs; a binding's
            # own match row may have changed while its inputs did not. Both can
            # alter historical results. Compare with persisted lineage rather
            # than the old in-memory plan so restart detects this too. A change
            # to other rows of the fleet leaves a per-match binding alone.
            repair = [b for b in current_bindings if
                      any(p.ref_uri in prior_outputs and prior_outputs[p.ref_uri] != b.progress_key for p in b.outputs.values())
                      or (prior_context.get(b.progress_key) and prior_context[b.progress_key] != b.context_hash)]
            # A context change repairs from the effective time on; results
            # before it stand as what the old model said.
            repair_since = effective if pending else None
            cascade = bool(pending)
            # Materialization-owned provenance is a complete projection of the
            # current plan. Publish it only when the projection actually changed:
            # lineage writes advance the graph's published_version, so publishing
            # on every refresh would self-trigger a perpetual recompile loop.
            signatures = frozenset(binding.signature for binding in current_bindings)
            republished = signatures != self._lineage_signatures
            if republished:
                self._publish_graph_lineage(current_bindings)
                self._lineage_signatures = signatures
            # Schedule repair before recording the new context fingerprint.
            # A crash can repeat a repair, but cannot forget that it is needed.
            with self._store._lock:
                self._revisions.active_bindings = {b.progress_key: b.generation for b in dag.bindings}
            for binding in repair:
                window = self._revisions.retained_window(binding)
                if window is not None and repair_since is not None and binding.valid_from is None:
                    since = boundary(binding.application_name)
                    window = TimeWindow(max(window.start, since), window.end) if window.end >= since else None
                if window is None:
                    continue
                self._revisions.initialise(binding, applications[binding.signature].backfill)
                with self._store._lock, self._store._write_conn() as conn:
                    self._execute(conn, "DELETE FROM materialization_work WHERE progress_key=?", [binding.progress_key])
                self._revisions.request_reprocess([binding], window)
            with self._store._lock, self._store._write_conn() as conn:
                self._revisions.active_bindings = {b.progress_key: b.generation for b in dag.bindings}
                # Bindings table: close what disappeared (and tombstone its
                # outputs from the effective time), open what appeared, and
                # keep a changed context as a closed-and-reopened row.
                retired: dict[datetime, list[str]] = {}
                still_owned = {p.ref_uri for b in current_bindings for p in b.outputs.values()}
                for key, row in open_rows.items():
                    if key in current and current[key].context_hash == row["context"]:
                        if current[key].signature != row["signature"]:
                            self._execute(conn, "UPDATE materialization_bindings SET binding_signature=? WHERE progress_key=? AND valid_to IS NULL",
                                          [current[key].signature, key])
                        continue
                    if key not in current and row["app"] in failed_apps:
                        continue   # its app failed to compile; keep its bindings open until it recovers
                    until = boundary(row["app"])
                    if key not in current and (not pending or key in dataflow_dropped):
                        self._execute(conn, "DELETE FROM materialization_bindings WHERE progress_key=?", [key])
                        continue
                    self._execute(conn, "UPDATE materialization_bindings SET valid_to=? WHERE progress_key=? AND valid_to IS NULL", [until.isoformat(), key])
                    if key not in current:
                        # Its outputs, from the stored definition, so a restart retires them too.
                        retired.setdefault(until, []).extend(
                            ref for ref, _, _ in row["definition"]["outputs"].values() if ref not in still_owned)
                for b in current_bindings:
                    row = open_rows.get(b.progress_key)
                    if row is None or row["context"] != b.context_hash:
                        self._execute(conn, "INSERT INTO materialization_bindings VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                                      [b.progress_key, b.signature, b.application_name, b.context_hash, revision,
                                       b.valid_from.isoformat() if b.valid_from else None, self._definition(b)])
                for until, refs in retired.items():
                    self._revisions.retire(refs, until, conn=conn)
                for key in dataflow_dropped:
                    self._execute(conn, "DELETE FROM materialization_bindings WHERE progress_key=? AND valid_to IS NOT NULL", [key])
                self._execute(conn, "DELETE FROM materialization_graph_changes WHERE graph_revision<=?", [revision])
                if cascade and republished:
                    # The derived points just published are what a downstream
                    # query matches, so the bindings that appear or disappear
                    # at the refresh that sees them belong to this same change.
                    published_source = int(self._graph.graph_status().get("source_version", revision))
                    if published_source > revision:
                        self._execute(conn, "INSERT INTO materialization_graph_changes VALUES (?, ?, ?)",
                                      [published_source, effective.isoformat(), datetime.now(timezone.utc).isoformat()])
                failed = list(self._plan_errors)
                if failed:
                    marks = ','.join('?' for _ in failed)
                    self._execute(conn, f"DELETE FROM materialization_lineage WHERE application_name NOT IN ({marks})", failed)
                else:
                    self._execute(conn, "DELETE FROM materialization_lineage")
                for binding in current_bindings:
                    rows = [(binding.signature, binding.progress_key, binding.application_name, binding.executable_digest,
                        alias, stream.ref_uri, output_name, output_ref, binding.context_hash)
                        for alias, streams in binding.inputs.items() for stream in streams
                        for output_name, output in binding.outputs.items()
                        for output_ref in (output.ref_uri,)]
                    if not rows:
                        # Empty aggregates retain output ownership across restart.
                        # Empty alias/ref fields represent no input edge in SQL.
                        rows = [(binding.signature, binding.progress_key, binding.application_name,
                                 binding.executable_digest, "", "", name, port.ref_uri,
                                 binding.context_hash)
                                for name, port in binding.outputs.items()]
                    if getattr(self._store, "materialization_backend", None) == "postgres":
                        with conn.cursor() as cur:
                            cur.executemany("INSERT INTO materialization_lineage VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)
                    else:
                        conn.executemany("INSERT OR REPLACE INTO materialization_lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            self._dag, self._applications, self._graph_revision = dag, applications, revision

    # ---- bindings as data ----

    @staticmethod
    def _definition(binding) -> str:
        """What is needed to rebuild a binding after it leaves the current plan."""
        return json.dumps({
            "inputs": {alias: [{**d.__dict__, "properties": {k: list(v) for k, v in d.properties.items()}}
                               for d in streams] for alias, streams in binding.inputs.items()},
            "outputs": {name: [port.ref_uri, port.ref_name, port.point_uri] for name, port in binding.outputs.items()},
            "row": binding.row, "result": list(binding.result),
        }, sort_keys=True, default=str)

    def _binding_rows(self, conn) -> list[dict[str, Any]]:
        rows = self._execute(conn, "SELECT progress_key, binding_signature, application_name, context_hash, "
                                   "valid_from, valid_to, definition FROM materialization_bindings").fetchall()
        when = lambda text: datetime.fromisoformat(text) if text else None
        return [{"key": key, "signature": sig, "app": app, "context": ctx, "valid_from": when(since),
                 "valid_to": when(until), "definition": json.loads(definition)}
                for key, sig, app, ctx, since, until, definition in rows]

    def _dataflow_dropped(self, closing, table, current, deployments) -> set[str]:
        """Bindings, closing now or closed before, whose input nothing produces any more.

        A binding whose input is a derived stream with no owner left exists
        only because a view upstream was removed, not because the model
        changed, even when a model change is planned in the same refresh. It
        has no interval to maintain. Applied to a fixed point over closing
        and closed bindings together, so a chain below a removed view goes.
        """
        rows = {}
        for row in table:
            key = row["key"]
            if key in current or row["app"] not in deployments:
                continue
            if (row["valid_to"] is None and key in closing) or row["valid_to"] is not None:
                rows.setdefault(key, row)
        if not rows:
            return set()
        with self._store._own_conn() as conn:
            derived = {row[0] for row in self._execute(
                conn, "SELECT ref_uri FROM streams WHERE source_id LIKE ?", ["derived:%"]).fetchall()}
        refs = lambda row, side: ({d["ref_uri"] for ds in row["definition"]["inputs"].values() for d in ds}
                                  if side == "in" else {ref for ref, _, _ in row["definition"]["outputs"].values()})
        owned = {p.ref_uri for b in current.values() for p in b.outputs.values()}
        kept = set(rows)
        while True:
            alive = owned | {ref for key in kept for ref in refs(rows[key], "out")}
            drop = {key for key in kept if any(ref in derived and ref not in alive for ref in refs(rows[key], "in"))}
            if not drop:
                return set(rows) - kept
            kept -= drop

    def _instance(self, app_name: str, applications: dict[str, Any], deployments: dict[str, Deployment]):
        for binding in self._dag.bindings:
            if binding.application_name == app_name and binding.signature in applications:
                return applications[binding.signature]
        return self._planner.instantiate(deployments[app_name])

    def _historical(self, table, closing, current, deployments, failed_apps, applications, revision) -> list:
        """Closed bindings of deployed apps, each over its own interval of validity.

        A progress key that is current again is not rebuilt: the current
        binding covers it. A key closed more than once keeps its latest
        interval. A failed app keeps whatever its last plan held.
        """
        from acquirium.Materialization.models import Binding, OutputPort, StreamDescriptor
        latest: dict[str, dict[str, Any]] = {}
        for row in table:
            until = row["valid_to"] if row["valid_to"] is not None else closing.get(row["key"])
            if until is None or row["key"] in current or row["app"] not in deployments or row["app"] in failed_apps:
                continue
            if row["valid_from"] is not None and row["valid_from"] >= until:
                continue
            if row["key"] not in latest or until > latest[row["key"]]["until"]:
                latest[row["key"]] = {**row, "until": until}
        rebuilt = []
        for key, row in latest.items():
            deployment, definition = deployments[row["app"]], row["definition"]
            app = self._instance(row["app"], applications, deployments)
            inputs = {alias: tuple(StreamDescriptor(**{**d, "properties": {k: tuple(v) for k, v in d.get("properties", {}).items()}})
                                   for d in streams) for alias, streams in definition["inputs"].items()}
            outputs = {name: OutputPort(ref, ref_name, point, deployment.outputs[name])
                       for name, (ref, ref_name, point) in definition["outputs"].items() if name in deployment.outputs}
            if not inputs or not outputs:
                continue
            binding = Binding(row["app"], deployment.executable_digest, inputs, outputs,
                              deployment.lookback, deployment.lookahead, revision, deployment.parameters,
                              definition["row"], tuple(definition["result"]), every=deployment.every,
                              custom_window=app.output_window if type(app).output_window is not App.output_window else None,
                              valid_from=row["valid_from"], valid_to=row["until"])
            generation = self._generations.setdefault(row["app"], uuid4().hex)
            rebuilt.append(replace(binding, generation=f"{generation}:{binding.signature}:{binding.context_hash}"))
        for failed in failed_apps:
            rebuilt.extend(b for b in self._dag.bindings if b.application_name == failed and b.valid_to is not None)
        return rebuilt

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
        """Run one step: apply the inbox and evaluate the plan in one transaction."""
        if not self._coordinator_lock.acquire(blocking=False):
            return False
        try:
            return self.engine.step() is not None
        finally:
            self._coordinator_lock.release()

    # Evaluator protocol for Engine.step().

    def prepare(self) -> tuple[ApplicationGraph, dict[str, Any]]:
        self._revisions.capture = []
        return self._plan_snapshot()

    def evaluate(self, conn: Any, plan: tuple[ApplicationGraph, dict[str, Any]]) -> bool:
        dag, applications = plan
        return self._scheduler.evaluate(conn, dag, applications, policy=self._cadence)

    def committed(self, report) -> None:
        captured, self._revisions.capture = self._revisions.capture or [], None
        if captured:
            self.changes.publish(StepChanges(report.step_no, report.revision, tuple(captured)))

    def dag(self) -> dict[str, Any]:
        """Combine durable progress and process-local diagnostics for display.

        These reads are observational, not one transactional scheduling snapshot.
        A global revision gap may come from unrelated streams; "pending" does
        not prove that this binding has input changes requiring a transform.
        """
        dag, applications = self._plan_snapshot()
        return self._dag_status(dag, applications)

    def list_apps(self) -> list[dict[str, Any]]:
        """Summarize every registered deployment without running or replanning it."""
        return self._inspect_deployments()

    def inspect_app(self, name: str) -> dict[str, Any]:
        """Describe a deployment and its latest compiled bindings, without writes."""
        return self._inspect_deployments(name)[0]

    def _inspect_deployments(self, name: str | None = None) -> list[dict[str, Any]]:
        with self._plan_lock:
            deployments = [d for d in self._deployments() if name is None or d.name == name]
            if name is not None and not deployments:
                raise KeyError(name)
            snapshot = self._dag_status(self._dag, self._applications)
            plan_current = self._graph_revision == int(self._graph.graph_status().get("published_version", 0))
            by_app: dict[str, list[dict[str, Any]]] = {}
            for node in snapshot["nodes"]:
                by_app.setdefault(node["application_name"], []).append(node)
            results = []
            for deployment in deployments:
                bindings = by_app.get(deployment.name, [])
                counts: dict[str, int] = {}
                for binding in bindings:
                    state = binding["status"]
                    counts[state] = counts.get(state, 0) + 1
                error = snapshot["errors"].get(deployment.name) or snapshot["errors"].get("graph")
                status = ("failed" if error else "planning" if not plan_current else
                          next((s for s in ("failed", "running", "reprocessing", "waiting", "pending", "idle")
                                if s in counts), "no_matches"))
                result = {"name": deployment.name, "entrypoint": deployment.entrypoint,
                          "grouping": deployment.grouping, "status": status,
                          "binding_count": len(bindings), "binding_statuses": counts,
                          "graph_revision": snapshot["graph_revision"], "plan_current": plan_current,
                          "error": error}
                if name is not None:
                    result["definition"] = json.loads(deployment.to_json())
                    result["output_schemas"] = {
                        key: {"time": {"type": "timestamp[us, tz=UTC]", "nullable": False},
                              "value": {"type": "float64" if spec.value_kind == "numeric" else "string",
                                        "nullable": False}}
                        for key, spec in deployment.outputs.items()}
                    result["bindings"] = bindings
                results.append(result)
            return results

    def _dag_status(self, dag: ApplicationGraph, applications: dict[str, Any]) -> dict[str, Any]:
        """Read progress for a captured plan; do not advance the coordinator."""
        current = self._revisions.current_revision()
        pending_work = self._revisions.pending_keys()
        with self._store._own_conn() as conn:
            progress = dict(self._execute(conn, "SELECT progress_key, consumed_revision FROM binding_progress").fetchall())
        blocked = set(self._scheduler.errors)
        for wave in dag.layers():
            blocked.update(target for source, target, _ in dag.edges if source in blocked)
        nodes = []
        for binding in dag.bindings:
            consumed = progress.get(binding.progress_key)
            nodes.append({"binding_signature": binding.signature, "application_name": binding.application_name,
                "inputs": {key: [item.ref_uri for item in value] for key,value in binding.inputs.items()},
                "outputs": {key: output.ref_uri for key, output in binding.outputs.items()},
                "lookback": "all" if binding.lookback is None else str(binding.lookback),
                "backfill": bool(applications[binding.signature].backfill),
                "consumed_revision": consumed, "current_revision": current,
                "status": ("running" if binding.signature in self._scheduler.running else
                           "failed" if binding.signature in self._scheduler.errors else
                           "waiting" if binding.signature in blocked else
                           "reprocessing" if binding.progress_key in pending_work else
                           "pending" if consumed is None or consumed < current else "idle"),
                "last_success": self._scheduler.last_success.get(binding.signature),
                "error": self._scheduler.errors.get(binding.signature)})
        return {"graph_revision": self._graph_revision, "nodes": nodes, "errors": dict(self._plan_errors),
                "edges": [{"source": source, "target": target, "ref_uri": ref} for source,target,ref in dag.edges]}
