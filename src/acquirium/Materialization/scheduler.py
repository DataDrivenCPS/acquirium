"""Evaluate a plan inside one transaction, layer by layer.

A step hands the scheduler its write connection. For each dependency layer
the scheduler reads one change index, decides which bindings have something
to do, reads their batches on that connection, runs the transforms in a
bounded pool, and publishes the accepted results on the same connection.
The next layer's index then already contains those outputs. Nothing is
visible outside the transaction until the step commits, and a database
failure anywhere rolls the whole step back.

A transform failure is different: it is recorded per binding, its
descendants are skipped for this step, and everything else proceeds. The
scheduler keeps that process-local record (errors, running, last success);
durable progress lives in RevisionStore.

Standalone use (tests, tooling) wraps the same evaluation in a transaction
of its own through ``run_graph_once``.
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
from time import monotonic
from typing import Any, Iterable, Mapping, Protocol
import pyarrow as pa
from acquirium.Materialization.models import App, ApplicationGraph, Batch, Binding, OutputBuilder, OutputPort
from acquirium.Materialization.revision_store import RevisionStore


class Executor(Protocol):
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]: ...


class InProcessExecutor:
    """Call application code on loaded data using the shared output validator.

    This is the production in-process execution boundary as well as the default
    for standalone schedulers. Determinism is the app's responsibility; the
    executor neither retries side effects nor enforces purity.
    """
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]:
        output = OutputBuilder(ports)
        application.transform(batch.inputs, output, batch.context)
        return output.values


class Policy(Protocol):
    """When a binding with pending input may run, and what to remember after."""
    def ready(self, binding: Binding, application: App, now: float) -> bool: ...
    def ran(self, binding: Binding, executed: bool, progressed: bool, now: float) -> None: ...


class RunWhenTouched:
    """The default: anything with changed input or pending work runs now."""
    def ready(self, binding: Binding, application: App, now: float) -> bool:
        return True

    def ran(self, binding: Binding, executed: bool, progressed: bool, now: float) -> None:
        return None


class Scheduler:
    """A persistent bounded executor, with failures recorded per binding."""
    def __init__(self, store: RevisionStore, executor: Executor | None = None, *, max_workers: int = 2):
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self.store, self.executor = store, executor or InProcessExecutor()
        self.capacity = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="acquirium-materialize")
        self.errors: dict[str, str] = {}
        self.running: set[str] = set()
        self.last_success: dict[str, str] = {}
        self._run_lock = Lock()

    def close(self) -> None:
        self._pool.shutdown(wait=True)

    def evaluate(self, conn: Any, graph: ApplicationGraph, applications: Mapping[str, App], *,
                 policy: Policy | None = None, max_workers: int | None = None) -> bool:
        """Run every layer of ``graph`` on ``conn``; True if anything published.

        ``conn`` is a write connection inside an open transaction, held by
        the caller, who also holds the backend lock. A transform failure
        blocks only its descendants; a database failure propagates and the
        caller's transaction rolls back.
        """
        policy = policy or RunWhenTouched()
        store = self.store
        ran, blocked = False, set()
        self.timings = {"read": 0.0, "transform": 0.0, "publish": 0.0}
        # Explicit repairs can be pending even when their input frontier is
        # current. Readiness must account for durable work as well as revisions.
        pending_work = store.pending_keys(conn)
        index, index_floor = None, 0
        with self._run_lock:
            for wave in graph.layers():
                blocked.update(target for source, target, _ in graph.edges if source in blocked)
                now = monotonic()
                current, progress = store.progress_snapshot(conn)
                # A binding behind a failed producer is not initialised: it
                # has no frontier to advance until its producer recovers.
                live = [b for b in wave if b.signature not in blocked]
                for binding in live:
                    if progress.get(binding.progress_key) is None:
                        progress[binding.progress_key] = store.initialise(
                            binding, applications[binding.signature].backfill, conn=conn)
                # One indexed scan tells which streams changed since the oldest
                # frontier in this wave. A binding whose inputs are absent from
                # it has nothing to read: its frontier moves in one batched
                # update. The index is rebuilt after a wave publishes, so a
                # dependent wave sees its predecessors' outputs.
                floor = min((progress[b.progress_key] for b in live), default=current)
                if index is None or floor < index_floor or index[0] < current:
                    index, index_floor = store.change_index(floor, conn=conn), floor
                target, changed, resets = index
                ready, previous, idle = [], {}, []
                for binding in live:
                    consumed = progress[binding.progress_key]
                    refs = [d.ref_uri for values in binding.inputs.values() for d in values]
                    touched = any(changed.get(ref, 0) > consumed or resets.get(ref, 0) > consumed for ref in refs)
                    if not touched and binding.progress_key not in pending_work:
                        if consumed < target:
                            idle.append((binding, consumed, target))
                        policy.ran(binding, False, True, now)
                        continue
                    if not policy.ready(binding, applications[binding.signature], now):
                        continue
                    ready.append(binding)
                    previous[binding.signature] = consumed
                store.advance_idle(idle, conn=conn)
                if not ready:
                    blocked.update(b.signature for b in wave if b.signature in self.errors)
                    continue
                successes = {b.signature: self.last_success.get(b.signature) for b in ready}
                published = self._run_layer(conn, ready, applications, max_workers)
                ran = published or ran
                if published:
                    index = None
                blocked.update(b.signature for b in ready if b.signature in self.errors)
                _, progressed = store.progress_snapshot(conn)
                for binding in ready:
                    # A frontier advance alone is not a successful calculation.
                    # A committed chunk can also succeed without advancing its
                    # frontier, so use the publication record here.
                    executed = successes[binding.signature] != self.last_success.get(binding.signature)
                    policy.ran(binding, executed, progressed.get(binding.progress_key, 0) > previous[binding.signature], now)
        return ran

    def _run_layer(self, conn: Any, bindings: Iterable[Binding], applications: Mapping[str, App],
                   max_workers: int | None) -> bool:
        # The pool bounds running transforms; chunking before the reads bounds
        # the number of loaded Arrow batches held at once. Reads run on the
        # step's single connection, so they are sequential; transforms run in
        # parallel on loaded data.
        capacity = self.capacity if max_workers is None else min(self.capacity, max_workers)
        if capacity < 1:
            raise ValueError("max_workers must be positive")
        wave, ran = tuple(bindings), False
        timings = self.timings
        for offset in range(0, len(wave), capacity):
            loaded = []
            reading = monotonic()
            for binding in wave[offset:offset + capacity]:
                self.errors.pop(binding.signature, None)
                try:
                    batch = self.store.next_batch(binding, conn=conn)
                except Exception as error:
                    self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                    continue
                if batch is not None:
                    loaded.append((binding, batch))
            timings["read"] += monotonic() - reading
            transforming = monotonic()
            pending = []
            for binding, batch in loaded:
                self.running.add(binding.signature)
                pending.append((binding, batch, self._pool.submit(
                    self.executor.execute, applications[binding.signature], batch, binding.outputs)))
            completed = []
            for binding, batch, future in pending:
                try:
                    completed.append((binding, batch, future.result()))
                except Exception as error:
                    self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                finally:
                    self.running.discard(binding.signature)
            timings["transform"] += monotonic() - transforming
            publishing = monotonic()
            # A transform failure omits only that binding from publication.
            # A database failure propagates: the caller's transaction, and
            # with it this whole step, rolls back.
            accepted = self.store.commit_wave(completed, conn=conn)
            timings["publish"] += monotonic() - publishing
            for signature in accepted:
                self.last_success[signature] = datetime.now(timezone.utc).isoformat()
            ran = any(accepted.values()) or ran
        return ran

    def run_graph_once(self, graph: ApplicationGraph, applications: Mapping[str, App], *,
                       max_workers: int | None = None, policy: Policy | None = None) -> bool:
        """Evaluate ``graph`` as one step in a transaction of its own."""
        backend = self.store.store
        with backend._lock, backend._write_conn() as conn:
            return self.evaluate(conn, graph, applications, policy=policy, max_workers=max_workers)

    def run_once(self, binding: Binding, application: App) -> bool:
        return self.run_graph_once(ApplicationGraph([binding]), {binding.signature: application})

    def run_until_idle(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> None:
        while self.run_graph_once(graph, applications, max_workers=max_workers):
            pass
