"""Bounded local execution and dependency failure isolation."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime, timezone
from typing import Iterable, Mapping, Protocol
import pyarrow as pa
from acquirium.Materialization.models import App, ApplicationGraph, Batch, Binding, OutputBuilder, OutputPort
from acquirium.Materialization.revision_store import RevisionStore

class Executor(Protocol):
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]: ...


class InProcessExecutor:
    """Deterministic executor useful for tests; it has the same task boundary."""
    def execute(self, application: App, batch: Batch,
                ports: Mapping[str, OutputPort]) -> Mapping[str, pa.Table]:
        output = OutputBuilder(ports)
        application.transform(batch.inputs, output, batch.context)
        return output.values


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

    def run_once(self, binding: Binding, application: App) -> bool:
        self.store.initialise(binding, application.backfill)
        batch = self.store.next_batch(binding)
        if batch is None:
            return False
        results = self.executor.execute(application, batch, binding.outputs)
        return self.store.commit(binding, batch, results)

    def run_layer(self, bindings: Iterable[Binding], applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
        # A chunk bounds both loaded batches and pending futures. All successes
        # in it publish together before dependent work can be scheduled.
        capacity = self.capacity if max_workers is None else min(self.capacity, max_workers)
        if capacity < 1:
            raise ValueError("max_workers must be positive")
        wave, ran = tuple(bindings), False
        with self._run_lock:
            for offset in range(0, len(wave), capacity):
                pending = []
                for binding in wave[offset:offset + capacity]:
                    self.errors.pop(binding.signature, None)
                    try:
                        self.store.initialise(binding, applications[binding.signature].backfill)
                        batch = self.store.next_batch(binding)
                        if batch is not None:
                            self.running.add(binding.signature)
                            future = self._pool.submit(self.executor.execute, applications[binding.signature], batch, binding.outputs)
                            pending.append((binding, batch, future))
                    except Exception as error:
                        self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                completed = []
                for binding, batch, future in pending:
                    try:
                        completed.append((binding, batch, future.result()))
                    except Exception as error:
                        self.errors[binding.signature] = f"{type(error).__name__}: {error}"
                    finally:
                        self.running.discard(binding.signature)
                accepted = self.store.commit_wave(completed)
                for signature in accepted:
                    self.last_success[signature] = datetime.now(timezone.utc).isoformat()
                ran = any(accepted.values()) or ran
        return ran

    def run_graph_once(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> bool:
        ran, blocked = False, set()
        for wave in graph.layers():
            blocked.update(target for source, target, _ in graph.edges if source in blocked)
            ready = [b for b in wave if b.signature not in blocked]
            ran = self.run_layer(ready, applications, max_workers=max_workers) or ran
            blocked.update(b.signature for b in ready if b.signature in self.errors)
        return ran

    def run_until_idle(self, graph: ApplicationGraph, applications: Mapping[str, App], *, max_workers: int | None = None) -> None:
        while self.run_graph_once(graph, applications, max_workers=max_workers):
            pass
