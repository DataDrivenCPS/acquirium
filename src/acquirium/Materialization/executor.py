"""Bounded local executor pool shared by all logical materializations."""
from __future__ import annotations
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable
import pyarrow as pa
from acquirium.Materialization.compute import PythonArrowAdapter
from acquirium.Materialization.context import ComputeRequest
from acquirium.Materialization.worker import DefinitionCache
from acquirium.Materialization.worker import load_entrypoint

class LocalExecutorPool:
    def __init__(self, workers: int = 2, *, adapter: PythonArrowAdapter | None = None) -> None:
        if workers < 1:
            raise ValueError("executor pool requires at least one worker")
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="acquirium-materialization")
        self._adapter = adapter or PythonArrowAdapter()
        self.definitions = DefinitionCache()
    def submit(self, target: Callable[..., Any], request: ComputeRequest) -> Future[pa.Table]:
        return self._executor.submit(self._adapter.execute, target, request)
    def submit_entrypoint(self, *, digest: str, entrypoint: str, request: ComputeRequest) -> Future[pa.Table]:
        target = self.definitions.load(digest, lambda: load_entrypoint(entrypoint))
        return self.submit(target, request)
    def submit_callable_entrypoint(self, *, digest: str, entrypoint: str, argument: Any) -> Future[Any]:
        """Run bounded non-materialization work from an immutable entrypoint."""
        target = self.definitions.load(digest, lambda: load_entrypoint(entrypoint))
        return self._executor.submit(target, argument)
    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)


class _RayFuture:
    def __init__(self, ref) -> None:
        self._ref = ref

    def result(self) -> pa.Table:
        import ray
        return ray.get(self._ref)


class RayExecutorPool:
    """Fixed-size Ray actor pool; logical bindings never create actors."""
    def __init__(self, workers: int = 2) -> None:
        if workers < 1:
            raise ValueError("executor pool requires at least one worker")
        import ray
        if not ray.is_initialized():
            raise RuntimeError("Ray must be initialized before creating a RayExecutorPool")

        @ray.remote
        class Worker:
            def __init__(self) -> None:
                self.adapter = PythonArrowAdapter()
                self.definitions = DefinitionCache()
            def execute(self, digest: str, entrypoint: str, request: ComputeRequest) -> pa.Table:
                target = self.definitions.load(digest, lambda: load_entrypoint(entrypoint))
                return self.adapter.execute(target, request)
            def call(self, digest: str, entrypoint: str, argument: Any) -> Any:
                target = self.definitions.load(digest, lambda: load_entrypoint(entrypoint))
                return target(argument)
            def clear(self) -> None:
                self.definitions.clear()

        self._workers = [Worker.remote() for _ in range(workers)]
        self._next = 0

    def _pick_worker(self):
        worker = self._workers[self._next % len(self._workers)]
        self._next += 1
        return worker

    def submit_entrypoint(self, *, digest: str, entrypoint: str, request: ComputeRequest) -> _RayFuture:
        return _RayFuture(self._pick_worker().execute.remote(digest, entrypoint, request))

    def submit_callable_entrypoint(self, *, digest: str, entrypoint: str, argument: Any) -> _RayFuture:
        """Run bounded non-materialization work (e.g. an experiment) on the pool."""
        return _RayFuture(self._pick_worker().call.remote(digest, entrypoint, argument))

    def close(self) -> None:
        import ray
        for worker in self._workers:
            ray.kill(worker, no_restart=True)
