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
    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)
