"""Stateless orchestration for topology-epoch control-plane work."""
from __future__ import annotations

from datetime import timedelta
from threading import Event, Thread
from typing import Callable

from acquirium.Materialization.context import ComputeRequest, TransformContext
from acquirium.Materialization.executor import LocalExecutorPool


class TopologyEpochReconciler:
    """Drive construction, execution, sealing, and recovery one claim at a time."""

    def __init__(self, storage, graph: object, executor=None, *, artifact_loader: Callable[[str], bytes] | None = None,
                 lease_duration: timedelta = timedelta(minutes=5)) -> None:
        if lease_duration <= timedelta():
            raise ValueError("lease_duration must be positive")
        self._storage = storage
        self._graph = graph
        self._executor = executor or LocalExecutorPool()
        self._artifact_loader = artifact_loader
        self._lease_duration = lease_duration

    def ensure_graph_epoch(self, graph_revision: int, graph_digest: str) -> str:
        return self._storage.ensure_epoch(graph_revision, graph_digest, self._graph)

    def _construct_once(self, owner: str) -> bool:
        epoch_id = self._storage.candidate_epoch_id()
        if epoch_id is None:
            return False
        if self._storage.epoch_summary(epoch_id).status != "constructing":
            return False
        claim = self._storage.claim("construct", epoch_id, owner)
        if claim is None:
            return False
        try:
            self._storage.construct_epoch(epoch_id, self._graph, claim=claim)
        finally:
            try:
                self._storage.release_claim(claim)
            except Exception:
                # A superseding epoch may have already invalidated the claim;
                # claim expiry remains the recovery path in that case.
                pass
        return True

    def _execute_once(self, owner: str) -> bool:
        claim = self._storage.claim_next_work(owner, duration=self._lease_duration)
        if claim is None:
            return False
        heartbeat_stop = Event()
        heartbeat = Thread(
            target=self._renew_while_running,
            args=(claim, heartbeat_stop),
            name=f"acquirium-lease-{claim.target_id[:8]}",
            daemon=True,
        )
        heartbeat.start()
        try:
            snapshot = self._storage.snapshot(claim)
            spec = snapshot.definition.spec
            scalar = isinstance(spec, dict) and spec.get("execution", "batch") == "scalar"
            request = ComputeRequest(
                snapshot.inputs,
                TransformContext(
                    binding_id=snapshot.binding.binding_id,
                    execution_id=f"{snapshot.work.work_id}:{claim.attempt}",
                    write_interval=snapshot.work.write_interval,
                    read_interval=snapshot.work.read_interval,
                    input_versions=snapshot.input_versions,
                    metadata=snapshot.binding.metadata,
                    state_revision=snapshot.binding.state_revision,
                    outputs=snapshot.binding.outputs,
                ),
                frozenset(snapshot.binding.output_refs),
                scalar=scalar,
                artifact_bytes=(self._artifact_loader(snapshot.binding.state_revision)
                                if snapshot.binding.state_revision and self._artifact_loader else None),
            )
            replacement = self._executor.submit_entrypoint(
                digest=snapshot.definition.source_digest,
                entrypoint=snapshot.definition.entrypoint,
                request=request,
            ).result()
            self._storage.commit_work(snapshot, replacement, claim)
        except Exception as error:
            try:
                self._storage.fail_work(claim, {"type": type(error).__name__, "message": str(error)})
            except Exception:
                # If the process lost the claim, expiry/reclaim is the only
                # valid recovery action and never requires manager ordering.
                pass
            raise
        finally:
            heartbeat_stop.set()
            heartbeat.join()
        return True

    def _renew_while_running(self, claim, stop: Event) -> None:
        interval = max(0.01, self._lease_duration.total_seconds() / 3)
        while not stop.wait(interval):
            try:
                self._storage.renew_claim(claim, duration=self._lease_duration)
            except Exception:
                # Commit remains fenced by the durable claim. If ownership was
                # lost, the result is discarded and another worker retries it.
                return

    def _seal_once(self, owner: str) -> bool:
        claim = self._storage.claim_next_component(owner)
        if claim is None:
            return False
        self._storage.seal_component(claim)
        return True

    def run_once(self, owner: str = "manager") -> bool:
        """Perform one durable transition, in construction/work/seal order."""
        if self._construct_once(owner):
            return True
        if self._execute_once(owner):
            return True
        return self._seal_once(owner)

    def run_until_idle(self, owner: str = "manager", *, limit: int = 100_000) -> int:
        if limit < 1:
            raise ValueError("limit must be positive")
        count = 0
        while count < limit and self.run_once(owner):
            count += 1
        return count

    def close(self) -> None:
        close = getattr(self._executor, "close", None)
        if callable(close):
            close()
