"""Durable reconciliation-plan creation from range manifests and progress."""
from __future__ import annotations
from datetime import timedelta
from typing import Callable, Mapping
import pyarrow as pa
from acquirium.Materialization.impact import ImpactPolicy, TimeRange, coalesce_ranges
from acquirium.Materialization.context import ComputeRequest, TransformContext

class MaterializationScheduler:
    """Stateless planner; durable storage is the authority for all work."""
    def __init__(self, storage) -> None:
        self._storage = storage

    def create_plan_for_binding(self, *, binding_id: str, generation: int, graph_revision: int,
                                progress: Mapping[str, int], heads: Mapping[str, int],
                                impact: ImpactPolicy, reason: dict, retained: TimeRange | None = None,
                                maximum_partition_duration: timedelta = timedelta(minutes=15)):
        ranges: list[TimeRange] = []
        vector = {ref: version for ref, version in heads.items() if version > progress.get(ref, 0)}
        for ref, to_version in vector.items():
            for change in self._storage.change_ranges(ref, after_version=progress.get(ref, 0), through_version=to_version):
                ranges.append(impact.affected(change.interval, retained=retained))
        reason = {**reason, "impact": impact.to_json()}
        return self._storage.create_plan(binding_id=binding_id, generation=generation,
            graph_revision=graph_revision, input_vector=vector, ranges=coalesce_ranges(ranges),
            reason=reason, maximum_partition_duration=maximum_partition_duration)

    def run_once(self, owner: str, execute: Callable[[object, tuple[str, ...]], pa.Table]) -> bool:
        """Execute at most one durable partition; failures remain retryable."""
        lease = self._storage.lease_partition(owner)
        if lease is None:
            return False
        inputs, outputs = self._storage.partition_refs(lease.partition.partition_id)
        try:
            snapshot = self._storage.snapshot_partition(lease, inputs)
            replacement = execute(snapshot, outputs)
            self._storage.commit_replacement(snapshot, input_refs=inputs, output_refs=outputs, replacement=replacement)
        except Exception as error:
            self._storage.fail_partition(lease, {"type": type(error).__name__, "message": str(error)})
            raise
        return True

    def discover_and_plan(self, *, impact: ImpactPolicy, deployment_name: str | None = None,
                          maximum_partition_duration: timedelta = timedelta(minutes=15)) -> tuple[str, ...]:
        """Safety-scan durable progress/head lag and create missing work idempotently."""
        plan_ids = []
        for binding in self._storage.stale_bindings():
            if deployment_name is not None and binding.get("deployment_name") != deployment_name:
                continue
            plan_id, _ = self.create_plan_for_binding(
                binding_id=binding["binding_id"], generation=binding["generation"], graph_revision=binding["graph_revision"],
                progress=binding["progress"], heads=binding["heads"], impact=impact,
                reason={"kind": "safety_scan"}, maximum_partition_duration=maximum_partition_duration,
            )
            plan_ids.append(plan_id)
        return tuple(plan_ids)

    def run_registered_once(self, owner: str, *, executor, source_digest: str, entrypoint: str,
                            scalar: bool, metadata: Mapping[str, object] | None = None) -> bool:
        """Lease and execute one partition using a digest-cached definition."""
        lease = self._storage.lease_partition(owner)
        if lease is None:
            return False
        inputs, outputs = self._storage.partition_refs(lease.partition.partition_id)
        try:
            snapshot = self._storage.snapshot_partition(lease, inputs)
            request = ComputeRequest(snapshot.inputs, TransformContext(
                binding_id=lease.partition.plan_id, execution_id=f"{lease.partition.partition_id}:{lease.attempt}",
                interval=lease.partition.interval, input_versions=snapshot.input_versions, metadata=metadata or {},
            ), frozenset(outputs), scalar=scalar)
            replacement = executor.submit_entrypoint(digest=source_digest, entrypoint=entrypoint, request=request).result()
            self._storage.commit_replacement(snapshot, input_refs=inputs, output_refs=outputs, replacement=replacement)
        except Exception as error:
            self._storage.fail_partition(lease, {"type": type(error).__name__, "message": str(error)})
            raise
        return True

    def run_next_registered(self, owner: str, *, executor) -> bool:
        """Execute next partition using its deployment's persisted definition."""
        lease = self._storage.lease_registered_partition(owner)
        if lease is None:
            return False
        bundle = self._storage.partition_definition(lease.partition.partition_id)
        spec = bundle["spec"]
        outputs = spec.get("outputs") if isinstance(spec, dict) else None
        scalar = isinstance(outputs, dict) and outputs.get("mode") == "per_input"
        inputs, output_refs = self._storage.partition_refs(lease.partition.partition_id)
        try:
            snapshot = self._storage.snapshot_partition(lease, inputs)
            request = ComputeRequest(snapshot.inputs, TransformContext(
                binding_id=lease.partition.plan_id, execution_id=f"{lease.partition.partition_id}:{lease.attempt}",
                interval=lease.partition.interval, input_versions=snapshot.input_versions,
            ), frozenset(output_refs), scalar=scalar)
            replacement = executor.submit_entrypoint(digest=bundle["source_digest"], entrypoint=bundle["entrypoint"], request=request).result()
            self._storage.commit_replacement(snapshot, input_refs=inputs, output_refs=output_refs, replacement=replacement)
        except Exception as error:
            self._storage.fail_partition(lease, {"type": type(error).__name__, "message": str(error)})
            raise
        return True
