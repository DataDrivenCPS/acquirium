"""First compute adapter: trusted Python code over Arrow tables."""
from __future__ import annotations
from collections import OrderedDict
from typing import Any, Callable, Protocol
from threading import Lock, get_ident
import pyarrow as pa
from acquirium.Materialization.context import ComputeRequest
from acquirium.Materialization.validation import validate_output
from acquirium.Materialization.api import StatefulTransformation, Transformation

class ComputeAdapter(Protocol):
    def execute(self, target: type, request: ComputeRequest) -> pa.Table: ...

class PythonArrowAdapter:
    """Execute class-based Arrow transformations."""
    def __init__(self, *, max_decoded_artifacts: int = 32) -> None:
        if max_decoded_artifacts < 1:
            raise ValueError("max_decoded_artifacts must be positive")
        self._state_lock = Lock()
        # The adapter is shared by a pool, but these values are deliberately
        # local to the physical worker thread.  They are performance caches,
        # never durable state: a replacement thread reconstructs them from
        # the revision artifact supplied with its request.
        self._state_instances: dict[tuple[int, type], StatefulTransformation] = {}
        self._worker_resources: dict[tuple[int, type], object] = {}
        self._max_decoded_artifacts = max_decoded_artifacts
        self._decoded_artifacts: OrderedDict[tuple[int, type, str], object] = OrderedDict()

    def execute(self, target: type, request: ComputeRequest) -> pa.Table:
        if isinstance(target, type) and issubclass(target, StatefulTransformation):
            if request.artifact_bytes is None:
                raise ValueError("stateful transformations require a pinned artifact")
            digest = request.context.state_revision
            if digest is None:
                raise ValueError("stateful transformations require a pinned state revision")
            with self._state_lock:
                worker_key = get_ident()
                instance_key = (worker_key, target)
                instance = self._state_instances.get(instance_key)
                if instance is None:
                    instance = target()
                    self._state_instances[instance_key] = instance
                worker = self._worker_resources.get(instance_key)
                if worker is None:
                    worker = instance.setup_worker()
                    self._worker_resources[instance_key] = worker
                artifact_key = (worker_key, target, digest)
                state = self._decoded_artifacts.get(artifact_key)
                if state is None:
                    state = instance.load_artifact(request.artifact_bytes, worker)
                    self._decoded_artifacts[artifact_key] = state
                    if len(self._decoded_artifacts) > self._max_decoded_artifacts:
                        self._decoded_artifacts.popitem(last=False)
                else:
                    self._decoded_artifacts.move_to_end(artifact_key)
            result = instance.transform(request.inputs, state, request.context)
            if not isinstance(result, pa.Table):
                raise TypeError("stateful transformations must return a pyarrow.Table")
            return validate_output(result, request)
        if isinstance(target, type) and issubclass(target, Transformation):
            transform = target().transform
            if request.scalar:
                result = self._scalar(transform, request)
            else:
                result = transform(request.inputs, request.context)
                if not isinstance(result, pa.Table):
                    raise TypeError("batch transformations must return a pyarrow.Table")
            return validate_output(result, request)
        raise TypeError("transformation entrypoints must be Transformation classes")

    def _scalar(self, target: Callable[[Any], Any], request: ComputeRequest) -> pa.Table:
        if len(request.output_refs) != 1:
            raise ValueError("scalar adapter requires exactly one owned output stream")
        output_ref = next(iter(request.output_refs))
        numeric = request.inputs.column("numeric_value").to_pylist()
        text = request.inputs.column("text_value").to_pylist()
        values = [number if number is not None else string for number, string in zip(numeric, text)]
        produced = [target(value) for value in values]
        if any(value is not None and (isinstance(value, bool) or not isinstance(value, (float, int, str))) for value in produced):
            raise TypeError("scalar transformations must return float, int, str, or None")
        numeric_values = [float(value) if isinstance(value, (float, int)) and not isinstance(value, bool) else None for value in produced]
        text_values = [value if isinstance(value, str) else None for value in produced]
        return pa.table({"ref_uri": [output_ref] * len(produced), "ts": request.inputs.column("ts"),
                         "numeric_value": numeric_values, "text_value": text_values})
