"""Compute adapter for normalized query inputs and output handles."""
from __future__ import annotations
from collections import OrderedDict
from dataclasses import replace
from typing import Any, Protocol
from threading import Lock, get_ident
import hashlib
import json
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from acquirium.Materialization.api import OutputSpec
from acquirium.Materialization.context import ComputeRequest, InputSet, InputStream
from acquirium.Materialization.outputs import OutputSet
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
        inputs = request.input_set or _input_set(request)
        specs = {
            name: _as_output_spec(value)
            for name, value in request.output_specs.items()
        }
        output_set = OutputSet(request.context.metadata.get("output_refs", {}), specs)
        context = replace(request.context, outputs=output_set)
        argument = inputs
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
            result = instance.transform(argument, state, context)
            if result is not None:
                raise TypeError("transformations must write through context.outputs")
            return validate_output(output_set.to_arrow(), request)
        if isinstance(target, type) and issubclass(target, Transformation):
            result = target().transform(argument, context)
            if result is not None:
                raise TypeError("transformations must write through context.outputs")
            return validate_output(output_set.to_arrow(), request)
        raise TypeError("transformation entrypoints must be Transformation classes")


def _as_output_spec(value: Any) -> OutputSpec:
    if isinstance(value, OutputSpec):
        return value
    if isinstance(value, dict):
        return OutputSpec(**{key: value[key] for key in ("value_kind", "unit", "quantity_kind", "ref_uri", "prefix") if key in value})
    raise TypeError("durable output specs must be mappings")


def _input_frame(table: pa.Table, ref_uri: str) -> pl.DataFrame:
    rows = table.filter(pc.equal(table["ref_uri"], pa.scalar(ref_uri))).select(
        ["ts", "numeric_value", "text_value"]
    ).to_pylist()
    if any(row["numeric_value"] is not None for row in rows):
        return pl.DataFrame({
            "time": [row["ts"] for row in rows],
            "value": [row["numeric_value"] for row in rows],
        })
    return pl.DataFrame({
        "time": [row["ts"] for row in rows],
        "value": [row["text_value"] for row in rows],
    })


def _input_set(request: ComputeRequest) -> InputSet:
    metadata = request.context.metadata
    raw_streams = metadata.get("input_streams", {})
    streams: dict[str, tuple[InputStream, ...]] = {}
    for alias, values in raw_streams.items():
        items: list[InputStream] = []
        for item in values:
            ref_uri = str(item["ref_uri"])
            key = hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()
            items.append(InputStream(
                alias=str(alias),
                ref_uri=ref_uri,
                values=_input_frame(request.inputs, ref_uri),
                point_uri=item.get("point_uri"),
                unit=item.get("unit"),
                key=key,
            ))
        streams[str(alias)] = tuple(items)
    return InputSet(streams, key=str(metadata.get("logical_key", "")))
