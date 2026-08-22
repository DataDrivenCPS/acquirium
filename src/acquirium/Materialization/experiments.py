"""Durable, bounded experiment-run records and validation helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any, Literal, Mapping, Sequence

from jsonschema import Draft202012Validator

from acquirium.Materialization.impact import TimeRange


ExperimentStatus = Literal["pending", "running", "succeeded", "failed", "cancelled", "collected"]


def _json_safe(value: object, *, label: str) -> None:
    try:
        json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be JSON-serializable") from error


def validate_params(params: Mapping[str, object], schema: Mapping[str, object]) -> None:
    """Validate a frozen parameter mapping with the declaration's JSON Schema."""
    _json_safe(params, label="experiment parameters")
    _json_safe(schema, label="experiment parameter schema")
    try:
        Draft202012Validator(dict(schema)).validate(dict(params))
    except Exception as error:
        raise ValueError(f"experiment parameters do not satisfy schema: {error.message}") from error


@dataclass(frozen=True)
class ExperimentRunRequest:
    run_id: str
    definition_id: str
    graph_revision: int
    interval: TimeRange
    params: Mapping[str, object]
    params_schema: Mapping[str, object]
    metadata: Mapping[str, object] = field(default_factory=dict)
    input_versions: Mapping[str, int] = field(default_factory=dict)
    binding_snapshot: Sequence[Mapping[str, object]] = field(default_factory=tuple)
    state_revision: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("experiment run_id is required")
        if not self.definition_id:
            raise ValueError("experiment definition_id is required")
        validate_params(self.params, self.params_schema)
        _json_safe(self.metadata, label="experiment metadata")
        _json_safe(self.input_versions, label="experiment input versions")
        _json_safe(self.binding_snapshot, label="experiment binding snapshot")


@dataclass(frozen=True)
class ExperimentRun:
    run_id: str
    definition_id: str
    graph_revision: int
    interval: TimeRange
    status: ExperimentStatus
    params: Mapping[str, object]
    params_schema: Mapping[str, object]
    metadata: Mapping[str, object]
    input_versions: Mapping[str, int]
    binding_snapshot: Sequence[Mapping[str, object]]
    state_revision: str | None
    started_at: datetime
    finished_at: datetime | None = None
    error: Mapping[str, object] | None = None
    keep_reason: str | None = None
    collected_at: datetime | None = None


@dataclass(frozen=True)
class ExperimentArtifact:
    name: str
    digest: str
    metadata: Mapping[str, object] = field(default_factory=dict)


def run_output_ref(run_id: str, name: str) -> str:
    if not name or "/" in name:
        raise ValueError("experiment output name must be a non-empty path segment")
    return f"urn:acquirium:experiment:{run_id}:output:{name}"


def frozen_inputs_match(run: "ExperimentRun", request: "ExperimentRunRequest") -> bool:
    """Whether an existing run's frozen snapshot equals a new request.

    A matching request is an idempotent replay of the same run; a mismatch on
    any frozen field means the run_id is being reused for different work and
    must be rejected rather than silently returning the original run.
    """
    return (
        run.definition_id == request.definition_id
        and run.graph_revision == request.graph_revision
        and run.interval == request.interval
        and dict(run.params) == dict(request.params)
        and dict(run.params_schema) == dict(request.params_schema)
        and dict(run.metadata) == dict(request.metadata)
        and dict(run.input_versions) == dict(request.input_versions)
        and list(run.binding_snapshot) == list(request.binding_snapshot)
        and run.state_revision == request.state_revision
    )


@dataclass
class ExperimentContext:
    """The only mutable facade supplied to bounded experiment code."""
    run: ExperimentRun
    storage: Any

    @property
    def params(self) -> Mapping[str, object]:
        return self.run.params

    def metric(self, name: str, value: object) -> None:
        self.storage.record_experiment_metric(self.run.run_id, name, value)

    def output_ref(self, name: str) -> str:
        return self.storage.declare_experiment_output(self.run.run_id, name)


class ExperimentRunner:
    """Execute a frozen experiment from durable storage using the shared pool."""
    def __init__(self, storage: Any, executor: Any) -> None:
        self._storage = storage
        self._executor = executor

    def run(self, run_id: str) -> object:
        run = self._storage.experiment_run(run_id)
        if run.status != "running":
            raise ValueError("only a running experiment can execute")
        definition = self._storage.experiment_definition(run.definition_id)
        context = ExperimentContext(run, self._storage)
        try:
            result = self._executor.submit_callable_entrypoint(digest=definition["source_digest"],
                entrypoint=definition["entrypoint"], argument=context).result()
        except Exception as error:
            self._storage.finish_experiment(run_id, status="failed", error={"type": type(error).__name__, "message": str(error)})
            raise
        self._storage.finish_experiment(run_id, status="succeeded")
        return result
