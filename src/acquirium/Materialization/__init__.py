"""Revision-aware materialization declarations and planning primitives."""

from acquirium.Materialization.api import StatefulTransformation, experiment, outputs, select, service, stateful, transform
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentContext, ExperimentRun, ExperimentRunRequest, ExperimentRunner
from acquirium.Materialization.effects import EffectIntent
from acquirium.Materialization.effect_worker import EffectDispatcher
from acquirium.Materialization.services import ChangeHint, ServiceRecord
from acquirium.Materialization.service_runtime import ServiceContext, ServiceSupervisor
from acquirium.Materialization.context import ComputeRequest, TransformContext
from acquirium.Materialization.executor import LocalExecutorPool, RayExecutorPool
from acquirium.Materialization.worker import ArtifactCache, DefinitionCache
from acquirium.Materialization.scheduler import MaterializationScheduler
from acquirium.Materialization.rebinding import MaterializationRebinder
from acquirium.Materialization.bindings import BindingDiff, BindingSpec, by_entity, diff_bindings, per_input, single, validate_binding_topology
from acquirium.Materialization.impact import TimeRange, coalesce_ranges, full_history, lookback, pointwise, window

__all__ = ["ArtifactCache", "ArtifactCandidate", "ArtifactLease", "ArtifactRequest", "BindingDiff", "BindingSpec", "ChangeHint", "ComputeRequest", "DefinitionCache", "EffectDispatcher", "EffectIntent", "ExperimentContext", "ExperimentRun", "ExperimentRunRequest", "ExperimentRunner", "LocalExecutorPool", "RayExecutorPool", "MaterializationRebinder", "MaterializationScheduler", "ServiceContext", "ServiceRecord", "ServiceSupervisor", "StateRevision", "StatefulTransformation", "TimeRange", "TransformContext", "by_entity", "coalesce_ranges", "diff_bindings", "experiment", "full_history", "lookback", "outputs", "per_input", "pointwise", "select", "service", "single", "stateful", "transform", "validate_binding_topology", "window"]
