"""Revision-aware materialization declarations and planning primitives."""

from acquirium.Materialization.api import Experiment, OutputSpec, Service, StatefulTransformation, Transformation, outputs
from acquirium.Materialization.state import ArtifactCandidate, ArtifactLease, ArtifactRequest, StateRevision
from acquirium.Materialization.experiments import ExperimentContext, ExperimentRun, ExperimentRunRequest, ExperimentRunner
from acquirium.Materialization.effects import EffectIntent
from acquirium.Materialization.effect_worker import EffectDispatcher
from acquirium.Materialization.services import ChangeHint, ServiceRecord
from acquirium.Materialization.service_runtime import ServiceContext, ServiceSupervisor
from acquirium.Materialization.context import ComputeRequest, InputSet, InputStream, TransformContext
from acquirium.Materialization.outputs import OutputSet, OutputStream
from acquirium.Materialization.executor import RayExecutorPool
from acquirium.Materialization.worker import DefinitionCache
from acquirium.Materialization.impact import TimeRange, coalesce_ranges, full_history, lookback, pointwise, window

__all__ = ["ArtifactCandidate", "ArtifactLease", "ArtifactRequest", "ChangeHint", "ComputeRequest", "DefinitionCache", "EffectDispatcher", "EffectIntent", "Experiment", "ExperimentContext", "ExperimentRun", "ExperimentRunRequest", "ExperimentRunner", "InputSet", "InputStream", "OutputSet", "OutputSpec", "OutputStream", "RayExecutorPool", "Service", "ServiceContext", "ServiceRecord", "ServiceSupervisor", "StateRevision", "StatefulTransformation", "TimeRange", "TransformContext", "Transformation", "coalesce_ranges", "full_history", "lookback", "outputs", "pointwise", "window"]
