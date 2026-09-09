"""Incremental materialization.

The authoring surface (``App``, ``output``, ``align``) is re-exported from
the top-level ``acquirium`` package. This package also exports the binding,
storage, and scheduling types used by embedders.

A normal invocation follows this path:

    Deployment -> BindingPlanner -> Binding -> RevisionStore.next_batch()
               -> Scheduler / App.transform() -> RevisionStore.commit_wave()

``runtime.Materializer`` coordinates this path with deployment changes and
changes to the plant graph. The planner resolves stream ownership and validates
its dependency graph; it does not publish results. The scheduler bounds loaded
batches and concurrent transforms; it does not own recovery state. RevisionStore
keeps input snapshots, output replacement, and progress updates consistent.

The timeseries database is the recovery authority. Deployments, lineage,
consumed revisions, and work cursors survive restart. Loaded App instances,
execution timers, errors, and active-generation tokens are process-local.
See ``Binding`` for the identities that connect these two kinds of state.

``local`` and ``checks`` reuse planning and output validation for dry runs.
They return computed results without taking the publication path.
"""
from acquirium.Materialization.models import (
    App, ApplicationGraph, Batch, Binding, InputBatch, OutputBuilder,
    OutputPort, OutputSpec, StreamDescriptor,
    StreamSet, TimeWindow, align, output,
)
from acquirium.Materialization.revision_store import RevisionStore
from acquirium.Materialization.scheduler import InProcessExecutor, Scheduler

__all__ = ["App", "ApplicationGraph", "Batch", "Binding", "InProcessExecutor", "InputBatch", "OutputBuilder", "OutputPort", "OutputSpec", "RevisionStore", "Scheduler", "StreamDescriptor", "StreamSet", "TimeWindow", "align", "output"]
