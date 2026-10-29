"""Incremental materialization.

The authoring surface (``App``, ``output``, ``align``) is re-exported from
the top-level ``acquirium`` package. This package also exports the binding
and stream types used by embedders.

A write follows this path:

    publish -> Engine.submit() (inbox) -> Engine.step()
            -> timeseries at one revision + ViewEvaluator on the same transaction

``runtime.Materializer`` owns the deployment log and the plan of every
deployment over all of time (``planner.BindingPlanner.temporal_plan``),
admits deployment changes and model writes, and runs the engine's steps.
``evaluation.ViewEvaluator`` recomputes, inside a step, every output point the
step's writes can change, in the order of the plan's schedule.

The timeseries database is the recovery authority: the deployment log, the
inbox, the evaluator's frontier and the outputs survive restart. The plan and
the loaded app code are rebuilt in the process.

``local`` and ``checks`` run an app without publishing what it computes.
"""
from acquirium.Materialization.models import (
    App, ApplicationGraph, Batch, Binding, InputBatch, OutputBuilder,
    OutputPort, OutputSpec, StreamDescriptor,
    StreamSet, TimeWindow, align, output,
)
__all__ = ["App", "ApplicationGraph", "Batch", "Binding", "InputBatch", "OutputBuilder", "OutputPort", "OutputSpec", "StreamDescriptor", "StreamSet", "TimeWindow", "align", "output"]
