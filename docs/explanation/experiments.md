---
title: Experiments
---

A Study declares inputs, outputs, and logs. An Experiment records one run:
its inputs, results, events, status, and errors.

## Studies and runs

A Study stores each variable's name, role, type, unit, and metadata. Runs reuse
these declarations so their results can be compared.

A variable is a named recording handle. Its role identifies an input, output,
or log. Its type determines how the recorded value is stored. For example,
with a connected client:

```python
study = ac.study.define("operating-scenarios")
# Declare a numeric result shared by runs of this Study.
cost = study.output("total operating cost").scalar(unit="USD")

experiment = study.start()
# Supply the value produced by your calculation.
cost.record(12.5)
experiment.finish()
```

For example, a sweep can record `total operating cost` in every run. Reusing
that label with a different role, type, unit, or metadata raises an error.

Adding a variable during a run emits a warning: it changes the shared Study,
and earlier runs may lack that variable.

Variable IDs are reference URIs derived from the Study ID and variable label.

## Handles follow the active run

A variable's `record()` method writes to its Study object's active run.
You can pass variable handles to functions without passing a run ID.

Each Study object allows one active Experiment. Use separate Study objects
for concurrent runs. Assignment through `experiment.output` is restricted to
that Experiment while it is active.

Reopen a Study and call `resume(run_id)` to continue recording a running
Experiment from another session.

Observations are saved immediately, so recorded data survives a failed run.
Sequence numbers preserve recording order when timestamps collide or events
arrive late. `finish()` and `fail()` end the run; further variable writes are
rejected. Stream annotations can still be added or closed afterward.

## Storage

The experiment ledger stores JSON, text, scalar, and log values directly.
For other data it stores references:

- Files are copied to artifact storage and identified by SHA-256 digest.
- Time-series samples go to Acquirium's time-series storage. The ledger records
  their stream URI and time range.
- `use()` records an existing stream URI and optional interval.

File artifacts are fixed snapshots; identical bytes share one copy. Stream
references read the current samples, including later corrections.

## Stream annotations

An annotation associates an existing stream interval with an Experiment.
It stores the stream URI, start time, optional end time, and JSON metadata in
the run's observation ledger. Intervals can overlap across Experiments.

Omit the end time for an ongoing trial, then close the interval later. You can
add or close an annotation after the Experiment finishes. `created_at` records
when the annotation was added; its interval describes the data's time range.

Find annotations by Experiment, Study, stream URI, or overlapping time window.
Use a variable's `use()` method to record a stream as a named input or output.
Use `annotate_stream()` to associate an interval without declaring a variable.

Metadata belongs to the object it describes. For example, `digester=A` can
describe a run and `treatment=new membrane` one annotated interval. Read stream
labels and units from stream metadata. Annotations require no plant model.

## Run streams and plant properties

Time-series results use source `experiment/<run_id>`. Two runs can therefore
record different values at the same timestamps without overwriting each other.

The declaration's optional `observed` URI identifies the plant property being
measured or simulated. A simulation and a site sensor can both refer to the
tank's volume property while keeping separate stream URIs.

To compare them, select a saved run's output and the site's stream. Read the
same time range from each, convert units if needed, and match timestamps.
The [notebook](../tutorials/first-experiment.py) shows these steps. Its synthetic
site stream supplies example data; use your driver's stream at a real site.

## Limits

Stream annotations read live data; there is no experiment snapshot operation
for stream intervals. Use file artifacts for fixed copies.

Stream discovery, metadata editing, and derived-stream lineage belong to their
respective APIs. The experiment interface uses the resulting stream references.
A combined query across variable, run, and stream metadata is not yet defined.
Recording every WaterTAP variable in large sweeps still needs a performance
evaluation.

See [Record experiments](../how-to/record-experiments.md) for examples and the
[Experiment reference](../reference/experiments.md) for the API.
