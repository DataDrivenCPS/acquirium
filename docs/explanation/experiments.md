---
title: Experiments
---

A Study describes what an experiment records. Each Experiment is one run of
that Study, with its own inputs, results, events, status, and errors.

## The four objects

- **Study:** the shared declarations for a set of runs.
- **Variable:** a named input, output, or log, with a storage type and metadata.
- **Experiment:** one run, including its status and scenario metadata.
- **Observation:** one recording of a variable during a run.

A variable's role says why you record it. Its type says how to store it:
scalar, JSON, text, file, or time series. A scalar cost output, for example,
records the number calculated by your model. The experiment interface stores
that result; your code still builds and solves the model.

Runs reuse the same declarations so their results can be compared. Reusing a
variable label with a different role, type, unit, or metadata raises an error.
Adding a variable during a run emits a warning because earlier runs may lack it.

One variable can have several observations in a run. A time-series output
recorded in three batches has three observation records. Each record identifies
the stream and interval for that batch.

## Finding and reading results

Studies, variables, runs, and stream annotations also have graph nodes created
by the recording API. Their metadata uses the same registry, URI resolution,
and query expressions as plant metadata. A scalar variable can have a semantic
unit without creating a time-series stream.

Select the runs to compare, find the variable, then read its observations.
Filters stay within the selected Study. You can reopen a saved Study in another
session to find its declarations and results.

`.frame()` lists declarations, runs, or observation records. Scalar observation
tables include the recorded values. Time-series observation tables contain
stream references and intervals; `.dataframe()` fetches the samples.
`.latest()` chooses the last recording, which may be just one batch of a run.

## Metadata and recorded values

Metadata describes a variable, run, or stream annotation. For example,
`scenario="baseline"` makes a run searchable. Values passed to `record()` are
the inputs, outputs, and events produced by the calculation.

Metadata keys follow the [shared metadata rules](../how-to/attach-metadata.md).
Built-in keys such as `unit` have semantic meanings: `"kW"` and `"KiloW"`
resolve to the same QUDT unit. Nulls and empty containers omit descriptive
metadata; JSON and log observations retain those values as data.

Saved `.metadata` objects keep their own fields and supplied unit strings.
Graph metadata tables display resolved units as URIs and use a common struct
schema across rows. A field missing from one row appears as null in that table.

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

The SQL experiment ledger stores lifecycle state, observation order, and metadata
snapshots for recovery. Saved metadata reads and filters use the shared graph
API. The ledger rebuilds that graph after startup or an interrupted write.
Recording these objects creates their graph nodes automatically.

The ledger stores JSON, text, scalar, and log observations directly.
For other data it stores references:

- Files are copied to artifact storage and identified by SHA-256 digest.
- Time-series samples go to Acquirium's time-series storage. The ledger records
  their stream URI and time range.
- `use()` records an existing stream URI and optional interval.

File artifacts are fixed snapshots; identical bytes share one copy. Stream
references read the current samples, including later corrections.

## Deleting runs

Runs cannot be changed after they end, but they can be deleted. Deleting a run
removes everything it recorded: its observations, its stream annotations, the
samples and stream points under `experiment/<run_id>`, and files no other run
uses. A shared file stays until the last run that uses it is deleted. Streams
a run only read or annotated are not changed.

A running run cannot be deleted. Neither can a run whose recorded stream
another run uses as an input, unless both are deleted together. Samples are
removed the same way a stream replacement removes them, so streams derived
from them are rebuilt. `clear()` deletes every Study and run, and nothing else.

`usage()` reports file sizes exactly and counts a shared file once. Sample and
ledger sizes are estimates of the stored values: 8 bytes per number or
timestamp, and text by its length. They are not the compressed size on disk.

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

Each run's time-series output is its own stream point, labeled with the
variable name and carrying `study_id`, `run_id`, and `variable_id`. Stream
queries such as `find_all_data()` and `ac.query().measurement()` list these
points, and `ac.query().measurement(run_id=run.run_id)` selects one run's
streams.

The declaration's optional `observed` URI identifies the plant property being
measured or simulated. It is an attribute of each run's point, not the point
itself: plant queries on the property return the site's measurements only, so
simulated runs never mix with them. Select the runs that simulate a property
with `ac.query().measurement(observed=property_uri)`.

Stream queries list streams only. Scalar, JSON, text, log, and file values
are stored in the experiment ledger, so they do not appear in
`find_all_data()`; read them through the Study's variables and observations.

To compare them, select a saved run's output and the site's stream. Read the
same time range from each, convert units if needed, and match timestamps.
The [notebook](../tutorials/first-experiment.py) shows these steps. Its synthetic
site stream supplies example data; use your driver's stream at a real site.

## Limits

`.metadata_objects()` returns each node's metadata in the shape it was saved;
table output keeps a shared schema. Three cases still use their SQL snapshots:
`None` positions in lists, a top-level `type` key, and lists on semantic
attributes such as `unit`. The
[integration plan](../plans/experiment-metadata-integration.md) tracks these
remaining differences and later cleanup.

Stream annotations read live data; there is no experiment snapshot operation
for stream intervals. Use file artifacts for fixed copies.

Stream discovery, metadata editing, and derived-stream lineage belong to their
respective APIs. The experiment interface uses the resulting stream references.
A combined query across variable, run, and stream metadata is not yet defined.
Recording every WaterTAP variable in large sweeps still needs a performance
evaluation.

See [Record experiments](../how-to/record-experiments.md) for examples and the
[Experiment reference](../reference/experiments.md) for the API.
