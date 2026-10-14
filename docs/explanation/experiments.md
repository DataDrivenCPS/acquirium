---
title: Experiments
---

An experiment record has to answer two different questions: what kind of work
was repeated, and what happened during one execution? Acquirium represents the
first as a Study and the second as an Experiment.

## Declarations outlive executions

A Study owns the stable schema of an analysis: named inputs, outputs, and logs,
including each variable's type, unit, and semantic metadata. An Experiment owns
the changing context of one execution: its metadata, observations, status, and
error.

Keeping declarations outside the run makes scenarios comparable. A parameter
sweep can record a cost called `total operating cost` in every run without
redefining what that cost means. Acquirium rejects a declaration that reuses
the label with another role, type, or unit because silently changing the schema
would make old and new observations appear comparable when they are not.

This is also why adding a declaration during a run produces a warning rather
than an error. Exploration is allowed, but the new variable changes the shared
Study and earlier runs may not contain it.

## Handles follow the active run

A variable handle belongs to a Study rather than to one Experiment. Its
`record()` method writes to that Study's active run, which lets ordinary helper
functions accept a `cost` or `solver_log` handle without threading a run ID
through the calculation.

That convenience makes concurrency ambiguous, so one Study object permits only
one active Experiment. Code that needs simultaneous runs can use separate Study
objects. Run-bound output assignment is stricter still: an old Experiment
cannot assign into a newer one accidentally.

A running Experiment can be resumed from a new Study object by its run ID.
This rebinds the Study's variable handles so a months-long trial can keep
recording after a script or client session ends.

Runs become terminal when they succeed or fail. Variable observations are
written as the calculation proceeds rather than buffered until `finish()`, so
partial context survives a failure. A per-run sequence number preserves
recording order even when receipt timestamps are equal or an event's
real-world occurrence time is earlier. Stream annotations can be added later
to document when an existing source contributed to the run.

## One ledger, specialized value storage

Small JSON, text, scalar, and log observations live directly in the experiment
ledger. Larger or already structured values use storage suited to their form:

- files are copied into a content-addressed artifact store, and the ledger
  records their digest;
- time-series samples use Acquirium's ordinary time-series storage, and the
  ledger records their stream reference and time extent;
- `use()` records an existing stream reference and optional interval without
  copying its samples.

The ledger is therefore the provenance spine, not a second storage engine for
every payload. Content addressing makes an attached file an immutable snapshot
and avoids storing identical bytes twice. A reference created with `use()` has
the opposite tradeoff: it is cheap and preserves the data's existing identity,
but later corrections to that stream are visible through the same reference.

## A live stream can participate without changing its identity

Plant data often arrives continuously before anyone names an experiment. An
Experiment can annotate a range of that existing stream, even after the data
was collected. The annotation is another record in the Experiment's observation
ledger and shares its run-local sequence. It holds the stream's reference URI,
a start time, an optional end time, and JSON metadata for that particular
relationship. It does not rename the stream, write experiment metadata onto every sample, or
create a second stream. The same samples may belong to overlapping experiments,
including experiments on parallel treatment trains.

An omitted end denotes an interval still in progress. It can be closed later,
including after the Experiment is finished. An interval can also be added to a
finished Experiment when the trial dates become known afterward. Its
`created_at` records when that attribution was made. Closing records the
effective data time supplied by the caller, which can differ from the server's
receipt time.
Annotations can be found from the Experiment, the Study, or by stream URI and
overlapping time across all Studies. The source stream remains authoritative;
its future corrections are visible when reading an annotation.

The variable `use()` operation still records that a named input or output
consumed a stream. A stream annotation answers the broader question of when a
stream was part of an experiment, without requiring a variable declaration.
Simulation outputs continue to use run-specific streams because independent
scenarios can have the same sample times and different values.

Study, Experiment, variable, and annotation metadata have different scopes.
For example, `digester=A` can describe one Experiment while
`treatment=new membrane` describes only one annotated stream interval. Stream
labels and units should be read from stream metadata, and a property URI can
link a result to a plant model when that link is known. Neither a WATR graph
nor an inferred equipment match is required to annotate a stream.

## Interface consequences from the September discussion

| Need | Experiment interface consequence |
|---|---|
| Select plant data by familiar names | Discover streams in a table with original labels and source information, then pass their stable reference URIs to `annotate_stream()` or `use()`; duplicate labels may identify multiple streams. |
| Record a simulation sweep | Keep declared variables and run-specific result streams so scenarios remain comparable and cannot overwrite each other. |
| Track a months-long plant trial | Persist a running Experiment, use open stream intervals, and resume the run from a later process. |
| Run trials on parallel equipment | Allow intervals to overlap across Experiments; separate Study objects bind variable handles to distinct active runs. |
| Associate a result with equipment | Accept an optional observed property URI when known, while allowing stream interval annotations without a plant model. |
| Keep raw and computed data together | Treat derived streams as normal stream references; an Experiment can annotate either raw or derived intervals. Derived-stream lineage remains the apps interface's responsibility. |

There is still a product decision around immutable experiment snapshots. A
stream annotation is a live reference, so backfilled or corrected source
samples change later reads. Materializing a copy would need an explicit
snapshot operation with provenance to its source range. The interface does not
silently copy samples when an interval is tagged.

The meeting also identified work beyond this interval interface: editing
stream metadata after registration, preserving raw labels and units, querying
arbitrary stream metadata, and aligning that metadata with a derived WATR
graph. Those changes belong to stream discovery and metadata services. The
experiment interface should consume the resulting stable stream references
and avoid assuming that every stream has a graph property.

Study variable IDs use the same source/name reference URI mechanism as data
streams: the Study is the source and the variable label is its local name.
The boundary between Study variable metadata, run metadata, and general stream
metadata still needs a stable query contract.
Capturing every WaterTAP variable and expression in a large parameter sweep
also needs a performance evaluation before promising that workflow as a
default. None of these choices is required for stream interval tagging.

## A result has a run identity and a domain identity

Every time-series result is placed under a run-specific source,
`experiment/<run_id>`. This keeps two scenarios from overwriting one another
when they produce values at the same timestamps.

The declaration's `observed` URI answers a separate question: which physical
or modeled property do those values describe? Several experiment streams and a
live sensor stream can all refer to the same property. Graph queries can then
discover them together, while their reference URIs still distinguish the
source and run.

Semantic linkage does not compare, align, or freeze the data automatically. It
makes the relationship explicit so later analysis can select the intended
streams, normalize their units, and compare them deliberately.

See [Record experiments](../how-to/record-experiments.md) for recording recipes
and the [Experiment reference](../reference/experiments.md) for the complete
interface.
