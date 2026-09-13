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

Runs become terminal when they succeed or fail. Observations are written as the
calculation proceeds rather than buffered until `finish()`, so partial context
survives a failure. A per-run sequence number preserves recording order even
when receipt timestamps are equal or an event's real-world occurrence time is
earlier.

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
