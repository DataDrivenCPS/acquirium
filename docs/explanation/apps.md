---
title: Apps
---

Apps exist to answer one question: how does a plant get derived values —
soft sensors, KPIs, converted units, anomaly flags — that stay correct as data
arrives, without anyone writing streaming infrastructure?

## Recomputed windows

An app is a pure function from a window of input samples to a window of
output samples. The runtime finds out what changed, determines the affected
output interval, and reads the input context needed to calculate it. Complete
buckets, trailing dependencies, and leading dependencies are declared through
`every`, `lookback`, and `lookahead`. Computation uses the latest available
readings, and late readings trigger another calculation.

Two properties make this simple model safe:

- **Replacement is idempotent.** An output value is identified by its stream
  and timestamp. Assigning a port replaces its output interval, including
  removing results that disappear. An alarm is removed when its corrected
  reading no longer exceeds the threshold. Downstream apps observe removals.
- **Progress is transactional.** Output rows and the consumed-input frontier
  commit together. After a crash there is either the output with its advanced
  frontier or neither — never progress without the output it represents.

The cost of the model is a discipline: given the same input batch, `transform`
must be deterministic, and side effects outside the database cannot be rolled
back. In exchange, restart, retry, and catch-up need no code at all.

## Queries bind apps to the plant

An app never lists stream IDs. Its `build_query` is a semantic query over the
plant model — “every temperature measurement on an air handling unit” — and
the compiler resolves the streams into concrete *bindings*. With
`grouping="per_match"`, each match gets its own invocation. With
`grouping="all_matches"`, one invocation receives all the matches. When the
model changes, queries are compiled again and retained output is repaired when
the match context changes. The calculation is written once; the plant model
decides where it applies.

## Two output identities

Grouping determines invocation cardinality. Output declarations determine
stream identity independently.

A derived stream needs a name, and there are exactly two reasonable places for
one to come from:

- **Relative** (`output.stream`): the identity is derived from the app,
  the port, and the bound inputs. This scales to thousands of matched streams
  — nobody names them, and recompiling the same inputs reuses the same
  streams.
- **Absolute** (`output.named`): the identity is chosen by the author. Use it
  whenever the result is a thing the plant refers to directly — a total, an
  index, a compliance figure — so it can be found by name rather than
  discovered relative to its inputs.

Derived streams are first-class: later apps' queries can select them, and
those dependencies form a DAG the scheduler runs in waves. They are also
findable on their own terms — a derived stream carries the metadata its
declaration gave it, and records the app that produced it, so
`measurement(quantity_kind="temperature", app="normalize-temperatures")` asks
for one app's output and nothing else.

## Why "materialization"

Internally the docs call this machinery *incremental materialization*: derived
streams are materialized views over raw streams, maintained incrementally by
revision rather than recomputed wholesale. That vocabulary lives in
[How it works](../reference/apps.md#how-it-works), with the backend and
operational details in
[Backends and operations](../materialization-implementation.md); writing an
app requires none of it.
