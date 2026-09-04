---
title: Apps
---

Apps exist to answer one question: how does a plant get derived values —
soft sensors, KPIs, converted units, anomaly flags — that stay correct as data
arrives, without anyone writing streaming infrastructure?

## Recomputed windows, not event pipelines

Most streaming frameworks make the author think about events: what happens on
each message, what state to carry, how to recover it. Acquirium apps invert
this. An app is a pure function from a window of input samples to a window of
output samples. The runtime finds out what changed, decides the window, calls
the function, and commits the result atomically with its saved position.

Two properties make this simple model safe:

- **Overwrites are idempotent.** An output value is identified by its stream
  and timestamp. Recomputing a window and re-emitting it produces the same
  rows, which overwrite themselves. The author never reasons about duplicates.
- **Progress is transactional.** Output rows and the consumed-input frontier
  commit together. After a crash there is either the output with its advanced
  frontier or neither — never progress without the output it represents.

The cost of the model is a discipline: given the same input batch, `transform`
must be deterministic, and side effects outside the database cannot be rolled
back. In exchange, restart, retry, and catch-up need no code at all.

## Queries bind apps to the plant, not to point lists

An app never lists stream IDs. Its `build_query` is a semantic query over the
plant model — “every temperature measurement on an air handling unit” — and
the compiler turns each match into a concrete *binding*. When the model
changes, the query is compiled again and bindings appear or disappear
accordingly. The calculation is written once; the plant model decides where it
applies.

## Two output identities

A derived stream needs a name, and there are exactly two reasonable places for
one to come from:

- **Relative** (`output.per_input`): the identity is derived from the app,
  the port, and the bound inputs. This scales to thousands of matched streams
  — nobody names them, and recompiling the same inputs reuses the same
  streams.
- **Absolute** (`output.named`): the identity is chosen by the author. Use it
  whenever the result is a thing the plant refers to directly — a total, an
  index, a compliance figure — so it can be found by name rather than
  discovered relative to its inputs.

Derived streams are first-class: later apps' queries can select them, and
those dependencies form a DAG the scheduler runs in waves.

## Why "materialization"

Internally the docs call this machinery *incremental materialization*: derived
streams are materialized views over raw streams, maintained incrementally by
revision rather than recomputed wholesale. That vocabulary lives in
[the implementation notes](../materialization-implementation.md); writing an
app requires none of it.
