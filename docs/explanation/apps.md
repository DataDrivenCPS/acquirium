---
title: Apps and native epochs
---

An app pairs semantic selections with an Escapement function. Each selection is
a graph query that names input streams and groups them; `outputs_of()` selects
another app's output, so apps chain. A graph change re-runs the selections and
rebuilds each group's inputs. A new or corrected reading re-evaluates only the
windows it falls in.

A window is `Pointwise`, `FixedBucket` or `Rolling`. A function is native
(`Native.mean`, `sum`, `max`, `min`, `count`, `threshold`), which runs in the
engine with no Python, or a `PythonFunction`, which runs in Escapement's worker
processes, one window per call or every affected window at once with
`batch=True`. Units and per-group parameters are frozen into each operator, so
changing one re-evaluates that group's history.

Every deploy builds one complete plan from all apps, so updating one app keeps
the rest. A function's code, window and outputs are versioned separately from
which streams it reads.

Escapement evaluates operators in dependency order, publishes source and derived
changes together, and keeps history. A failed function publishes nothing from
its epoch and pauses all processing until `retry_engine()`. Replaying accepted
work is safe because functions read only their declared inputs and frozen
context.

See [the app reference](../reference/apps.md), [your first app](../tutorials/first-app.md),
and [the core architecture](escapement-core.md).
