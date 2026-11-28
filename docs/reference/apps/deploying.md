---
title: Deploying apps
---

## Deployment

`client.deploy_app(app)` creates or updates an app. `list_apps()` lists them;
`inspect_app(name)` reports its state and compiled operators. Each deployment
keeps the other apps and validates the full set, rejecting cycles or outputs
with conflicting owners.

Deployment and inspection report input `matches` and `warnings`. Zero matches
are valid, for example before a driver registers its streams.

`deploy_app(app, dry_run=True)` previews work without activating the change.
Its `instances` entry reports each group's change, estimated windows and input
rows, and stored output rows that may be replaced or retracted. `may_change`
flags a dependency that could change the group's inputs. `within_limits`
checks the counted work against engine limits.

`client.remove_app(name)` removes an app after its dependants are removed. Its
current outputs are retracted; historical versions remain. Output ownership
stays reserved. For operating steps and failure recovery, see
[deploy and inspect apps](../../how-to/check-deploy-apps.md).

### Deployment files

Paths name files on the server at deployment. `PythonFunction.from_file` hashes
the file; deployment checks the hash and retains a copy under it. The running
app uses that copy, so the source file can be edited or removed afterwards.
To deploy an edit, rebuild the declaration and redeploy; the path can stay the
same. Identical file content keeps the same calculation version.

The worker copies the calculation file into a temporary directory. Helpers
must be in that file or installed packages; adjacent files and paths relative
to `__file__` are unavailable. Dependencies use the server's installed Python
environment. Pin that environment and record interpreter and platform versions
with the release.

The file digest does not cover installed packages or model assets. Version
those separately and record their identities in the calculation when they affect
results. Changing a dependency alone does not trigger history recalculation.
Drain accepted work before changing a shared environment, preserve earlier
releases for recovery, and deploy a newly versioned calculation.

Complete any work spawned by a function before returning.

### Background refresh

Set `freshness="asynchronous"` on `aq.App` to run an expensive calculation,
such as a rolling aggregate over a long history, on Escapement's background
workers. Apps default to `freshness="synchronous"` and evaluate as part of the
epoch that changed their inputs.

The refresher reads a published snapshot and computes the affected windows.
Source writes continue publishing, and readers use the results already
published while the refresh runs. A successful refresh publishes its results
together with updates to synchronous apps that read them.

Window declarations set the event-time calculation intervals. The refresher
runs when work is due. Publishing a deployment or write receipt can precede
the first results from an asynchronous app.

For soft sensors that decide when to train and activate a model, see the
[soft sensor actor examples](../../explanation/soft-sensor-actors.md).

### App status

`inspect_app(name)["app"]` includes `freshness` and `status`. The status can
be `active`, `refreshing` while a background refresh is due or in progress,
`failed` for an evaluation error or a rejected refresh, or `blocked` by an
upstream failure. `pending` describes a plan publication in progress. The
inspection describes the currently published plan.

`stale` indicates that at least one operator is behind. `failures` maps those
operators to the epoch their output reflects and the reason they are behind.
`engine_snapshot()["failures"]` reports the same information by output stream.

Failed calculations retain their last successful outputs. Readers of a failed
synchronous app wait for recovery. Readers of an asynchronous app can continue
using its last published output, including after a failed refresh. Failure
status survives restart. A corrected deployment, changed input, or
`retry_engine()` requests another attempt. Recovery recomputes the affected
branch from current inputs, including missed deletions, before clearing its
failure status. A database commit failure still stops publication globally.

Worker-pool settings and limits are in [server configuration](../server/configuration.md).
