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

### Failure status

Function failures are isolated to an operator and its downstream consumers.
Their last successful values remain queryable. Other groups and apps continue
updating. `inspect_app(name)["app"]` includes:

- `status`: `active`, `failed` for an evaluation error, or `blocked` for a failed
  upstream operator. `pending` means a deployment or restart is still
  publishing; the rest of the response describes the plan it replaces.
- `stale`: whether any operator in the app is failed or blocked.
- `failures` maps instance IDs to the epoch their output reflects and the
  reason they are behind. The entry includes the evaluation message or the
  upstream instances holding it.

`engine_snapshot()["failures"]` reports the same information by output stream.
Publication of a receipt does not mean all derived streams are current.
Failure status survives restart. A corrected deployment, changed input, or
`retry_engine()` requests another attempt. Recovery recomputes the affected
branch from current inputs, including missed deletions, before clearing its
failure status. A database commit failure still stops publication globally.

Branches share an epoch commit. A slow function can delay publication until it
finishes or times out.

Worker-pool settings and limits are in [server configuration](../server/configuration.md).
