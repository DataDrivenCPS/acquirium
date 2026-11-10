---
title: Deploy and inspect apps
---

Before changing a running app, preview the deployment:

```python
preview = client.deploy_app(app, dry_run=True)
```

The preview reports which groups would be rebuilt and how many input rows they
would read. Check `warnings` for selections that matched no streams. An empty
selection is allowed, but often means a stream has not been registered or a query
is too narrow.

## Deploy and read the state

```python
result = client.deploy_app(app)
state = client.inspect_app(app.name)["app"]
```

Inspect `status`, `error`, and `warnings`. The `operators` list shows the groups,
inputs, and output references. Updating an existing app keeps the other deployed
apps and recalculates the history affected by the change.

To use a declaration factory from the command line:

```sh
acquirium app deploy examples/transformation/threshold_app.py:high_wave
acquirium app list --json
acquirium app inspect high-wave --json
```

A factory accepts `(client, parameters)` and returns an `App`. Add `--dry-run`
to preview it.

## Recover from a failed calculation

A failed calculation pauses publication for the server. Accepted writes stay
pending. Inspect the error before retrying:

```python
status = client.engine_status()
```

Restore the required calculation file or fix its environment, then resume:

```python
client.retry_engine()
client.wait(receipt, timeout=30)
```

Here `receipt` is the pending write's receipt. Deploying a replacement does not
skip failed work, so preserve the files and dependencies that work needs.

## Remove an app

Call `client.remove_app(name)` after removing any apps that select its outputs.
Removal clears its current results; earlier snapshots retain their history.

The [app reference](../reference/apps.md#deployment) describes the preview fields
and file-versioning rules.
