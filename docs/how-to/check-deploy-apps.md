---
title: Check, deploy, and repair an app
---

This guide uses the `Celsius` water-temperature app from
[Calculate derived streams](calculate-derived-streams.md), saved in
`plant_apps.py`. It assumes an Acquirium server is running with input data.

## Check an app

A check executes the app against retained data without publishing results:

```bash
acquirium app check plant_apps:Celsius --local
```

Use `--local` to run in your terminal with breakpoints and tracebacks. Without
it, the server executes the check. Checks load retained input history and can
consume more memory than normal partitioned execution.

## Deploy an app

Deploy an imported class with `client.deploy_app(Celsius)`, where `client` is
your configured Acquirium instance. The server must be able to import the same
module; deployment sends its entrypoint, parameters, and source digest.
Use `parameters={...}` for constructor arguments.

## Apply code changes to retained history

Code updates preserve stream identities and consumed progress. To apply changed
code to retained history, use an explicit output interval:

```python
from datetime import datetime, timezone

client.reprocess_app(
    "celsius",
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
)
```

Reprocessing survives a restart and does not reset incremental progress.
Bucketed apps expand the requested interval to complete buckets.
A second request for a binding already processing a durable interval is rejected;
wait for that work to finish.

## Remove an app

`client.remove_app("celsius")` stops the app and forgets its progress and pending
work. Existing derived history remains stored.

## Advanced execution controls

- `batch_delay = "2s"`: wait two seconds from the first pending change before
  running. This lets a rapidly updating stream collect several readings into
  one invocation and can reduce the overhead of expensive computations.
  Subsequent changes do not restart the timer.
- `min_interval = "1m"`: wait at least one minute after a successful invocation
  before running the binding again, even when additional changes arrive. Use
  this to cap the successful execution rate of an expensive computation.
- `backfill = True`: process retained history on initial activation.

Most apps can leave `batch_delay` and `min_interval` at their defaults. These
advanced settings control how often the runtime performs a calculation, while
`every`, `lookback`, and `lookahead` describe which readings the calculation
needs. A ten-minute rolling average, for example, may be cheap enough to update
on every arrival or expensive enough to run less often. Its lookback alone
does not determine an appropriate delay.

Both controls measure elapsed wall-clock time, and their timing state resets
on server restart. They do not delay failure retries; a failed transform can
retry at the materialization polling cadence.

See the [app reference](../reference/apps.md) for the complete contract,
and [operations](../explanation/materialization-internals.md) for storage and server settings.
