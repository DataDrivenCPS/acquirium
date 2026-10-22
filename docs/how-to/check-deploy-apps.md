---
title: Deploy and inspect native apps
---

Deploy an `acquirium.App` declaration with `client.deploy_app(declaration)`, or use
a factory accepting `(client, parameters)`:

```sh
acquirium app deploy examples/transformation/temperature_conversion.py:fahrenheit_to_celsius
acquirium app list --json
acquirium app inspect fahrenheit-to-celsius --json
```

Before deploying a change, `client.deploy_app(declaration, dry_run=True)` (or
`acquirium app deploy --dry-run ...`) compiles it against the current graph and
reports what deploying would do, without publishing metadata or activating a
plan. For each app, `apps` gives its operators by change (`new`, `redefined`
for a rebuilt history, `rebound` for new inputs under the same definition, or
`unchanged`), the groups to re-evaluate, the input rows they read, and the
output rows cleared and recomputed. `downstream` marks apps that read a changed
result: they may also repair groups that only running the upstream app reveals.
`removed` lists retracted rows of apps the deploy drops, `within_limits` checks
the counted work against the engine's limits, and `matches` and `warnings` are
those a deploy would report. Apps that select an app listed in
`metadata_changes` are compiled against its current outputs, so membership
changes it causes downstream appear only after the real deploy.

App inspection reports desired and active plan IDs, compiled operators, graph
generation, and native errors. Check `client.engine_status()` for backlog and
paused computation. Repair the artifact/environment, then explicitly call
`client.retry_engine()` (`POST /engine/retry`). Accepted changes remain durable while paused.

To remove an app, call `client.remove_app(name)`. Native removal retracts current
outputs and retains historical versions.
