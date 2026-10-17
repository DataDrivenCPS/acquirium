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

App inspection reports desired and active plan IDs, compiled operators, graph
generation, and native errors. Check `client.engine_status()` for backlog and
paused computation. Repair the artifact/environment, then explicitly call
`client.retry_engine()` (`POST /engine/retry`). Accepted changes remain durable while paused.

To remove an app, call `client.remove_app(name)`. Native removal retracts current
outputs and retains historical versions. The former class-based app check and
reprocess commands are not part of this initial runtime.
