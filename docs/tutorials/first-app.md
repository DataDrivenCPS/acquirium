---
title: Your first native app
---

Start a server with a fresh data directory and `exact_only=true`. Run:

```sh
uv run python examples/transformation/publish.py
```

The script registers a Fahrenheit stream, deploys a native function selected
through Acquirium's query API, inserts six values, and fetches Celsius results.
Read `examples/transformation/temperature_conversion.py`: `convert` performs the
pure numeric calculation, while `fahrenheit_to_celsius` builds the declaration.

The declaration has semantic input selections, output metadata, a native window,
and a digest-pinned Python function. `client.deploy_app()` compiles the complete
set of declarations into an Escapement plan. `client.insert_timeseries()` returns
accepted/publication receipts; corrected or deleted input timestamps repair the
derived history through the native engine.

Inspect with `acquirium app list` or `acquirium app inspect fahrenheit-to-celsius`.
For authoring details, see [the native app reference](../reference/apps.md).
