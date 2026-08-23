# Batch API example

This is a deliberately small ergonomics example:

- `driver.py` generates synchronized temperature samples for two zones.
- `apps.py` contains two class-based query-driven transformations.
- `demo.py` deploys the classes and prints raw and derived values.

From the repository root, start the server and its configured driver:

```bash
uv run acquirium server --config batch_example/acquirium.toml
```

In a second terminal, deploy the transformations and inspect their output:

```bash
uv run python batch_example/demo.py
```

The two invocation styles are deliberately side-by-side:

```text
whole query: every temperature stream
                  └─> AverageTemperature ─> one average stream

per row:  every stream whose quantity kind is Temperature
                  └─> FahrenheitPerTemperature ─> one output per match
```

Both classes build an immutable query. `AverageTemperature` receives the
entire query result once, while `FahrenheitPerTemperature` receives one query
row per invocation. Adding another temperature declaration to the driver adds
another per-row binding without changing the app.

The transform receives normalized `inputs` and a `TransformContext`. It writes
through `context.outputs.declare(...).write(...)`; Acquirium owns output IDs and
validates the staged values.

The demo keeps its local state under `batch_example/.data`, so restarting the
server also demonstrates durable stream and materialization recovery.
