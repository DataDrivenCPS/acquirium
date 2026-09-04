# Revision-frontier materialization for DuckDB and TimescaleDB

This replaces the legacy `Apps` runtime with a small revision-frontier
materializer sharing one scheduler and transaction model across DuckDB and
PostgreSQL/TimescaleDB. An app is now a class whose semantic query selects its
input streams and whose `transform` recomputes a window of derived values
whenever those inputs change; output rows and the consumed input frontier
commit in one transaction, so restarts resume exactly where the database says
they stopped. Progress is keyed by what a binding reads and writes rather than
by its code digest, so editing an app's source or parameters continues its
frontier instead of resetting it.

```python
class TemperatureNormalizer(aq.App):
    name = "temperature-normalizer"
    outputs = {"normalized": aq.output.per_row(value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        temperature = inputs["temperature"].in_unit("DEG_C").df()
        output["normalized"] = temperature.select(
            "time", (pl.col("value") - 273.15).alias("value")
        )
```

Outputs come in two flavors that also decide execution: `aq.output.per_row(...)`
runs the transform once per query match and derives one stream beside each
(fan-out across many sensors with no manual naming), while `aq.output.named("...")`
runs it once over the complete result and publishes one absolute stream whose
identity the author chooses — there is a single `App` base class and no
separate row-wise variant. `inputs[alias].in_unit("DEG_C")` converts every
bound stream from its own recorded unit and raises on missing or incompatible
units; `aq.align` resamples differently clocked inputs onto one shared time
grid. Scheduling and windowing are plain attributes — `lookback = "5m"` or
`"all"`, `backfill = True`, and the composable throttles `coalesce`,
`max_delay`, and `min_interval` — with no policy classes to learn, and apps
deploy via `client.deploy_app(...)` or an `[[apps]]` entry in
`acquirium.toml`. Before deploying, `acquirium app check module:AppClass`
(or `client.check_app(...)`) compiles the query, runs the transform over
every retained input row, and prints what it computed without writing
anything — no deployment, no derived streams, no progress rows. The authoring contract is in
[docs/reference/apps.md](docs/reference/apps.md); the runtime and backend
design is in
[docs/materialization-implementation.md](docs/materialization-implementation.md).
Focused materialization and storage tests pass locally, and the compose-backed
suite includes DuckDB and TimescaleDB contract coverage for revisions, output
visibility, and durable progress.
