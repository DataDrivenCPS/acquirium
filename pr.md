# Revision-frontier materialization for DuckDB and TimescaleDB

## Example

An app is now a `Transformation`: it reads the streams selected by its semantic
query and publishes a derived stream whenever those inputs change.

```python
class TemperatureNormalizer(aq.Transformation):
    name = "temperature-normalizer"
    window = aq.AroundChange(before="5m")
    outputs = {"normalized": aq.outputs.stream(value_kind="numeric", inherit=True)}

    def build_query(self, aq):
        return aq.query().measurement(alias="temperature")

    def transform(self, inputs, output, context):
        source = inputs["temperature"].collect()
        output["normalized"] = pa.table({
            "time": source["time"],
            "value": pa.compute.subtract(source["value"], 273.15),
        })
```

Deploy the class from Python with
`client.deploy_transformation(TemperatureNormalizer)`, or declare it in an
`[[apps]]` entry in `acquirium.toml`.

## What changed

- Replaces the legacy `Apps` runtime with a small revision-frontier
  materializer.
- Compiles semantic transformations into durable stream bindings and runs them
  incrementally after relevant writes.
- Uses one shared scheduler and transaction model for DuckDB and
  PostgreSQL/TimescaleDB.
- Enables `timeseries_backend = "timescale"` for the new materializer.
- Adds backend contract coverage for revisions, output visibility, and durable
  progress.

The key design and operational reference is
[docs/materialization-implementation.md](docs/materialization-implementation.md).

## Validation

- Focused materialization and storage tests pass locally.
- The compose-backed suite includes DuckDB and TimescaleDB contract coverage.
