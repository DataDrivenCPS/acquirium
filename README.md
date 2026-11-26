# Acquirium

A semantic data framework for water treatment systems, built on
[Escapement](https://github.com/gtfierro/escapement).

Acquirium connects plant models and semantic queries to sensor streams.
Escapement stores the readings and runs the calculations that keep derived
streams current. Data lives in a local DuckDB file or PostgreSQL.

## Development setup

```sh
uv sync --locked --extra mqtt
uv run acquirium server --config acquirium.toml
```

A script can own a shared local server instead:

```python
import acquirium as aq

client = aq.init(data_dir=".acquirium", exact_only=True)
try:
    client.register_streams([{
        "source_id": "gateway", "ref_name": "temperature",
        "point_uri": "urn:plant:tank-temperature",
        "unit": "http://qudt.org/vocab/unit/DEG_F",
        "value_kind": "numeric",
    }])
    result = client.insert_timeseries(
        "gateway", "temperature", [(1_800_000_000_000_000_000, 68.0)],
        request_id="gateway-batch-1",
    )
    snapshot = client.wait(result["receipts"][0])
    frame = client.query().measurement(alias="temperature").dataframe()
finally:
    aq.shutdown()
```

Integer timestamps are UTC nanoseconds. Observation values must be finite,
non-null numbers. Ranges are half-open. Reuse a source/request ID with the same
normalized payload to retry within the engine's bounded retry window. A response
reports durable acceptance separately from publication; accepted work remains
pending if a computation fails.

## Derived streams

An app turns selected source streams into derived streams. New readings and
corrections update the results, including those of downstream apps.

Start with [Soft sensor app](docs/tutorials/first-app.md): generate a sine wave
with a driver and write a Python threshold function. The
[Turbidity alarm pipeline](docs/tutorials/turbidity.md) tutorial cleans faults,
spikes, and gaps from turbidity readings, then raises a sustained alarm. The turbidity demo starts its own
temporary server and checks its results.

To build your own, see [Soft sensor calculations](docs/how-to/write-soft-sensors.md).
The [app reference](docs/reference/apps.md) covers the interface options.

## Documentation and tests

Start with [the documentation index](docs/_index.md),
[semantic queries](docs/tutorials/querying.md), and
[the HTTP API](docs/reference/http-api.md).
To record experiments, see [your first experiment](docs/tutorials/first-experiment.py),
[Experiment recording](docs/how-to/record-experiments.md), and the
[experiment reference](docs/reference/experiments.md).

```sh
uv run pytest
```

Every test starts its server in-process with `aq.init()` on temporary
storage, so no Docker services are needed. PostgreSQL tests run when
`ACQUIRIUM_TEST_PG_DSN` is set, and MQTT tests when a broker answers at
`ACQUIRIUM_TEST_MQTT` (default `localhost:1883`); otherwise they skip.
The optional Docker configuration runs Acquirium and Mosquitto; native builds
forward your SSH agent with BuildKit.
