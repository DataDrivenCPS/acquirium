# Acquirium

A semantic data framework for water treatment systems, built on
[Escapement](https://github.com/gtfierro/escapement).

Acquirium connects plant models, units, and semantic queries to sensor streams.
Escapement is the timeseries engine. It stores raw observations, corrections,
deletions, derived values, plans and epoch history, in an embedded DuckDB file
or in PostgreSQL. One server process owns the graph and the engine.

Databases from before Escapement are not imported. Use a new data directory.

## Development setup

```sh
uv sync --locked --extra mqtt
uv run acquirium server --config acquirium.toml
```

Escapement is pinned to a Git revision and currently fetched over SSH. Installation
requires GitHub access and a Rust/C++ toolchain to build the native extension.
CI uses the `ESCAPEMENT_SSH_KEY` repository secret for that source access.
The checkout-local `escapement` symlink is not needed by the installed package.

A script can own a shared local server instead:

```python
import acquirium as aq

client = aq.init(data_dir=".acquirium-native", exact_only=True)
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

Declare semantic selections, a native pointwise/fixed-bucket window, output
metadata, and a versioned Python/Polars function. Acquirium compiles all deployed
apps into one complete Escapement plan. Functions use only their declared inputs
and frozen context; Escapement schedules affected groups and commits raw and
derived changes together.

The [temperature example](examples/transformation/README.md) demonstrates the
basic path. The [soft-sensor chain](examples/soft_sensors/README.md) demonstrates
tank means, semantic discovery of derived inputs, and an alarm in native Python
subprocesses. The [authoring guide](docs/how-to/write-soft-sensors.md) and
[app reference](docs/reference/apps.md) describe deployment and function contracts.
The [streaming workload](examples/streaming/README.md) runs a durable source driver
and resumable committed-change consumer continuously, with bursts, historical
repair, restart checks, and measured latency/backlog.
The [execution profiler](examples/profiling/README.md) measures native epochs and
Python worker stages; [initial results](docs/profiling/2026-10-18/README.md) led to
the bounded persistent worker pool in Escapement.
The [pool comparison](docs/profiling/2026-10-18-pool/README.md) records warm
execution and streaming workload results.
The [sustained model benchmark](examples/model_workload/README.md) measures pool
queueing, warm service, and worker RSS under bursts, recycling, and restart;
[recorded results](docs/profiling/2026-10-18-model/README.md) compare fixed pools.
The [core architecture](docs/explanation/escapement-core.md) explains ownership,
receipts, snapshots, restart, and failure behavior.

## Documentation and tests

Start with [the documentation index](docs/_index.md),
[semantic queries](docs/tutorials/querying.md), and
[the HTTP API](docs/reference/http-api.md).

```sh
uv run pytest tests/unit
```

The native/semantic integration tests use temporary local engine databases and
require no Docker services. MQTT service tests still need a broker.
The optional Docker configuration runs Acquirium and Mosquitto; native builds
forward your SSH agent with BuildKit.
