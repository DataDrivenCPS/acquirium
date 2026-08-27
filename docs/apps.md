# Apps

An app is a piece of Python that reads plant data, computes something, and
emits a result. Soft sensors, anomaly detectors, and alerting rules are all
apps.

Apps are the second half of Acquirium. Drivers push data *in* (see
[drivers.md](drivers.md)); apps read it back *out* and derive new values from
it. The two halves are configured differently, and mixing them up is the most
common early mistake:

| | Drivers | Apps |
|---|---|---|
| Job | pull data from a source into the server | compute derived values from stored data |
| Defined by | subclassing `Driver` | subclassing `Transformation` |
| Deployed by | a `[[drivers]]` block in `acquirium.toml` | an `[[apps]]` block, or `aq.deploy_transformation(...)` |

Apps are revision-frontier transformations over the semantic query graph. See
[materialization-implementation.md](materialization-implementation.md) for the
durable runtime and backend details, and the examples under
`examples/transformation/`, `propagation_demo/`, and `dpr-trailer-data/`.

## Configured materialization apps

Revision-frontier transformations can be deployed when the server starts. They
are listed in `[[apps]]` and run after every configured driver has completed
setup, so their semantic queries see the plant model and stream bindings.

```toml
[[apps]]
# A transformation class.
spec = "./temperature_normalizer.py:TemperatureNormalizer"
window = "30m"  # passed to TemperatureNormalizer(window="30m")

[[apps]]
# A registrar function for a multi-transformation DAG.
spec = "./plant_materializations.py:register"
```

A class spec must name an `acquirium.Transformation` subclass. The remaining
keys in that table are passed as keyword arguments to its constructor, making
window and threshold settings durable deployment parameters. A registrar is
called as `register(aq, config)`, where `config` is the rest of its TOML table.
It may call `aq.deploy_transformation(...)` itself, or return one transformation
class (or an iterable of classes). Derived-stream dependencies are inferred
from the transformations' queries and outputs; deploy related transformations
from one registrar when you want to define a DAG together.

Use `acquirium.Transformation` for one invocation over the complete query
result, which supports fan-in across all matching streams. Subclass
`acquirium.RowWiseTransformation` when one independent derived stream should
be created for every query-result row; it still supports fan-in across aliases
within that row and fan-out through multiple named output ports.

## Writing a transformation

An app class declares its inputs with `build_query`, its output ports with
`outputs`, and its calculation with `transform`. The runtime calls
`build_query` while compiling the current graph; `transform` receives a sealed
Arrow-backed input batch and must assign Arrow tables to named output ports.

```python
# temperature_normalizer.py
from __future__ import annotations

import pyarrow as pa
import acquirium as aq


class TemperatureNormalizer(aq.Transformation):
    name = "temperature-normalizer"
    trigger = aq.OnChange(coalesce="250ms", max_delay="5s")
    window = aq.AroundChange(before="5m")
    start = aq.AllAvailable()
    outputs = {
        "normalized": aq.outputs.stream(value_kind="numeric", inherit=True),
    }

    def __init__(self, offset: float = 0.0):
        self.offset = offset

    def build_query(self, aq):
        # The alias becomes the key in `inputs` below.
        return aq.query().measurement(alias="temperature")

    def transform(self, inputs, output, context):
        source = inputs["temperature"].collect()
        if source.num_rows == 0:
            return
        output["normalized"] = pa.table({
            "time": source["time"],
            "value": pa.compute.subtract(source["value"], self.offset),
        })
```

Each output is a table with exactly `time` and `value` columns. `time` must be
timezone-aware, values must be non-null, and timestamps must be unique within
an output. Numeric ports are converted to `float64`; text ports require
strings. Assign only declared ports, and assign each at most once. A missing
port means that invocation publishes no values for it.

`inputs[alias]` is a `StreamSet`. Use `.collect()` for an Arrow table,
`.batches()` to stream Arrow record batches, or `.df("polars")` / `.df("pandas")`
when a dataframe is more convenient. Each table has `ref_uri`, `time`, and
`value` columns. `StreamSet.changes` contains just the values whose storage
revision triggered this invocation; `context.changed_window` and
`context.read_window` explain the triggering and read ranges.

`inherit=True` copies semantic metadata that is common to all input streams
(for example a shared unit). Specify `point_uri`, `unit`, `quantity_kind`, or
other `outputs.stream(...)` fields when output metadata should be explicit.

## Trigger, window, and startup policies

`OnChange()` is the default trigger. It runs after relevant input revisions;
`coalesce` batches rapid writes and `max_delay` prevents indefinite batching.
Use `Every("5m")` for periodic execution. `Changed()` reads only the changed
time extent, `AroundChange(before="5m", after="0s")` adds context, and
`AllAvailable()` reads the complete available extent. `Current()` (the
default) begins after deployment; `AllAvailable()` replays retained inputs.

The output rows and the consumed input frontier advance in one database
transaction. On restart, an already committed batch is not executed again.

## Deploying and inspecting apps

Configure the example above with constructor parameters:

```toml
[[apps]]
spec = "./temperature_normalizer.py:TemperatureNormalizer"
offset = 273.15
```

Or deploy the same class from Python after connecting to a server:

```python
import acquirium as aq
from temperature_normalizer import TemperatureNormalizer

client = aq.Acquirium(server_url="localhost", server_port=8000)
client.deploy_transformation(TemperatureNormalizer, parameters={"offset": 273.15})
dag = client.application_dag()
```

Deployments are durable. Updating the class source or its constructor
parameters creates a distinct deployment identity; remove an obsolete one with
`client.remove_transformation("temperature-normalizer")`.
