# Drivers

A **Driver** is a Python class that periodically collects data and writes it to Acquirium. The CLI calls `setup()` once at startup, then calls `loop()` on a fixed interval until the process is stopped.

## Writing a driver

Subclass `acquirium.Driver` and implement `setup()` and `loop()`. You get `self.aq` (an `Acquirium` client) and `self.config` (the full parsed TOML dict) for free.

```python
from datetime import datetime, timezone
from acquirium import Driver

class TemperatureDriver(Driver):
    def setup(self):
        self.aq.register_datasource("sensors")

    def loop(self):
        ts = datetime.now(timezone.utc)
        temp = read_sensor()          # your code here
        self.aq.insert_timeseries_batch("sensors", {
            "temp/room1": [(ts, temp)],
        })
```

Rules:
- **No `while True` or `time.sleep` in `loop()`** — the CLI handles the sleep between calls.
- `setup()` is called once before the first `loop()`. Register datasources and insert any RDF here.
- Override `stop()` if you need to release resources on shutdown (close serial ports, flush buffers, etc.). The default is a no-op.

### Reacting to graph changes

Override `on_graph_change()` to run code when the knowledge graph is updated — for example, to discover newly-registered data streams without polling on every tick.

```python
class MQTTDriver(Driver):
    def setup(self):
        self.aq.register_datasource("mqtt")
        self._subscribe()          # initial subscription from graph

    def on_graph_change(self):
        # run queries to find new MQTT topics to subscribe to, update self._subscribe()'s internal state, etc.
        self._subscribe()          # pick up any new MQTTReference nodes

    def loop(self):
        self._flush_pending()      # just move buffered data, no graph queries
```

The CLI polls `GET /graph_version` before each `loop()` call and invokes `on_graph_change()` only when the version has advanced since `setup()` returned. The default is a no-op — drivers that don't need it pay only the cost of one cheap HTTP call per tick.

### Reading from config

`self.config` is the complete TOML dict. Drivers conventionally read their own keys from `self.config["driver"]` (for `acquirium run`) or from a merged view that includes their `[[drivers]]` entry keys (for default drivers — see below).

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.source_id = cfg.get("my_source_id", "default-source")
    self.aq.register_datasource(self.source_id)
```

### Canonical external reference URIs

Drivers should mint external-reference URIs with the helper on `Driver` instead
of inventing their own node names:

```python
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF
from acquirium import Driver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    HAS_EXTERNAL_REFERENCE,
    MQTT_BROKER,
    MQTT_REFERENCE,
    MQTT_TOPIC,
)

class MQTTDriver(Driver):
    def setup(self):
        self._source_id = "mqtt"
        self.aq.register_datasource(self._source_id)

        point_uri = URIRef("urn:point:temp")
        ref_name = "temp-room-1"
        ref_uri = self.reference_uri(ref_name)

        g = Graph()
        g.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
        g.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(self.source_id())))
        g.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)))
        g.add((ref_uri, MQTT_BROKER, Literal("broker.local:1883")))
        g.add((ref_uri, MQTT_TOPIC, Literal("plant/temp/room1")))
        g.add((ref_uri, RDF.type, MQTT_REFERENCE))
        self.aq.insert_graph(g.serialize(format="turtle"), format="turtle", replace=False)
```

Rules:
- Use `self.reference_uri(ref_name)` to compute the canonical `acq:uuid...` URI.
- Use that URI as the object of `ref:hasExternalReference` in the graph.
- Write `acq:sourceId` and `acq:refName` on the same node.
- Attach driver- or app-specific provenance metadata to that same node.
- When inserting data, continue to use the source-local `ref_name` with `insert_timeseries_batch()`. Acquirium derives the same canonical URI internally from `(source_id, ref_name)`.

If you are not inside a `Driver` subclass, the equivalent helper is `aq.reference_uri(source_id, ref_name)`.

## Running a driver manually

```
acquirium run path/to/driver.py:ClassName [--config acquirium.toml] [--interval SECONDS]
```

The driver spec must always include the class name after a colon. Both file paths and dotted module paths are accepted:

```
acquirium run scripts/temp_driver.py:TemperatureDriver --config acquirium.toml
acquirium run mypackage.drivers:TemperatureDriver --config acquirium.toml --interval 5
```

`--interval` overrides the `interval` key in `[driver]` in the config file. If neither is given, the default is 10 seconds.

The `[driver]` section of `acquirium.toml` sets connection defaults:

```toml
[driver]
server_url  = "localhost"
server_port = 8000
use_ssl     = false
interval    = 10.0
```

## Default drivers (auto-start with the server)

Add `[[drivers]]` entries to `acquirium.toml` to have drivers start automatically alongside `acquirium server`. Each entry requires a `spec` and can override any `[driver]` key.

```toml
[[drivers]]
spec     = "scripts/temp_driver.py:TemperatureDriver"
interval = 5.0
```

Multiple drivers:

```toml
[[drivers]]
spec     = "scripts/temp_driver.py:TemperatureDriver"
interval = 5.0

[[drivers]]
spec     = "scripts/pressure_driver.py:PressureDriver"
interval = 30.0
```

When the server starts, each `[[drivers]]` entry is launched in a background thread. The thread waits up to 30 seconds for the server's `/health` endpoint to respond before calling `setup()`, so drivers can safely use `self.aq` from the moment `setup()` runs.

Keys in a `[[drivers]]` entry (other than `spec`) are merged on top of `[driver]` defaults before the driver instance is created. This means a driver can read its own custom keys from `self.config["driver"]`:

```toml
[driver]
interval = 10.0

[[drivers]]
spec           = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
interval       = 5.0
mqtt_source_id = "mqtt"
```

Inside the driver:

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.source_id = cfg.get("mqtt_source_id", "mytopic")  # reads "mqtt" from the [[drivers]] entry. Defaults to "mytopic" if not set.
```

## Built-in MQTT driver

`acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver` subscribes to MQTT topics declared in the knowledge graph and ingests samples via the timeseries API.

```toml
[[drivers]]
spec           = "acquirium.BuiltinDrivers.mqtt_ingestion:MQTTIngestDriver"
interval       = 5.0
mqtt_source_id = "mqtt"
mqtt_qos       = 0
```

Each stream is discovered by querying for `ref:MQTTReference` nodes in the graph. The reference must declare a broker and topic; `ref:timeKey` and `ref:valueKey` identify which fields in the payload carry the timestamp and value (defaulting to `"Timestamp"` and `"Value"`).

### Custom payload encoding

Override `decode_payload()` to handle any wire format without touching subscription or batching logic. The stream identity is already known from `spec.ref_uri` — only the observation needs to be returned. The base implementation decodes JSON or Python-literal dicts and extracts fields by `spec.time_key` / `spec.value_key`.

`scripts/custom_mqtt_driver.py` provides a MessagePack example:

```python
import msgpack
from acquirium.BuiltinDrivers.mqtt_ingestion import MQTTIngestDriver, MQTTStreamSpec

class MyCustomMQTTIngestDriver(MQTTIngestDriver):
    def decode_payload(self, payload: bytes, spec: MQTTStreamSpec) -> tuple[datetime, Any]:
        obj = msgpack.unpackb(payload, raw=False)
        if not isinstance(obj, dict):
            raise ValueError(f"msgpack payload is not a map: {type(obj)}")
        raw_ts = obj.get(spec.time_key)
        raw_val = obj.get(spec.value_key)
        ts = _parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
        return ts, raw_val
```

## Built-in WaterTAP driver

`acquirium.BuiltinDrivers.watertap:WaterTAPDriver` runs a configurable WaterTAP build/solve callable and ingests values mapped in an RDF model. The graph file must contain both:

- `ref:hasExternalReference` from each point to its reference URI
- `acquirium:hasPyomoVar` on the same point with the Pyomo component path to read after solve

Install the optional dependencies with:

```bash
uv run --extra watertap acquirium run acquirium.BuiltinDrivers.watertap:WaterTAPDriver --config acquirium.toml
```

The `watertap` extra only installs the Python packages declared in `pyproject.toml`. Some WaterTAP/Pyomo environments also require separate native extension setup for PyNumero and IDAES. That build step is not handled by `pyproject.toml` extras alone. If your model requires it, run the relevant Pyomo/IDAES extension install commands after enabling the extra.

Typical setup commands are:

```bash
uv sync --extra watertap
uv run pyomo download-extensions
uv run --with setuptools pyomo build-extensions
uv run idaes get-extensions
```

Example:

```toml
[[drivers]]
spec = "acquirium.BuiltinDrivers.watertap:WaterTAPDriver"
interval = 30.0
watertap_source_id = "watertap"
watertap_graph_path = "deployments/WATERTAP2/models/test-model.ttl"
watertap_build_spec = "deployments/WATERTAP2/scripts/example_watertap.py:build_and_solve"
watertap_insert_graph = true
watertap_build_kwargs = { flow_vol = 0.001, salt_mass_conc = 0.035, operating_pressure = 5000000.0, flow_mass_liq = 0.985, flow_mass_salt = 0.015 }
```

Optional keys:

- `watertap_insert_graph_replace` replaces the main graph when inserting it
- `watertap_register_streams` defaults to `true` and registers each mapped point/ref pair
- `watertap_result_attr` extracts the model from an attribute when the build function returns a wrapper object instead of the model directly or as the first tuple item

## Lifecycle

```
acquirium server          acquirium run
      │                         │
      │  (server starts)        │
      │                         │
      ├── wait for /health      │
      │                         │
      ▼                         ▼
   setup()                   setup()
      │                         │
      ▼                         ▼
   ┌─ check graph version ──────┤
   │  (if changed)              │
   │  on_graph_change()         │
   ▼                            ▼
   loop()  ◄─── interval ───► loop()
   loop()                     loop()
   loop()                     ...
      │                         │
   stop()  ◄── Ctrl-C/SIGTERM ► stop()
```

A loop or `on_graph_change` error is logged and the driver continues running. A setup error aborts that driver but does not stop the server.
