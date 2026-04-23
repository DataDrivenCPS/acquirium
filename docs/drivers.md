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

### Reading from config

`self.config` is the complete TOML dict. Drivers conventionally read their own keys from `self.config["driver"]` (for `acquirium run`) or from a merged view that includes their `[[drivers]]` entry keys (for default drivers — see below).

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.source_id = cfg.get("my_source_id", "default-source")
    self.aq.register_datasource(self.source_id)
```

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
spec           = "acquirium.Server.mqtt_ingestion:MQTTIngestDriver"
interval       = 5.0
mqtt_source_id = "mqtt"
```

Inside the driver:

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.source_id = cfg.get("mqtt_source_id", "mytopic")  # reads "mqtt" from the [[drivers]] entry. Defaults to "mytopic" if not set.
```

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
   loop()  ◄─── interval ───► loop()
   loop()                     loop()
   loop()                     ...
      │                         │
   stop()  ◄── Ctrl-C/SIGTERM ► stop()
```

A loop error is logged and the driver continues running. A setup error aborts that driver but does not stop the server.
