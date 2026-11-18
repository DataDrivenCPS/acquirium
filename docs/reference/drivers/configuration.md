---
title: Configuration
---

Drivers are configured in the server's configuration file, `acquirium.toml`. The `[driver]` table holds defaults for all drivers. Each `[[drivers]]` entry starts one driver and can override any default. The rest of the file is described in [the server configuration reference](../server/configuration.md).

| Section | Covers |
|---|---|
| [How the tables are combined](#how-the-tables-are-combined) | What a driver sees in `self.config` |
| [Keys of a `[[drivers]]` entry](#keys-of-a-drivers-entry) | `spec`, `name` |
| [Keys read for every driver](#keys-read-for-every-driver) | `interval`, `graph_poll_interval`, `driver_id`, `insert_batch_rows` |
| [Connection keys](#connection-keys) | `server_url`, `server_port`, `use_ssl` |
| [The `spec` value](#the-spec-value) | How a driver class is named |
| [Driver status](#driver-status) | What `running`, `stopped` and `failed` mean |

```toml
[driver]
interval = 10.0

[[drivers]]
spec     = "acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver"
interval = 30.0

[[drivers]]
spec      = "tank_driver.py:TankDriver"
name      = "tank"
driver_id = "tank"
address   = "192.168.1.20"     # a key of your own
```

## How the tables are combined

For each `[[drivers]]` entry the server builds one dictionary and passes it to the driver as `self.config`.

- `self.config` is the whole configuration file.
- `self.config["driver"]` is the `[driver]` table with the keys of the entry written over it. The keys `spec` and `name` of the entry are left out.
- `self.config["__config_dir"]` is the directory of the configuration file. Read it with [`self.config_dir()`](helpers.md#driverconfig_dir).

A driver reads its own keys from `self.config["driver"]`. Keys of your own need no declaration.

```python
def setup(self):
    cfg = self.config.get("driver", {})
    self.address = cfg.get("address", "127.0.0.1")
```

## Keys of a `[[drivers]]` entry

| Key | Type | Default | Meaning |
|---|---|---|---|
| `spec` | `str` | required | The driver class to start. See [The `spec` value](#the-spec-value). An entry without `spec` is skipped with a warning |
| `name` | `str` | the class name in `spec` | Name of the running driver. It is shown by `acquirium driver list` and used by `acquirium driver stop`. Two running drivers cannot have the same name |

Note that two entries with the same class and no `name` get the same name. The second one does not start and the server logs `Driver '<name>' is already running`. To run one class twice, give each entry its own `name` and `driver_id`.

## Keys read for every driver

These keys can be set in `[driver]` or in a `[[drivers]]` entry.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `interval` | `float` | `10.0` | Seconds between two ticks. For an ingest driver this is also how often buffered values are inserted. Must be greater than zero |
| `graph_poll_interval` | `float` | the larger of `interval` and `10.0` | Seconds between two checks for a change of the graph. See [`on_graph_change`](hooks.md#driveron_graph_change). Must be greater than zero |
| `driver_id` | `str` | none | Name of the driver's [state file](state.md). Without it the file is named after the class |
| `insert_batch_rows` | `int` | `50000` | Largest number of rows per request of `self.aq.insert_timeseries_batch`. It does not change `add` or `insert_observations` |

Note that without `driver_id` the state file is `<ClassName>.json`. If the `[driver]` table itself has a `spec` key, the file is `<ClassName>_<hash of that spec>.json`.

`source_id` is not read by the base classes. `FileIngestDriver` and the built-in drivers read it from the configuration, see [Built-in drivers](built-in-drivers.md). A driver of your own assigns `self.source_id` in `setup()`, from a constant or from a key it reads itself.

The data directory used by [`self.data_dir()`](helpers.md#driverdata_dir) comes from `[server] data_dir`.

## Connection keys

These keys are read from the `[driver]` table only.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `server_url` | `str` | `"127.0.0.1"` | Host of the server. `"0.0.0.0"` is read as `"127.0.0.1"` |
| `server_port` | `int` | the port the server listens on | Port of the server |
| `use_ssl` | `bool` | `false` | Use `https` |

The server uses them as the address its drivers connect back to. The commands `acquirium driver ...` and `acquirium app ...` use them as the address of the server, with port `8000` when `server_port` is not set. See [the server CLI reference](../server/cli.md).

## The `spec` value

`spec` names a driver class in one of two forms.

| Form | Example | Meaning |
|---|---|---|
| `file.py:ClassName` | `"drivers/tank_driver.py:TankDriver"` | A Python file. A relative path is resolved against the directory of the configuration file. Other modules in the same directory can be imported by the file |
| `package.module:ClassName` | `"acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver"` | A module that the server can import |

A value is read as a file when the part before the colon contains `/`, ends with `.py`, or is an existing path. The class must be a subclass of [`Driver`](base-classes.md#driver).

Note that the server imports the class. The file or module must exist on the machine where the server runs.

## Driver status

`acquirium driver list` and the HTTP endpoint `/drivers/list` report one status per driver. See [the HTTP API reference](../server/http-api.md).

| Status | Meaning |
|---|---|
| `running` | The tick loop is active. A tick that raised an exception was logged and the loop continued |
| `stopped` | The loop ended without an error |
| `failed: <error>` | The loop ended with an error |

A driver whose `setup()` raises is not listed. The start request fails with the error.

Stopping a driver ends its loop, calls [`stop()`](hooks.md#driverstop) and sends the buffered rows. A driver that has not finished after 10 seconds is ended by the server.
