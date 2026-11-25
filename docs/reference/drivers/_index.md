---
title: Drivers
---

A driver is a Python class that reads data from one place and writes it to Acquirium as streams. The server starts each driver listed in the configuration file, calls its `setup()` once, and then calls its `tick()` on a fixed interval.

| Page | Covers |
|---|---|
| [Base classes](base-classes.md) | The classes a driver extends, and the two exceptions they raise |
| [Hooks](hooks.md) | The methods a driver author overrides, and when the server calls them |
| [Helpers](helpers.md) | The methods available on `self` inside a driver |
| [Persistent state](state.md) | `self.state`, a key-value store that survives restarts |
| [Configuration](configuration.md) | The `[driver]` and `[[drivers]]` tables and their keys |
| [Built-in drivers](built-in-drivers.md) | The drivers that ship with Acquirium and their configuration keys |
| [Tabular helpers](tabular-helpers.md) | `to_observations` and `to_timestamp` |

## All items

| Item | Kind | Page |
|---|---|---|
| [`Driver`](base-classes.md#driver) | class | Base classes |
| [`IngestDriver`](base-classes.md#ingestdriver) | class | Base classes |
| [`PollingIngestDriver`](base-classes.md#pollingingestdriver) | class | Base classes |
| [`EventIngestDriver`](base-classes.md#eventingestdriver) | class | Base classes |
| [`FileIngestDriver`](base-classes.md#fileingestdriver) | class | Base classes |
| [`FileBatch`](base-classes.md#filebatch) | class | Base classes |
| [`UndeclaredStreamError`](base-classes.md#undeclaredstreamerror) | exception | Base classes |
| [`DriverBufferFull`](base-classes.md#driverbufferfull) | exception | Base classes |
| [`Driver.setup`](hooks.md#driversetup) | hook | Hooks |
| [`Driver.tick`](hooks.md#drivertick) | hook | Hooks |
| [`Driver.on_graph_change`](hooks.md#driveron_graph_change) | hook | Hooks |
| [`Driver.stop`](hooks.md#driverstop) | hook | Hooks |
| [`PollingIngestDriver.read`](hooks.md#pollingingestdriverread) | hook | Hooks |
| [`PollingIngestDriver.collect`](hooks.md#pollingingestdrivercollect) | hook | Hooks |
| [`FileIngestDriver.read`](hooks.md#fileingestdriverread) | hook | Hooks |
| [`IngestDriver.declare`](helpers.md#ingestdriverdeclare) | method | Helpers |
| [`IngestDriver.is_declared`](helpers.md#ingestdriveris_declared) | method | Helpers |
| [`IngestDriver.register_declared`](helpers.md#ingestdriverregister_declared) | method | Helpers |
| [`IngestDriver.add`](helpers.md#ingestdriveradd) | method | Helpers |
| [`IngestDriver.flush`](helpers.md#ingestdriverflush) | method | Helpers |
| [`IngestDriver.insert_observations`](helpers.md#ingestdriverinsert_observations) | method | Helpers |
| [`IngestDriver.normalize_observations`](helpers.md#ingestdrivernormalize_observations) | method | Helpers |
| [`IngestDriver.to_timestamp`](helpers.md#ingestdriverto_timestamp) | method | Helpers |
| [`Driver.reference_uri`](helpers.md#driverreference_uri) | method | Helpers |
| [`Driver.insert_graph`](helpers.md#driverinsert_graph) | method | Helpers |
| [`Driver.insert_graph_file`](helpers.md#driverinsert_graph_file) | method | Helpers |
| [`Driver.sparql_update`](helpers.md#driversparql_update) | method | Helpers |
| [`Driver.config_dir`](helpers.md#driverconfig_dir) | method | Helpers |
| [`Driver.data_dir`](helpers.md#driverdata_dir) | method | Helpers |
| [`DriverState`](state.md#driverstate) | class | Persistent state |
| [`DriverState.get`](state.md#driverstateget) | method | Persistent state |
| [`DriverState.set`](state.md#driverstateset) | method | Persistent state |
| [`DriverState.update`](state.md#driverstateupdate) | method | Persistent state |
| [`DriverState.delete`](state.md#driverstatedelete) | method | Persistent state |
| [`DriverState.keys`](state.md#driverstatekeys) | method | Persistent state |
| [`DriverState.clear`](state.md#driverstateclear) | method | Persistent state |
| [`CSVIngestDriver`](built-in-drivers.md#csvingestdriver) | class | Built-in drivers |
| [`CSVIngestDriver.prepare_frame`](built-in-drivers.md#csvingestdriverprepare_frame) | hook | Built-in drivers |
| [`CSVIngestDriver.declare_stream`](built-in-drivers.md#csvingestdriverdeclare_stream) | hook | Built-in drivers |
| [`XLSXIngestDriver`](built-in-drivers.md#xlsxingestdriver) | class | Built-in drivers |
| [`ParquetIngestDriver`](built-in-drivers.md#parquetingestdriver) | class | Built-in drivers |
| [`MQTTIngestDriver`](built-in-drivers.md#mqttingestdriver) | class | Built-in drivers |
| [`MQTTIngestDriver.decode_payload`](built-in-drivers.md#mqttingestdriverdecode_payload) | hook | Built-in drivers |
| [`WaterTAPDriver`](built-in-drivers.md#watertapdriver) | class | Built-in drivers |
| [`SystemMetricsDriver`](built-in-drivers.md#systemmetricsdriver) | class | Built-in drivers |
| [`to_observations`](tabular-helpers.md#to_observations) | function | Tabular helpers |
| [`to_timestamp`](tabular-helpers.md#to_timestamp) | function | Tabular helpers |

## Imports

```python
from acquirium import (
    Driver, IngestDriver, PollingIngestDriver, EventIngestDriver,
    FileIngestDriver, FileBatch, CSVIngestDriver,
    UndeclaredStreamError, DriverBufferFull,
    to_observations, to_timestamp,
)
```

The other built-in drivers are imported from their own modules, for example `acquirium.Drivers.BuiltInDrivers.parquet_ingest.ParquetIngestDriver`.

## A minimal driver

```python
import acquirium as aq


class TankDriver(aq.PollingIngestDriver):
    def setup(self):
        self.source_id = "tank"
        self.declare("temp", unit="http://qudt.org/vocab/unit/DEG_C", label="Tank temperature")

    def read(self):
        self.add("temp", 21.5)
```

```toml
[[drivers]]
spec     = "tank_driver.py:TankDriver"
interval = 5.0
```

The server commands that start, list and stop drivers are described in [the server CLI reference](../server/cli.md).
