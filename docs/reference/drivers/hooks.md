---
title: Hooks
---

Hooks are the methods a driver author writes. The server calls them; driver code does not call them itself. Which hooks to implement depends on the [base class](base-classes.md).

| Item | Class | Required | Called |
|---|---|---|---|
| [`Driver.setup`](#driversetup) | every driver | yes, except for `FileIngestDriver` | once, before the first tick |
| [`Driver.tick`](#drivertick) | `Driver`, `IngestDriver` | yes; the three ingest subclasses implement it already | right after `setup`, then every `interval` seconds |
| [`Driver.on_graph_change`](#driveron_graph_change) | every driver | no | when the graph has changed |
| [`Driver.stop`](#driverstop) | every driver | no | when the driver is stopped |
| [`PollingIngestDriver.read`](#pollingingestdriverread) | `PollingIngestDriver` | `read` or `collect` | on every tick |
| [`PollingIngestDriver.collect`](#pollingingestdrivercollect) | `PollingIngestDriver` | `read` or `collect` | on every tick |
| [`FileIngestDriver.read`](#fileingestdriverread) | `FileIngestDriver` | yes | on every tick, once per matching file |

## Order of calls

```text
setup()                 once
                        declared streams are registered on the server
tick()                  immediately
repeat:
    wait until the next tick or the next graph check
    on_graph_change()   only when the graph changed since the last check
    tick()              every `interval` seconds
stop()                  when the driver is stopped
                        rows still in the buffer are sent
```

Note that an exception raised in `tick()` is written to the log and the loop continues. An exception raised in `setup()` stops the driver from starting, and the start request reports the error. An exception raised in `on_graph_change()` is written to the log.

The graph is checked every `graph_poll_interval` seconds, see [Configuration](configuration.md).

## `Driver.setup`

```python
Driver.setup()
```

One-time initialisation. Assign `self.source_id`, declare streams, and open connections here.

**Returns**

`None`.

**Example**

```python
def setup(self):
    self.source_id = "tank"
    self.declare("temp", unit="http://qudt.org/vocab/unit/DEG_C")
```

## `Driver.tick`

```python
Driver.tick()
```

One iteration of the driver. `PollingIngestDriver`, `EventIngestDriver` and `FileIngestDriver` already implement it. Implement it only in a class that extends `Driver` or `IngestDriver` directly.

**Returns**

`None`.

## `Driver.on_graph_change`

```python
Driver.on_graph_change()
```

Called when the graph on the server has changed since the last check. The default does nothing. Override it to react to new metadata, for example to query the graph again for streams to subscribe to. It is not called for changes made during `setup()`.

**Returns**

`None`.

## `Driver.stop`

```python
Driver.stop()
```

Cleanup when the driver is stopped. The default does nothing. Override it to close connections. For an ingest driver, the rows still in the buffer are sent after this method returns.

**Returns**

`None`.

**Example**

```python
def stop(self):
    self.client.close()
```

## `PollingIngestDriver.read`

```python
PollingIngestDriver.read()
```

Reads the source once and reports each value with [`self.add(...)`](helpers.md#ingestdriveradd). The default does nothing.

**Returns**

`None`.

**Example**

```python
def read(self):
    self.add("temp", read_sensor())
```

## `PollingIngestDriver.collect`

```python
PollingIngestDriver.collect()
```

Returns all values of one tick as a dataframe. Use it in place of `read` when the data is already a table, for example the result of a model run. The default returns `None`.

**Returns**

`polars.DataFrame` or `None`. The dataframe needs the columns `ts`, `stream_name` and `value`. A `source_id` column is optional; without it the rows belong to `self.source_id`. `None` means there is nothing to insert.

**Raises**

Errors from [`insert_observations`](helpers.md#ingestdriverinsert_observations) apply to the returned dataframe.

**Example**

```python
from datetime import datetime, timezone
import polars as pl

def collect(self):
    now = datetime.now(timezone.utc)
    return pl.DataFrame({
        "ts": [now, now],
        "stream_name": ["temp", "flow"],
        "value": [21.5, 3.0],
    })
```

## `FileIngestDriver.read`

```python
FileIngestDriver.read(path, cursor)
```

Reads the new rows of one file. Declare each stream found in the file before returning.

**Parameters**

- `path` (`Path`): the file to read.
- `cursor` (any JSON value or `None`): the `next_cursor` this method returned for the same file last time. `None` the first time the file is seen.

**Returns**

[`FileBatch`](base-classes.md#filebatch). Its `observations` have the columns `ts`, `stream_name` and `value`. Its `next_cursor` must be a JSON value, for example a row count, a byte position or an ISO timestamp. Return the unchanged `cursor` when the file has nothing new.

**Raises**

Any exception is logged with the file name and the file is tried again on the next tick. Returning something other than a `FileBatch`, or a cursor that is not a JSON value, is treated the same way.

**Example**

```python
def read(self, path, cursor):
    offset = cursor or 0
    df = pl.read_csv(path, skip_rows_after_header=offset)
    if df.is_empty():
        return aq.FileBatch(None, cursor)
    observations = aq.to_observations(df, layout="wide", time_col="ts")
    for name in observations["stream_name"].unique():
        self.declare(name)
    return aq.FileBatch(observations, offset + len(df))
```
