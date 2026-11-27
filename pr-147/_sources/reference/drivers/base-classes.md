---
title: Base classes
---

A driver extends one of the classes on this page. Each class adds one behaviour to the one above it, so the choice depends on how the data arrives.

| Item | Extends | Use for |
|---|---|---|
| [`Driver`](#driver) | | Drivers that do not write streams, for example one that only maintains the graph |
| [`IngestDriver`](#ingestdriver) | `Driver` | Base of the three classes below. Adds declaring, buffering and inserting |
| [`PollingIngestDriver`](#pollingingestdriver) | `IngestDriver` | Sources that are read on every tick |
| [`EventIngestDriver`](#eventingestdriver) | `IngestDriver` | Sources that push values through a callback |
| [`FileIngestDriver`](#fileingestdriver) | `IngestDriver` | Files that appear in a directory |
| [`FileBatch`](#filebatch) | | The return value of `FileIngestDriver.read` |
| [`UndeclaredStreamError`](#undeclaredstreamerror) | `ValueError` | Raised when a value is reported for a stream that was not declared |
| [`DriverBufferFull`](#driverbufferfull) | `RuntimeError` | Raised when the buffer of unsent rows is full |

## `Driver`

```python
Driver(aq, config)
```

Base class of every driver. It is abstract: a subclass must implement [`setup`](hooks.md#driversetup) and [`tick`](hooks.md#drivertick). The server creates the instance, so a driver does not call this constructor itself.

**Parameters**

- `aq` (`Acquirium`): a client connected to the server that runs the driver.
- `config` (`dict`): the parsed configuration file. The driver's own keys are under `config["driver"]`. See [Configuration](configuration.md).

**Attributes**

| Attribute | Type | Meaning |
|---|---|---|
| `aq` | `Acquirium` | The client passed to the constructor. See [the client reference](../client/starting.md) |
| `config` | `dict` | The full configuration. `config["driver"]` holds this driver's keys |
| `state` | [`DriverState`](state.md#driverstate) | Key-value store saved to disk |
| `source_id` | `str` | The `source_id` of the streams this driver writes. Assign it in `setup()`. Reading it before it is set raises `AttributeError` |

**Example**

A driver that writes no streams and only keeps a node in the graph.

```python
import acquirium as aq


class SiteLabel(aq.Driver):
    def setup(self):
        self.source_id = "site-label"
        self.insert_graph(
            '<urn:site:tank> <http://www.w3.org/2000/01/rdf-schema#label> "Tank" .'
        )

    def tick(self):
        pass
```

## `IngestDriver`

```python
IngestDriver(aq, config)
```

Base class for drivers that write streams. It adds [`declare`](helpers.md#ingestdriverdeclare), [`add`](helpers.md#ingestdriveradd), [`flush`](helpers.md#ingestdriverflush) and [`insert_observations`](helpers.md#ingestdriverinsert_observations). Most drivers extend one of its three subclasses instead of this class.

**Parameters**

- `aq` (`Acquirium`): as for [`Driver`](#driver).
- `config` (`dict`): as for [`Driver`](#driver).

**Attributes**

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `max_buffered_rows` | `int` | `100000` | Largest number of rows that may wait to be inserted. `add` raises [`DriverBufferFull`](#driverbufferfull) beyond it. Set it as a class attribute to change it |

Note that every stream must be declared before a value is reported for it. Declared streams are registered on the server after `setup()` returns and again before each insert.

## `PollingIngestDriver`

```python
PollingIngestDriver(aq, config)
```

Driver whose data is read on every tick. Implement [`read`](hooks.md#pollingingestdriverread) and report each value with `self.add(...)`, or implement [`collect`](hooks.md#pollingingestdrivercollect) and return a dataframe.

On each tick the class sends rows left from a failed insert, calls `read()`, calls `collect()` and inserts its result, then sends the rows buffered by `add`.

**Parameters**

- `aq` (`Acquirium`): as for [`Driver`](#driver).
- `config` (`dict`): as for [`Driver`](#driver).

**Raises**

- `TypeError`: when the subclass implements neither `read` nor `collect`.

**Example**

```python
import acquirium as aq


class TankDriver(aq.PollingIngestDriver):
    def setup(self):
        self.source_id = "tank"
        self.declare("temp")

    def read(self):
        self.add("temp", 21.5)
```

## `EventIngestDriver`

```python
EventIngestDriver(aq, config)
```

Driver whose data arrives through callbacks, for example from a message broker. The callback reports values with `self.add(...)`. Each tick sends the buffered rows, so values are inserted once per interval and not once per message.

`add` may be called from another thread.

**Parameters**

- `aq` (`Acquirium`): as for [`Driver`](#driver).
- `config` (`dict`): as for [`Driver`](#driver).

**Example**

```python
import acquirium as aq


class PushDriver(aq.EventIngestDriver):
    def setup(self):
        self.source_id = "push"
        self.declare("level")
        self.client = connect_to_device(on_value=self.on_value)  # your own code

    def on_value(self, value):
        self.add("level", value)

    def stop(self):
        self.client.close()
```

## `FileIngestDriver`

```python
FileIngestDriver(aq, config)
```

Driver that reads files from a watched directory. Implement [`read(path, cursor)`](hooks.md#fileingestdriverread). The class finds the files, remembers a cursor for each file across restarts, and inserts the rows that `read` returns.

`setup()` is already implemented. It reads three required keys from `config["driver"]`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `source_id` | `str` | required | The `source_id` of the streams. Not required when a subclass assigns `self.source_id` before calling `super().setup()` |
| `watch_dir` | `str` | required | Directory to watch. A relative path is resolved against the directory of the configuration file. Subdirectories are searched too |
| `glob` | `str` or list of `str` | required | File name pattern, or a list of patterns, for example `"*.csv"` |

**Parameters**

- `aq` (`Acquirium`): as for [`Driver`](#driver).
- `config` (`dict`): as for [`Driver`](#driver).

**Attributes**

| Attribute | Type | Meaning |
|---|---|---|
| `watch_dir` | `Path` | The resolved watched directory. Set by `setup()` |
| `file_patterns` | `tuple[str, ...]` | The patterns from `glob`. Set by `setup()` |

**Raises**

- `ValueError`: from `setup()`, when `source_id`, `watch_dir` or `glob` is missing or empty.

On each tick the class visits the matching files in sorted order. A file whose `read` raises is logged and skipped, and the other files are still read. A file whose `read` returns the same cursor as before is skipped. The cursors are saved in [`self.state`](state.md) under the key `"cursors"`.

**Example**

```python
import polars as pl
import acquirium as aq


class LogDriver(aq.FileIngestDriver):
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

## `FileBatch`

```python
FileBatch(observations, next_cursor)
```

The rows read from one file and the position to continue from. This is what [`FileIngestDriver.read`](hooks.md#fileingestdriverread) returns. Instances cannot be changed after creation.

**Parameters**

- `observations` (`polars.DataFrame` or `None`): rows with the columns `ts`, `stream_name` and `value`. `None` or an empty dataframe means there is nothing to insert.
- `next_cursor` (any JSON value): the position after these rows, for example a row count. It is passed back as `cursor` on the next call for the same file.

## `UndeclaredStreamError`

```python
UndeclaredStreamError(message)
```

Raised when a value is reported for a stream that the driver did not declare. It is a subclass of `ValueError`. [`add`](helpers.md#ingestdriveradd), [`register_declared`](helpers.md#ingestdriverregister_declared) and [`insert_observations`](helpers.md#ingestdriverinsert_observations) raise it.

**Example**

```python
self.add("nope", 1)
```

```text
acquirium.Drivers.Driver.UndeclaredStreamError: observation stream ('demo', 'nope') was not declared
```

## `DriverBufferFull`

```python
DriverBufferFull(message)
```

Raised by [`add`](helpers.md#ingestdriveradd) when `max_buffered_rows` rows are already waiting to be inserted. It is a subclass of `RuntimeError`. Rows that were accepted earlier are kept. This happens when values arrive faster than they are inserted, or when the server cannot be reached for a long time.

**Example**

```python
try:
    self.add("level", value)
except aq.DriverBufferFull:
    pass  # drop this value; earlier values are still buffered
```
