# Driver Local State

Every driver gets a local **state directory** on disk. It provides two things:

- A **key-value store** for persisting arbitrary driver state across restarts
  (file offsets, watermarks, counters, last-seen timestamps).
- A **write-ahead log (WAL)** that buffers observation data when the server is
  unreachable and retransmits it automatically once the connection is restored.

Both are available immediately in `setup()` and `tick()` through attributes set
by the base classes before `setup()` is called.

---

## State Directory

Each driver instance gets its own directory under a configurable base:

```
<state_dir>/<driver_id>/
  state.json      ← key-value store
  wal/            ← write-ahead log entries
    00000000.parquet
    00000001.parquet
    ...
```

`state_dir` defaults to `driver_state/` relative to the config file. The
`driver_id` is derived from the driver spec string (the argument passed to
`acquirium run` or the `spec` key in `[[drivers]]`). For example:

```
spec = "scripts/my_driver.py:MyDriver"
→  driver_state/scripts_my_driver.py_MyDriver/
```

Two `[[drivers]]` entries with the same class but different configs get
separate directories because the spec strings differ.

To set an explicit, human-readable directory name, add `driver_id` to the
driver's config section:

```toml
[[drivers]]
spec = "scripts/my_driver.py:MyDriver"
driver_id = "my-driver"
```

That produces `driver_state/my-driver/`.

The base directory is configurable globally under `[driver]`:

```toml
[driver]
state_dir = "./data/driver_state"   # default: "driver_state"
```

Relative paths are resolved against the directory containing `acquirium.toml`.

### In-process drivers

Drivers started via `[[drivers]]` entries run as threads inside the server
process and use `DirectAcquirium`, which dispatches directly to the server's
`Manager` instead of making HTTP requests. The state directory and key-value
store work identically for these drivers.

The WAL and backoff do **not** activate for in-process drivers. Because
`DirectAcquirium` never makes HTTP requests, it never raises the connection
errors (`requests.ConnectionError`, `Timeout`) that trigger buffering. If the
underlying storage fails (e.g. TimescaleDB is unreachable), the Manager raises
a database-level exception. That propagates through `tick()`, is caught by the
thread's top-level error handler, logged, and dropped. The data for that tick
is lost.

This is intentional: a driver and server in the same process cannot be
"disconnected" from each other. The WAL protects against network outages
between a remote driver and the server, a scenario that does not exist for
in-process drivers.

---

## Key-Value Store

`self.state` is a `DriverState` instance. Use it to persist any
JSON-serializable value across restarts.

```python
class MyDriver(PollingIngestDriver):
    def setup(self):
        self.source_id = "my-source"
        self.aq.register_datasource(self.source_id)
        self._cursor = self.state.get("cursor", 0)

    def collect(self):
        rows = fetch_from_api(since=self._cursor)
        if rows:
            self._cursor = rows[-1]["id"]
            self.state.set("cursor", self._cursor)
        return to_dataframe(rows)
```

### API

```python
self.state.get(key: str, default=None) -> Any
```
Return the stored value for `key`, or `default` if absent.

```python
self.state.set(key: str, value: Any) -> None
```
Persist `key → value` immediately. Values must be JSON-serializable (strings,
numbers, lists, dicts, booleans, `None`). Non-serializable types (e.g.
`datetime`) are coerced to strings via `str()`.

```python
self.state.delete(key: str) -> None
```
Remove `key` from the store. No-op if the key is absent.

### Behavior

- Every `set()` and `delete()` writes to disk immediately. There is no buffering
  or deferred flush — state is durable as soon as the call returns.
- Writes are atomic: the new value is written to a `.tmp` file and then renamed
  over the live file, so a crash mid-write cannot corrupt the store.
- On startup, the store is loaded from `state.json`. If the file is missing or
  unreadable, the store starts empty and a warning is logged.
- All values for a driver are stored in a single JSON file. For large datasets
  (e.g. per-file row offsets for hundreds of CSV files), consider grouping keys
  with a prefix rather than storing them as separate top-level keys.

### Common patterns

**Tracking a file offset:**

```python
offset = self.state.get(f"offset:{path}", 0)
new_rows = read_file(path, skip=offset)
# ... process new_rows ...
self.state.set(f"offset:{path}", offset + len(new_rows))
```

**Remembering a high-water mark:**

```python
watermark = self.state.get("watermark")
if watermark:
    watermark = datetime.fromisoformat(watermark)
rows = query_db(after=watermark)
if rows:
    self.state.set("watermark", rows[-1].ts.isoformat())
```

**Counting ticks across restarts:**

```python
ticks = self.state.get("tick_count", 0) + 1
self.state.set("tick_count", ticks)
```

---

## Write-Ahead Log

`IngestDriver` (and its subclasses `PollingIngestDriver` and
`EventIngestDriver`) automatically buffer observation data when the server is
unreachable. This requires no changes to the driver code.

### What happens during a server outage

1. A tick calls `insert_observations()` as normal.
2. The insert attempt raises a connection error (`ConnectionError`, `Timeout`).
3. The observation DataFrame is written to `wal/<seq>.parquet` in the state
   directory.
4. `insert_observations()` returns `{"ok": True, "rows_inserted": 0, "buffered": N}`.
5. The driver continues ticking.

Each subsequent tick while the server is unreachable:

- New observations are also buffered to the WAL (each tick appends a new
  Parquet file).
- Retransmission is attempted according to the backoff schedule (see below).
  When the backoff timer has not elapsed, the retry is skipped and new data is
  buffered immediately without attempting a connection.

### What happens when the server comes back

1. At the start of the next tick where the backoff timer has elapsed, the driver
   flushes the WAL before inserting new live data.
2. Each WAL entry is sent in sequence. Entries are delivered in the order they
   were buffered.
3. On success, the entry's Parquet file is deleted.
4. If a connection error occurs mid-drain, the remaining entries stay on disk
   and the backoff timer is reset. The next attempt will resume from the oldest
   un-acked entry.
5. Once the WAL is empty, the driver resumes normal live insertion.

### WAL entry format

Each WAL entry is a self-contained Parquet file with columns:

```
source_id | ts | ref_name | value
```

`source_id` is always materialized at write time. For single-source drivers
that set `self.source_id`, it is filled in automatically. For multi-source
drivers that include a `source_id` column in the observation frame, each row
keeps its original `source_id`.

### Crash recovery

WAL files are written atomically one at a time. If the driver process crashes
mid-tick, at most the one entry currently being written may be incomplete; all
previously committed entries are intact. On the next startup, the driver picks
up any remaining `.parquet` files and sends them before processing new data.

### Non-connection errors

If a WAL entry fails to send for a reason other than a network error (e.g. a
bad payload the server rejects), the entry is discarded and a warning is logged.
This prevents a single malformed entry from blocking the WAL indefinitely.

### WAL size

WAL entries accumulate on disk until the server is reachable again. There is no
automatic size limit. The practical cap is available disk space. Monitor the
state directory during prolonged outages if disk space is constrained.

---

## Exponential Backoff

After a connection failure, the driver waits before attempting to contact the
server again. The wait grows exponentially with each consecutive failure and is
capped at a maximum.

Default schedule (no jitter shown):

| Failures | Delay |
|----------|-------|
| 1        | 2 s   |
| 2        | 4 s   |
| 3        | 8 s   |
| 4        | 16 s  |
| 5        | 32 s  |
| 6        | 64 s  |
| 7        | 128 s |
| 8+       | 300 s |

A small random jitter (±10% of the delay) is applied to prevent synchronized
retry storms when multiple drivers are running.

The backoff resets to zero as soon as one successful insertion is made.

**Important**: the driver's tick loop continues to run at its normal interval
during an outage. The backoff only controls how frequently a retransmission
attempt is made, not the tick rate. New data continues to be buffered on every
tick regardless of the backoff state.

### Configuration

```toml
[driver]
backoff_base      = 2.0    # seconds; delay = base ^ failures
backoff_max_delay = 300.0  # seconds; caps the delay
```

---

## Configuration Reference

All keys go under `[driver]` in `acquirium.toml`:

| Key | Default | Description |
|-----|---------|-------------|
| `state_dir` | `"driver_state"` | Base directory for all driver state. Relative to config file. |
| `driver_id` | *(derived from spec)* | Override the sub-directory name for this driver's state. |
| `backoff_base` | `2.0` | Exponential backoff base in seconds. |
| `backoff_max_delay` | `300.0` | Maximum backoff delay in seconds. |

When set on a `[[drivers]]` entry, `driver_id` applies only to that driver:

```toml
[[drivers]]
spec = "scripts/csv_ingest.py:CSVIngestDriver"
driver_id = "plant-a-csv"

[[drivers]]
spec = "scripts/csv_ingest.py:CSVIngestDriver"
driver_id = "plant-b-csv"
```

This runs two instances of the same driver class with separate state
directories, so their KV stores and WALs do not interfere.

---

## Full Example

```python
from datetime import datetime, timezone

import polars as pl

from acquirium.Driver import PollingIngestDriver


class PaginatedAPIDriver(PollingIngestDriver):
    """Incrementally fetches rows from a paginated REST API.

    On restart, picks up where it left off using the persisted cursor.
    If the server is unreachable, observations are buffered locally and
    delivered once the connection is restored.
    """

    def setup(self):
        cfg = self.config.get("driver", {})
        self.source_id = cfg.get("source_id", "paginated-api")
        self.aq.register_datasource(self.source_id)
        self.aq.register_stream(
            source_id=self.source_id,
            ref_name="measurements/temp",
            value_kind="numeric",
        )
        # Load cursor from persistent state (0 if first run).
        self._cursor = self.state.get("cursor", 0)

    def collect(self) -> pl.DataFrame:
        rows = fetch_api_page(since_id=self._cursor)
        if not rows:
            return pl.DataFrame(
                schema={"ts": pl.Datetime("us", "UTC"), "ref_name": pl.Utf8, "value": pl.Utf8}
            )

        # Advance cursor only after a successful collect.
        self._cursor = rows[-1]["id"]
        self.state.set("cursor", self._cursor)

        return pl.DataFrame({
            "ts": [r["timestamp"] for r in rows],
            "ref_name": ["measurements/temp"] * len(rows),
            "value": [str(r["value"]) for r in rows],
        })
```

With this driver running and the server temporarily stopped:

1. `collect()` still runs every tick and advances the cursor.
2. Observations go to `driver_state/PaginatedAPIDriver/wal/`.
3. When the server restarts, the WAL drains before any new data is sent.
4. No observations are lost and the cursor is never double-advanced.
