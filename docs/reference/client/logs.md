---
title: Logs
---

A log entry is a dated note about the plant, such as a cleaning or a shift change. An entry has a message, the time it was logged, and optionally the period it is about. The three methods on this page work on the entries of the plant as a whole.

| Item | Purpose |
|---|---|
| [`Acquirium.insert_log`](#acquiriuminsert_log) | Add a plant log entry |
| [`Acquirium.read_logs`](#acquiriumread_logs) | Read plant log entries |
| [`Acquirium.delete_logs`](#acquiriumdelete_logs) | Delete all plant log entries |

Every time on this page is a `datetime` or an ISO 8601 string. A time without a time zone is taken as UTC.

## `Acquirium.insert_log`

```python
Acquirium.insert_log(message, log_time=None, observation_start=None,
                     observation_end=None)
```

Adds one log entry.

**Parameters**

- `message` (`str`): the text of the entry.
- `log_time` (`datetime` or `str`, optional): when the entry was logged. Defaults to now.
- `observation_start` (`datetime` or `str`, optional): start of the period the entry is about.
- `observation_end` (`datetime` or `str`, optional): end of that period.

**Returns**

A `dict`:

```python
{'ok': True}
```

**Raises**

- `requests.exceptions.HTTPError`: when a time cannot be read as a timestamp.

**Example**

```python
acq.insert_log(
    "Membrane cleaned",
    log_time="2026-01-02T08:00:00Z",
    observation_start="2026-01-01T00:00:00Z",
    observation_end="2026-01-01T02:00:00Z",
)
acq.insert_log("Operator shift change", log_time="2026-01-03T06:00:00Z")
```

## `Acquirium.read_logs`

```python
Acquirium.read_logs(log_time_start=None, log_time_end=None,
                    observation_start=None, observation_end=None)
```

Reads the log entries, oldest first. Without arguments it returns all of them.

**Parameters**

- `log_time_start` (`datetime` or `str`, optional): only entries logged at or after this time.
- `log_time_end` (`datetime` or `str`, optional): only entries logged at or before this time.
- `observation_start` (`datetime` or `str`, optional): only entries whose period overlaps the time range that starts here. Entries without a period are left out.
- `observation_end` (`datetime` or `str`, optional): end of that time range.

**Returns**

A Polars `DataFrame` with one row per entry and the columns `property_uri`, `message`, `log_time`, `observation_start` and `observation_end`. The three time columns are in UTC. `property_uri` is `urn:acquirium#Plant` for every plant log entry.

Note that when no entry matches, the frame has the same five columns but their type is `null`.

**Example**

```python
print(acq.read_logs())
print(acq.read_logs(log_time_start="2026-01-03"))
```

```text
shape: (2, 5)
┌─────────────────────┬───────────────────────┬─────────────────────────┬─────────────────────────┬─────────────────────────┐
│ property_uri        ┆ message               ┆ log_time                ┆ observation_start       ┆ observation_end         │
│ ---                 ┆ ---                   ┆ ---                     ┆ ---                     ┆ ---                     │
│ str                 ┆ str                   ┆ datetime[μs, UTC]       ┆ datetime[μs, UTC]       ┆ datetime[μs, UTC]       │
╞═════════════════════╪═══════════════════════╪═════════════════════════╪═════════════════════════╪═════════════════════════╡
│ urn:acquirium#Plant ┆ Membrane cleaned      ┆ 2026-01-02 08:00:00 UTC ┆ 2026-01-01 00:00:00 UTC ┆ 2026-01-01 02:00:00 UTC │
│ urn:acquirium#Plant ┆ Operator shift change ┆ 2026-01-03 06:00:00 UTC ┆ null                    ┆ null                    │
└─────────────────────┴───────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
shape: (1, 5)
┌─────────────────────┬───────────────────────┬─────────────────────────┬───────────────────┬───────────────────┐
│ property_uri        ┆ message               ┆ log_time                ┆ observation_start ┆ observation_end   │
│ ---                 ┆ ---                   ┆ ---                     ┆ ---               ┆ ---               │
│ str                 ┆ str                   ┆ datetime[μs, UTC]       ┆ datetime[μs, UTC] ┆ datetime[μs, UTC] │
╞═════════════════════╪═══════════════════════╪═════════════════════════╪═══════════════════╪═══════════════════╡
│ urn:acquirium#Plant ┆ Operator shift change ┆ 2026-01-03 06:00:00 UTC ┆ null              ┆ null              │
└─────────────────────┴───────────────────────┴─────────────────────────┴───────────────────┴───────────────────┘
```

## `Acquirium.delete_logs`

```python
Acquirium.delete_logs()
```

Deletes every plant log entry. This cannot be undone, and there is no way to delete a single entry.

**Returns**

A `dict`:

```python
{'ok': True}
```

**Example**

```python
acq.delete_logs()
print(acq.read_logs().shape)
```

```text
(0, 5)
```
