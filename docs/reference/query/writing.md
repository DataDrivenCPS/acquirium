---
title: Writing from a query
---

These methods run the query and then write to, or read from, every node it matches. They cover metadata and logbook notes.
To write to one node by its URI, use the client methods in [metadata](../client/metadata.md).
The examples use the [example setup](_index.md#example-setup).

| Method | Description |
| --- | --- |
| [`Query.insert_metadata`](#queryinsert_metadata) | Write the same metadata on every matched node |
| [`Query.insert_log`](#queryinsert_log) | Write a logbook note on every matched node |
| [`Query.read_logs`](#queryread_logs) | Read the logbook notes of the matched nodes |

## `Query.insert_metadata`

```python
Query.insert_metadata(values, *, of=None, include_dependencies=True)
```

Writes the same metadata on every node that one node of the query matches. The values follow the rules of `Acquirium.insert_metadata`; see [metadata](../client/metadata.md).

**Parameters**

- `values` (`dict`): attribute names mapped to values.
- `of` (`str | None`, default `None`): the alias of the node whose matches are written to. The default is the current node.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports when the query runs.

**Returns**

`dict`: the answer of the server. `"nodes"` is the number of nodes written to.

**Raises**

- `ValueError`: when `of` is not an alias of the query, or the query has no nodes.

**Example**

```python
acq.query().entity("Pump", label="Booster Pump P2").insert_metadata({"reviewed": True})
```

```text
{'ok': True, 'message': 'update applied', 'changed': True, 'nodes': 1}
```

```python
acq.query().entity("Pump").include("reviewed").metadata()
```

```text
shape: (3, 2)
┌────────────┬───────────────┐
│ Pump       ┆ Pump.reviewed │
│ ---        ┆ ---           │
│ str        ┆ bool          │
╞════════════╪═══════════════╡
│ wbs:intake ┆ null          │
│ wbs:P2     ┆ true          │
│ wbs:P1     ┆ null          │
└────────────┴───────────────┘
```

Note that the second query is a new one. A `Query` object that was built before the write may not know the new attribute name.

## `Query.insert_log`

```python
Query.insert_log(message, *, alias=None, observation_start=None, observation_end=None)
```

Writes a logbook note on every node the query matches. The time of the note is the time of the call.

**Parameters**

- `message` (`str`): the text of the note.
- `alias` (`str | None`, default `None`): the node whose matches get the note. `"*"` writes to the matches of every node of the query. The default is the current node.
- `observation_start` (`datetime | str | None`, default `None`): the start of the period the note is about. A string is an ISO 8601 timestamp.
- `observation_end` (`datetime | str | None`, default `None`): the end of the period the note is about.

**Returns**

`list[str]`: the URIs of the nodes written to, sorted.

**Raises**

- `ValueError`: when the query has no nodes, or matches no node.
- `KeyError`: when `alias` is not an alias of the query.

**Example**

```python
q = acq.query().entity("Pump", alias="pump", label="Booster Pump P2")
q.insert_log(
    "Bearing replaced",
    observation_start="2026-01-01T08:00:00+00:00",
    observation_end="2026-01-01T09:00:00+00:00",
)
```

```text
['urn:swro/P2']
```

## `Query.read_logs`

```python
Query.read_logs(*, alias=None, log_time_start=None, log_time_end=None,
                observation_start=None, observation_end=None)
```

Reads the logbook notes of every node the query matches.

**Parameters**

- `alias` (`str | None`, default `None`): the node whose matches are read. `"*"` reads the matches of every node of the query. The default is the current node.
- `log_time_start` (`datetime | str | None`, default `None`): keep notes written at or after this time.
- `log_time_end` (`datetime | str | None`, default `None`): keep notes written at or before this time.
- `observation_start` (`datetime | str | None`, default `None`): filter on the observation period of the notes, from this time.
- `observation_end` (`datetime | str | None`, default `None`): filter on the observation period of the notes, up to this time.

**Returns**

`polars.DataFrame` with the columns `property_uri` (the URI of the node the note is on), `message`, `log_time`, `observation_start` and `observation_end`. Rows are sorted by `log_time`, oldest first. The table is empty when there are no notes.

**Raises**

- `ValueError`: when the query has no nodes.
- `KeyError`: when `alias` is not an alias of the query.

**Example**

```python
q.read_logs()
```

```text
shape: (1, 5)
┌──────────────┬──────────────────┬────────────────────────────────┬─────────────────────────┬─────────────────────────┐
│ property_uri ┆ message          ┆ log_time                       ┆ observation_start       ┆ observation_end         │
│ ---          ┆ ---              ┆ ---                            ┆ ---                     ┆ ---                     │
│ str          ┆ str              ┆ datetime[μs, UTC]              ┆ datetime[μs, UTC]       ┆ datetime[μs, UTC]       │
╞══════════════╪══════════════════╪════════════════════════════════╪═════════════════════════╪═════════════════════════╡
│ urn:swro/P2  ┆ Bearing replaced ┆ 2026-11-18 01:04:04.496034 UTC ┆ 2026-01-01 08:00:00 UTC ┆ 2026-01-01 09:00:00 UTC │
└──────────────┴──────────────────┴────────────────────────────────┴─────────────────────────┴─────────────────────────┘
```
