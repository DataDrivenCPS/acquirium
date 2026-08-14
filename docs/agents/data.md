---
name: acquirium-data
description: Fetch, shape, convert and write Acquirium timeseries via DataObject and the insert APIs.
load_when: The task involves timeseries values from an Acquirium server — reading, converting units, grouping, or writing data.
human_doc: ../data.md
---

# Acquirium data

`q.data()` returns a lazy `DataObject`; `q.dataframe(...)` is the one-step
shortcut. Nothing is fetched until values are actually needed; after the
first fetch the object caches them.

## Signatures

```python
# on Query
data(*, start=None, end=None, limit=None, order="asc",
     cast_value="float", value_mode="default") -> DataObject
dataframe(*, start=None, end=None, limit=None, order="asc", shape="narrow",
          cast_value="str", value_mode="default", include_ref=False) -> pl.DataFrame

# on DataObject — lazy (no fetch)
metadata(*, include_ref_uris=False) -> pl.DataFrame   # matched points + entity context
total_rows; time_range; is_empty(); aliases; entity_aliases   # properties from server stats
units() -> dict[alias, unit_uri | None]
bindings -> list[BindingInfo]      # per point: row_count, earliest/latest, property_unit, ref_unit
convert_to(to_unit, *, from_unit=None, alias=None) -> DataObject   # queued while lazy

# on DataObject — fetches (then cached)
dataframe(shape="wide", *, include_ref=False, compact=False) -> pl.DataFrame
d[alias] -> pl.DataFrame                       # [time, value(, point_uri)]
iter(alias) -> Iterator[(point_uri, pl.DataFrame)]
by(entity_alias) -> Iterator[(entity_uri, DataObject)]
latest(alias) -> pl.DataFrame                  # fetches the WHOLE window first
```

## Decision rules

- Just need a frame → `q.dataframe(shape="wide")`.
- Unknown result size → `q.data()`, check `total_rows`/`time_range`, narrow
  with `start=`/`end=`/`limit=`, then fetch.
- "What did I match?" → `d.metadata()` (free, no values move).
- Per-equipment processing → `d.by(entity_alias)`; per-point → `d.iter(alias)`.
- Latest value cheaply → `q.dataframe(limit=1, order="desc")`, NOT
  `d.latest()` (it fetches the whole window).
- Wide = `time` first, one column per point, aligned timestamps; good for
  plotting/joining. Narrow (default on Query.dataframe) = one row per
  reading, `value_numeric`/`value_text` split.

## Hard rules

- `DataObject.dataframe()` defaults to `shape="wide"`; `Query.dataframe()`
  defaults to `shape="narrow"`. Don't assume; pass `shape=`.
- Wide column names: plain alias when it covers one point
  (`power`), alias + point when several (`m__wbs:RO-in-pressure`). Columns
  after `time` are alphabetical.
- `convert_to()` returns a NEW object; assign it. It never mutates.
- Unit text resolves through the semantic matcher and can miss
  (`"lb/s"` → `unit:S`, seconds → "no convertible unit pair"). On failure,
  read the candidate URIs in the message; prefer QUDT URIs or long names
  ("pound per second") for anything symbolic.
- A point with no unit annotation cannot be converted without `from_unit=`.
- `value_kind` (numeric/text) is fixed at registration by the writer;
  `value_mode` picks which side you read (`default|numeric|text|coalesce`);
  `cast_value` only does anything for `"float"`/`"int"`.
- Streams MUST be registered before insert:
  `400 ... stream urn:acquirium#<uuid> is not registered`. There is no
  auto-creation.
- A stream registered without `point_uri` stores rows that no semantic query
  can reach. Always pass `point_uri` unless the data is deliberately
  orphaned.
- Inserts are idempotent per (stream, timestamp): same timestamps overwrite,
  never duplicate. `replace=True` truncates the stream first — destructive.
- An empty match yields `DataObject(0 rows, ...)`, not an error; gate on
  `is_empty()`.

## Writing

```python
acq.register_datasource("src-id")                      # once, idempotent
acq.register_streams([{                                # once per stream, BEFORE insert
    "source_id": "src-id", "ref_name": "series-name",
    "value_kind": "numeric",                           # or "text"
    "point_uri": "urn:.../point",                      # ties stream to the model
    "unit": "mg/L", "quantity_kind": "mass concentration",  # free text or URIs
}])
acq.insert_timeseries("src-id", "series-name", [(ts, 412.0), ...])
acq.insert_timeseries_batch("src-id", {"a": rows_a, "b": rows_b})   # auto-chunked
acq.insert_timeseries_arrow("src-id", arrow_table)     # (ts, ref_name, value); bulk path
```

Plant logbook (human notes, separate from timeseries):
`acq.insert_log(msg)`, `acq.read_logs()`, `acq.delete_logs()`.

## Canonical snippets

```python
# look before fetching
d = acq.query().entity("Equipment").measurement(alias="m", quantity_kind="mass flow rate").data()
if not d.is_empty() and d.total_rows < 1_000_000:
    df = d.dataframe(shape="wide")

# convert while still lazy (free), scoped to one alias
d = d.convert_to("kg/min", alias="m")

# per-entity loop
for entity_uri, sub in d.by("Equipment"):
    frame = sub.dataframe(shape="wide")

# unit missing in the graph but known to you
d.convert_to("psi", from_unit="Pa")
```

## Anti-patterns

```python
d.convert_to("kg/min")               # WRONG: result discarded
d = d.convert_to("kg/min")           # right

d.latest("m")                        # WRONG for large windows (full fetch)
q.dataframe(limit=1, order="desc")   # right

q.dataframe()                        # narrow; if you wanted columns per point:
q.dataframe(shape="wide")

acq.insert_timeseries(...)           # WRONG as first call on a new stream
acq.register_streams([...]); acq.insert_timeseries(...)   # right
```
