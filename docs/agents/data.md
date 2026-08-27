---
name: acquirium-data
description: Fetch, shape, convert and write Acquirium timeseries via DataObject and the insert APIs.
load_when: The task involves timeseries values from an Acquirium server — reading, converting units, grouping, or writing data.
human_doc: ../tutorials/data.md
---

# Acquirium data

`q.data()` returns a lazy `DataObject`; `q.dataframe(...)` is the one-step
shortcut and equals `q.data(...).dataframe(...)`. Construction runs the
SPARQL and one stats request; values move only when they are needed, and
after the first fetch the object caches them.

## Signatures

```python
# on Query
data(*, start=None, end=None, limit=None, order="asc", include_dependencies=True,
     cast_value="float", value_mode="default") -> DataObject
dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc",
          include_dependencies=True, cast_value="str", value_mode="default",
          include_ref=False, compact=True) -> pl.DataFrame

# on DataObject — lazy (no fetch)
metadata(*, include_ref_uris=False) -> pl.DataFrame   # [data_alias, point_label, point_uri(, ref_uri), entity__*]
total_rows; time_range; is_empty(); aliases; entity_aliases   # from server stats
units() -> dict[alias, unit_uri | None]        # property unit, else the ref's unit
bindings -> list[BindingInfo]  # per point: row_count, earliest/latest, property_unit, ref_unit, point_label
ref_info(alias) -> list[(index, ref_uri)]
convert_to(to_unit, *, from_unit=None, alias=None) -> DataObject   # queued while lazy
by(entity_alias) -> Iterator[(entity_uri, DataObject)]             # lazy sub-objects

# on DataObject — fetches (then cached)
dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc",
          include_ref=False, compact=True) -> pl.DataFrame   # window filters the cached rows
d[alias] -> pl.DataFrame                       # [time, value(, point_uri)]
iter(alias) -> Iterator[(point_uri, pl.DataFrame[time, value])]
latest(alias) -> pl.DataFrame                  # fetches the WHOLE window, then head(1)
```

`start`/`end`/`limit`/`order` on `Query.data`/`Query.dataframe` bound the
server fetch. The same keywords on `DataObject.dataframe` filter the
already-fetched rows. `limit` is per stream in both places. `cast_value`,
`value_mode` and `include_dependencies` exist only on `Query`; they are fixed
at `data()` time.

## Decision rules

- Just need a frame → `q.dataframe()` (wide, compact).
- Unknown result size → `q.data()`, check `total_rows`/`time_range`, narrow
  with `start=`/`end=`/`limit=`, then fetch.
- "What did I match?" → `d.metadata()` (free, no values move).
- Per-equipment processing → `d.by(entity_alias)`; per-point → `d.iter(alias)`.
- Latest value cheaply → `q.dataframe(limit=1, order="desc")`, NOT
  `d.latest()` (it fetches the whole window).
- Wide = `time` first, one column per point, aligned timestamps; good for
  plotting/joining. Narrow = one row per reading,
  `[data_alias, point_id, time, value_numeric, value_text]`
  (`include_ref=True` inserts `ref` after `point_id`).
- Raw identifiers (full `point_uri`, `ref_uri`, `entity__*` columns) →
  `compact=False`; narrow then returns the internal tall frame.

## Hard rules

- Both `Query.dataframe()` and `DataObject.dataframe()` default to
  `shape="wide"`, `compact=True`. Pass `shape="narrow"` for rows.
- Wide column names: plain alias when it covers one point (`power`);
  `alias__<point>` when several, where `<point>` is the point's `rdfs:label`
  if it has one, else its CURIE (`m__wbs:RO-in-pressure`). A data node you
  did not alias is named by label or CURIE alone. A label shared by several
  points is not used. Columns after `time` are sorted case-insensitively. A
  key carrying both numeric and text values gets a second `<key>_text` column.
- `convert_to()` returns a NEW object; assign it. It never mutates.
- Unit text resolves through the semantic matcher; the server picks, among
  the top matches for each side, a pair that is actually convertible. It
  fails with `convert_to: no convertible unit pair for 'x' -> 'y' (...)`, and
  the wrapped detail lists the candidate URIs tried. Prefer QUDT URIs or long
  names ("pound per second") for anything symbolic.
- `units()` is the property's `qudt:hasUnit`, else the reference's. A point
  with neither cannot be converted without `from_unit=`:
  `No unit annotation found for alias 'm'. Provide from_unit explicitly.`
- When the reference's unit differs from the point's, materialisation
  converts the stored rows to the point's unit automatically; you read the
  point's unit. Incompatible pairs are skipped with a warning.
- `value_kind` (numeric/text) is a property of the stream, set at
  registration; `value_mode` picks which side you read:
  `default` (the registered kind), `numeric`/`text` (only rows with that
  side set), `coalesce` (text, else the number as a string). `cast_value`
  only does anything for `"float"`/`"int"`; a failed strict cast logs a
  warning and leaves the values as sent.
- Streams MUST be registered before insert. The server answers 400 with
  `stream urn:acquirium#<uuid> is not registered`. There is no
  auto-creation; `point_uri=` on `insert_timeseries` does not register
  anything.
- `register_streams` requires a non-empty `source_id` per item
  (`each stream registration requires a non-empty source_id`) and either a
  `point_uri` or a `(source_id, ref_name)` pair to mint one.
- A stream registered without `value_kind` is stored as text: numbers land
  in `value_text` as strings. Always pass `value_kind`.
- A stream registered without `point_uri` gets a placeholder point
  `<ref_uri>__point` labelled `<source_id>__<ref_name>`, attached to no
  entity. Entity-scoped queries never reach it. Pass `point_uri` unless the
  data is deliberately orphaned.
- Re-registering a known `point_uri` adds fields it lacks; a conflicting
  value raises `ValueError` (`<field> mismatch for point <...>`) before
  anything is written. A different but convertible `unit` is accepted and
  recorded on the reference as the storage unit; a non-convertible one raises.
- Inserts are idempotent per (stream, timestamp): colliding timestamps are
  deleted and re-inserted, never duplicated; within one batch the last row
  for a timestamp wins. `replace=True` deletes every row of the stream and
  inserts the new ones in one transaction — destructive.
- An empty match yields `DataObject(0 rows, aliases=[], entities=[])`, not
  an error; gate on `is_empty()`.
- `by()` on an unknown entity alias raises `KeyError` listing
  `entity_aliases`.

## Writing

```python
# NOTE: from inside a driver use self.declare(...) instead — the platform
# calls register_datasource/register_streams for you and infers value_kind
# from the first values. This is the direct form, for backfills, imports and
# notebooks.
acq.register_datasource("src-id")                      # once, idempotent
acq.register_streams([{                                # once per stream, BEFORE insert
    "source_id": "src-id", "ref_name": "series-name",  # both required
    "value_kind": "numeric",                           # or "text"; omitted = text
    "point_uri": "urn:.../point",                      # ties stream to the model
    "label": "RO feed pressure",                       # optional rdfs:label
    "unit": "mg/L", "quantity_kind": "mass concentration",  # free text or URIs
    # also: "medium", "substance", "data_source", "properties"
}])
acq.insert_timeseries("src-id", "series-name", [(ts, 412.0), ...])   # kw: replace=False
acq.insert_timeseries_batch("src-id", {"a": rows_a, "b": rows_b})   # chunked by insert_batch_rows (50_000)
acq.insert_timeseries_arrow("src-id", arrow_table)     # columns ts, ref_name, value; bulk path
acq.reference_uri("src-id", "series-name")             # the urn:acquirium#<uuid> a stream is stored under
```

Plant logbook (human notes, separate from timeseries):
`acq.insert_log(message, log_time=None, observation_start=None, observation_end=None)`,
`acq.read_logs(...) -> pl.DataFrame`, `acq.delete_logs()`.

## Canonical snippets

```python
# look before fetching
d = acq.query().entity("Equipment").measurement(alias="m", quantity_kind="mass flow rate").data()
if not d.is_empty() and d.total_rows < 1_000_000:
    df = d.dataframe()

# convert while still lazy (free), scoped to one alias
d = d.convert_to("kg/min", alias="m")

# per-entity loop; sub-objects stay lazy and keep queued conversions
for entity_uri, sub in d.by("Equipment"):
    frame = sub.dataframe()

# unit missing in the graph but known to you
d = d.convert_to("psi", from_unit="Pa")

# rows instead of columns, with the stream reference
df = q.dataframe(shape="narrow", include_ref=True)
```

## Anti-patterns

```python
d.convert_to("kg/min")               # WRONG: result discarded
d = d.convert_to("kg/min")           # right

d.latest("m")                        # WRONG for large windows (full fetch)
q.dataframe(limit=1, order="desc")   # right

q.dataframe(shape="wide")            # redundant; wide is the default
q.dataframe(shape="narrow")          # when you want one row per reading

acq.insert_timeseries(...)           # WRONG as first call on a new stream
acq.register_streams([...]); acq.insert_timeseries(...)   # right

acq.register_streams([{"source_id": s, "ref_name": r}])   # WRONG: text stream, no point
acq.register_streams([{"source_id": s, "ref_name": r,
                       "value_kind": "numeric", "point_uri": p}])   # right
```
