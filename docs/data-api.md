# Data API

This page describes the Python and HTTP APIs for reading and writing managed
timeseries data. For the identity model behind these APIs, see
[`data-stream-lifecycle.md`](data-stream-lifecycle.md).

## Writing Observations

External programs write observations by `source_id` and `ref_name`; they do not
need to compute or store `ref_uri` themselves. Driver authors use `declare()`
and `add()` (or return a canonical observation frame), as described in
[`drivers.md`](drivers.md); the platform performs the equivalent registration
and insertion calls.

```python
aq.register_stream(
    source_id="plant-historian",
    ref_name="TI-101",
    value_kind="numeric",
)

aq.insert_timeseries_batch(
    "plant-historian",
    {
        "TI-101": [
            (ts1, 21.5),
            (ts2, "Manual Control"),
            (ts3, 22.1),
        ],
    },
)
```

`value_kind` is stream-level metadata. Use:

- `"numeric"` for telemetry that should be stored in `numeric_value`
- `"text"` for status, state, enum, JSON-like, or other nonnumeric samples

For numeric streams, Acquirium tries to convert each non-null observation to a
float. If conversion fails, the row is stored in `text_value` instead of
rejecting the whole insert. This lets a mostly numeric stream preserve
occasional status strings such as `"Manual Control"`.

Blank strings in numeric streams are treated as null values. Boolean values in
numeric streams are stored as text.

## Reading Observations

The low-level client reads one stream at a time:

```python
df = aq.client.timeseries_df(
    "urn:acquirium#...",
    start="2026-01-01T00:00:00Z",
    end="2026-01-02T00:00:00Z",
    order="asc",
    value_mode="default",
)
```

The same `value_mode` option is available through query APIs:

```python
df = query.dataframe(value_mode="coalesce")
latest = query.latest_data(value_mode="text")
data = query.data(value_mode="numeric")
```

The HTTP endpoint accepts the same query parameter:

```text
GET /timeseries?uri=<ref-or-point-uri>&value_mode=coalesce
```

## `value_mode`

Timeseries rows are physically stored in two nullable columns:

- `numeric_value`
- `text_value`

Read APIs return a single `value` column, so `value_mode` controls how those
two storage columns are projected:

| Mode | Behavior |
| --- | --- |
| `default` | Default. Use the stream's `value_kind`: numeric streams return `numeric_value`; text streams return `text_value`. |
| `numeric` | Return only rows where `numeric_value` is not null. The returned `value` column is numeric. |
| `text` | Return only rows where `text_value` is not null. The returned `value` column is text. |
| `coalesce` | Return every row as a text-valued stream, using `text_value` when present and otherwise `numeric_value`. |

Example storage for a numeric stream:

| time | numeric_value | text_value |
| --- | ---: | --- |
| `t1` | `1.0` | null |
| `t2` | null | `Manual Control` |
| `t3` | `2.5` | null |

Read results:

```python
value_mode="default"   # [1.0, None, 2.5]
value_mode="numeric"   # [1.0, 2.5]
value_mode="text"      # ["Manual Control"]
value_mode="coalesce"  # ["1.0", "Manual Control", "2.5"]
```

`default` is the default to preserve existing behavior. Use `coalesce` when
you want a single complete stream for display or export. Use `numeric` when
doing numeric calculations and `text` when inspecting status/fallback rows.

## Query Shapes

`Query.dataframe()` supports narrow and wide shapes:

```python
query.dataframe(shape="narrow", value_mode="default")
query.dataframe(shape="wide", value_mode="coalesce")
```

Narrow query results expose split value columns:

```text
point_id | ref | time | value_numeric | value_text
```

Wide query results pivot numeric and text rows separately. If the same stream
has both numeric and text rows in a wide result, the text column is suffixed
with `_text` when needed to avoid overwriting the numeric column.

`Query.data()` returns a `DataObject`; it accepts the same `value_mode` at
construction time:

```python
data = query.data(value_mode="coalesce")
df = data.dataframe(shape="wide")
```

`value_mode` is applied when the `DataObject` materializes its timeseries data.

## Tabular Driver Value Assignment

CSV and XLSX drivers infer `value_kind` per stream using
`assign_stream_value_kind()` from `acquirium.Storage.values`.

For inferred streams, `value_kind` is the preferred/default storage column:
if any numeric value is observed in a stream, that stream is registered as
numeric. Text-only streams are assigned text.

Numeric rows go to `numeric_value`; unparseable rows go to `text_value`.

If a driver needs different semantics, for example numeric-looking enum codes
that should be treated as text, it should register those streams explicitly
with `value_kind="text"` instead of relying on data inference.
