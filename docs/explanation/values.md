---
title: Values
---

In this article we'll describe how Acquirium stores values that can be numbers or text, and how `value_mode` and `cast_value` decide what you get back.

## Numbers and text

The values of a stream are not always of one type.
A flow meter that normally reports numbers can report "Bad" during a fault, and a status stream can carry "1" next to "ON".

To keep every value, each row of a stream has two value columns: one for a number and one for a text.
A value is stored in one of them, and the other one is empty.
This way a value that does not fit the stream is kept, and it does not change the type of the other values.

Each stream has a **value kind**, which is `"numeric"` or `"text"`.
It decides where a value goes:

- In a numeric stream, a value that can be read as a number is stored as a number. This includes text such as "13.0", for example from a CSV file. Any other text is stored as text.
- In a text stream, every value is stored as text, numbers included.
- A boolean is stored as the number `1.0` or `0.0` in a numeric stream, and as the text "True" or "False" in a text stream.
- A missing value (`None`) is stored as a row with no value. In a numeric stream, an empty text and the texts "NaN" and "inf" are stored the same way.

You do not have to give the value kind.
A stream registered without `value_kind` holds text until it receives its first number.
From then on it is a numeric stream.

Pass `"value_kind": "text"` or `"value_kind": "numeric"` to `register_streams()` to set the kind yourself.
This is useful for a status stream that carries "1" next to "ON" and should stay text.

**Note:** The value kind applies to the values inserted after it is set.
Values that are already stored are not moved to the other column.

## Choosing what is read

The stream below is registered without a value kind.
It becomes numeric with its first value, so "13.0" is stored as a number and "Bad" is stored as text.

```python
from datetime import datetime, timezone

def at(minute):
    return datetime(2026, 8, 8, 10, minute, tzinfo=timezone.utc)

acq.register_streams([{"source_id": "scada", "stream_name": "feed-flow"}])
acq.insert_timeseries("scada", "feed-flow", [
    (at(0), 12.5),
    (at(5), "13.0"),
    (at(10), "Bad"),
    (at(15), 14.0),
])

flow = acq.stream("scada", "feed-flow")
flow.dataframe()
```
```text
shape: (4, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-08-08 10:00:00 UTC ┆ 12.5  │
│ 2026-08-08 10:05:00 UTC ┆ 13.0  │
│ 2026-08-08 10:10:00 UTC ┆ null  │
│ 2026-08-08 10:15:00 UTC ┆ 14.0  │
└─────────────────────────┴───────┘
```

By default a read follows the value kind of the stream.
The stream is numeric, so the numbers are returned and the row that holds "Bad" has no value.

`value_mode` changes which values are read:

- `"default"` follows the value kind of the stream.
- `"numeric"` returns only the rows that hold a number.
- `"text"` returns only the rows that hold a text.
- `"coalesce"` returns every value as text, so nothing is left out.

For the stream above:

| `value_mode` | values |
|---|---|
| `"default"` | `12.5`, `13.0`, null, `14.0` |
| `"numeric"` | `12.5`, `13.0`, `14.0` |
| `"text"` | `"Bad"` |
| `"coalesce"` | `"12.5"`, `"13.0"`, `"Bad"`, `"14.0"` |

```python
flow.dataframe(value_mode="coalesce")
```
```text
shape: (4, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ str   │
╞═════════════════════════╪═══════╡
│ 2026-08-08 10:00:00 UTC ┆ 12.5  │
│ 2026-08-08 10:05:00 UTC ┆ 13.0  │
│ 2026-08-08 10:10:00 UTC ┆ Bad   │
│ 2026-08-08 10:15:00 UTC ┆ 14.0  │
└─────────────────────────┴───────┘
```

`value_mode` is also a parameter of `dataframe()` and `data()` of a query.
Note that the wide table of a query leaves out the rows that have no value in any column.

## Changing the type after the read

`cast_value` is a parameter of `dataframe()` and `data()` of a query.
It is applied by the client, after the values arrive from the server.
The default is `None`, which leaves the values as they are.

The status stream below is a text stream, so the numbers in it are stored as text:

```python
acq.register_streams([{"source_id": "scada", "stream_name": "pump-status", "value_kind": "text"}])
acq.insert_timeseries("scada", "pump-status", [
    (at(0), "ON"),
    (at(5), 1),
    (at(10), 0),
    (at(15), "OFF"),
])

status = acq.query().measurement(stream=acq.stream("scada", "pump-status"))
status.dataframe()
```
```text
shape: (4, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__pump-status │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ str               │
╞═════════════════════════╪═══════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ ON                │
│ 2026-08-08 10:05:00 UTC ┆ 1                 │
│ 2026-08-08 10:10:00 UTC ┆ 0                 │
│ 2026-08-08 10:15:00 UTC ┆ OFF               │
└─────────────────────────┴───────────────────┘
```

`cast_value="float"` turns the text that can be read as a number into a number.
The rest stays text, in a second column:

```python
status.dataframe(cast_value="float")
```
```text
shape: (4, 3)
┌─────────────────────────┬───────────────────┬────────────────────────┐
│ time                    ┆ data__pump-status ┆ data__pump-status_text │
│ ---                     ┆ ---               ┆ ---                    │
│ datetime[μs, UTC]       ┆ f64               ┆ str                    │
╞═════════════════════════╪═══════════════════╪════════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ null              ┆ ON                     │
│ 2026-08-08 10:05:00 UTC ┆ 1.0               ┆ null                   │
│ 2026-08-08 10:10:00 UTC ┆ 0.0               ┆ null                   │
│ 2026-08-08 10:15:00 UTC ┆ null              ┆ OFF                    │
└─────────────────────────┴───────────────────┴────────────────────────┘
```

The other options are:

- `"int"` works like `"float"` and returns whole numbers. The fraction is cut off, and a `UserWarning` names a value that was changed.
- `"str"` returns every value as text. Be aware that `convert_to()` needs numbers, so it raises a `ValueError` on such a result.

For instance, `cast_value="int"` on the `feed-flow` stream gives this warning:

```text
UserWarning: cast_value='int' truncates 1 non-integer value(s) in column 'data__feed-flow', such as 12.5; use cast_value='float' to keep them
```

## Further reading

- [`Query.data`](../reference/query/reading.md#querydata) and [`Query.dataframe`](../reference/query/reading.md#querydataframe): the parameters of a read.
- [`register_streams`](../reference/client/streams.md#acquiriumregister_streams): the keys of a stream, including `value_kind`.
- [Units](units.md): how numeric values are converted between units.
