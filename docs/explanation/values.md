---
title: Values
---

## Numbers and text

Each reading is a finite number or text. A stream can hold both: a flow meter
that normally reports numbers can report `"Bad"` during a fault, and keeping
that row as text means a bad reading is not lost and does not change the type
of the others. Escapement stores the readings; Acquirium decides how each
value is stored.

A stream is declared numeric or text when it is registered. That declaration
is called `value_kind`, and it is set by whoever writes the data, not by the
reader. When a driver does not declare it, it is inferred from the first
observed values: a stream with any numeric value is `numeric`, otherwise
`text`. A stream registered without one is numeric.

On a numeric stream, a value is stored as a number when it parses as one
(`"12.5"` counts), including numeric strings coming from a CSV. Anything else
is stored as text, even on a numeric stream. Booleans are stored as text
(`"True"`). On a text stream every value is stored as text, numbers included.
Null, blank, `NaN` and infinite values are not readings: those rows are
dropped. To remove a stored reading, delete it explicitly by stream and
timestamp.

## Reading values

When reading, `value_mode` decides which readings you get.

| value_mode | you get |
|---|---|
| `"default"` | the stream's declared kind: numbers on a numeric stream, text on a text stream |
| `"numeric"` | numeric readings only, as `Float64` |
| `"text"` | text readings only, as strings |
| `"coalesce"` | every reading as a string, so no row is left out |

```python
d = q.data(value_mode="numeric")
```

## In apps

Native functions (`mean`, `max`, `threshold`, and so on) read only the numbers
in their input; text readings do not take part. A Python function's frame has
a `value` column for numbers and a `text` column for text, exactly one set per
row, so Polars aggregations such as `mean` skip text rows on their own. A
function may emit text, for example `emit(context, "anoxic")`; declare that
port with `Output(value_kind="text")` so default reads return it.

### cast_value

`cast_value` is the last step, applied client side after the values arrive.
`"float"` casts the value column to `Float64` and `"int"` casts to `Int64`.
Anything else, including `None`, leaves the column exactly as the server sent
it.
