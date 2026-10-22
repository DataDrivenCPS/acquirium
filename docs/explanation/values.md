---
title: Values
---

<!-- TODO: intro -->

## Numbers and text

Even if a stream is known to be numeric or text, sometimes values are not consistent.
A flow meter that normally reports numbers can report `"Bad"` or `"NaN"` during a fault, and a status stream can carry `"1"` next to `"ON"`.
To avoid data loss, we keep data in two columns for each `(stream, timestamp)` row: `numeric_value` and `text_value`.
Exactly one of them holds the reading; the other is null.
This lets us store a row of any type without changing the column type of the stream, and without dropping the rows that do not fit.

A stream is declared numeric or text when it is registered, and readings are
stored in the matching column: numbers in `numeric_value`, everything else in
`text_value`.
That declaration is called `value_kind`, and it is set by whoever writes the
data, not by the reader.
When a driver does not declare it, it is inferred from the first observed
values: a stream with any numeric value is `numeric`, otherwise `text`.

On a numeric stream, a value is stored as a number when it parses as one
(`"12.5"` counts), including numeric strings coming from a CSV.
A reading that cannot be parsed as a number falls back to the text column
even on a numeric stream, so a bad row does not affect the type of the
others.
Booleans are stored as text (`"True"`), and blank, `NaN` and infinite values are
stored as a row with both columns null.
On a text stream every value is stored as text, numbers included.

When reading, `value_mode` decides which of those columns you get.

| value_mode | you get |
|---|---|
| `"default"` | the column the stream was registered as; for a stream with no registered kind, the queried range is probed: numeric-only reads as numbers, text-only as text, and a mix as `"coalesce"` |
| `"numeric"` | numeric readings only, text rows filtered out |
| `"text"` | text readings only, numeric rows filtered out |
| `"coalesce"` | both columns as one string column, so no row is lost |

**TODO:** I'm not sure how reliable this is, I need to check.


```python
d = q.data(value_mode="numeric")
```

### cast_value

`cast_value` is the last step, applied client side after the values arrive.
It takes `"float"`, `"int"`, `"str"` or `None`; anything else is a
`ValueError`. The default is `None`, which leaves the columns exactly as the
server sent them.

`"float"` parses the values of a text column that read as numbers into
`value_numeric` and leaves the rest in `value_text`, so a status stream
carrying `"12.5"` next to `"ON"` keeps both.
A numeric column is already `Float64` and is left alone.

`"int"` does the same parsing and returns the value columns as `Int64`.
The cast truncates toward zero, so a column that holds fractional values
raises a `UserWarning` naming the column and one of the values it changed.

`"str"` sends every value to `value_text` as a string, numbers included.
Unit conversion needs numbers, so `convert_to()` refuses such an object.

All of these are vectorized polars casts; none of them loops over rows.
