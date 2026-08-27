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
`"float"` casts the value column to `Float64` and `"int"` casts to `Int64`.
Anything else, including `None`, leaves the column exactly as the server sent
it, which for a numeric stream is already `Float64`.

Note that the defaults differ: `Query.data()` casts to `"float"`, while
`Query.dataframe()` passes `"str"`, which is one of the values that does
nothing.
