---
title: Tabular helpers
---

Two plain functions turn a table read from a file into the rows a driver inserts. They do not need a driver or a server. The built-in file drivers use them, and a driver of your own can call them from `read` or `collect`.

| Item | Purpose |
|---|---|
| [`to_observations`](#to_observations) | Reshape a wide or narrow table into `ts`, `stream_name`, `value` rows |
| [`to_timestamp`](#to_timestamp) | Convert a timestamp column to UTC |

```python
import polars as pl
import acquirium as aq
```

## `to_observations`

```python
to_observations(df, *, time_col=None, date_col=None, clock_col=None,
                id_col='id', value_col='value', layout,
                date_format=None, timezone='UTC', day_first=False)
```

Reshapes a table into one row per value, with the columns `ts`, `stream_name` and `value`. The result can be passed to [`insert_observations`](helpers.md#ingestdriverinsert_observations) or returned in a [`FileBatch`](base-classes.md#filebatch).

**Parameters**

- `df` (`polars.DataFrame`): the table to reshape.
- `time_col` (`str`, optional): column that holds the full timestamp. Cannot be combined with `date_col` or `clock_col`.
- `date_col` (`str`, optional): column that holds the date, when date and time are in two columns. It can also be used alone.
- `clock_col` (`str`, optional): column that holds the time of day. Requires `date_col`.
- `id_col` (`str`): narrow layout only. Column that holds the stream name.
- `value_col` (`str`): narrow layout only. Column that holds the value.
- `layout` (`str`): required. `"wide"` means one column per stream and one row per time; every column that is not a timestamp column becomes a stream. `"narrow"` means one row per value.
- `date_format` (`str`, optional): `strptime` format of the timestamps. See [`to_timestamp`](#to_timestamp).
- `timezone` (`str`): time zone of timestamps that carry none.
- `day_first` (`bool`): see [`to_timestamp`](#to_timestamp).

When `time_col`, `date_col` and `clock_col` are all omitted, the timestamp columns are found by name. Upper and lower case, spaces and punctuation are ignored. The function looks for, in this order:

1. one column named `date` and one named `time`;
2. one pair of columns that end in `date` and `time` with the same beginning, for example `Sample Date` and `Sample Time`;
3. one column named `timestamp`, `datetime`, `ts`, `time` or `date`.

**Returns**

`polars.DataFrame` with the columns `ts` (`datetime[μs, UTC]`), `stream_name` (`str`) and `value` (`str`). Stream names are not changed. Rows whose timestamp cannot be read are dropped and a warning with their number is logged. Rows without a value are dropped.

**Raises**

- `ValueError`: when `layout` is not `"wide"` or `"narrow"`.
- `ValueError`: when `time_col` is combined with `date_col` or `clock_col`, or `clock_col` is given without `date_col`.
- `ValueError`: when a named column is not in `df`, or no timestamp column can be found by name.
- `ValueError`: when several date and time pairs are found by name.
- `TypeError`: when the timestamp column has a type that cannot be converted, see [`to_timestamp`](#to_timestamp).

**Example**

A wide table.

```python
wide = pl.DataFrame({
    "time": ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"],
    "temp": [22.5, 22.6],
    "rh": [55.0, None],
})
aq.to_observations(wide, layout="wide")
```

```text
shape: (3, 3)
┌─────────────────────────┬─────────────┬───────┐
│ ts                      ┆ stream_name ┆ value │
│ ---                     ┆ ---         ┆ ---   │
│ datetime[μs, UTC]       ┆ str         ┆ str   │
╞═════════════════════════╪═════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ temp        ┆ 22.5  │
│ 2026-01-01 00:01:00 UTC ┆ temp        ┆ 22.6  │
│ 2026-01-01 00:00:00 UTC ┆ rh          ┆ 55.0  │
└─────────────────────────┴─────────────┴───────┘
```

A narrow table with separate date and time columns in local time. The `Date` and `Time` columns are found by name.

```python
narrow = pl.DataFrame({
    "Date": ["2026-01-02", "2026-01-02"],
    "Time": ["08:00", "08:05"],
    "tag": ["FT-1", "FT-1"],
    "reading": ["1.2", "LOW"],
})
aq.to_observations(narrow, layout="narrow", id_col="tag", value_col="reading",
                   timezone="America/Los_Angeles")
```

```text
shape: (2, 3)
┌─────────────────────────┬─────────────┬───────┐
│ ts                      ┆ stream_name ┆ value │
│ ---                     ┆ ---         ┆ ---   │
│ datetime[μs, UTC]       ┆ str         ┆ str   │
╞═════════════════════════╪═════════════╪═══════╡
│ 2026-01-02 16:00:00 UTC ┆ FT-1        ┆ 1.2   │
│ 2026-01-02 16:05:00 UTC ┆ FT-1        ┆ LOW   │
└─────────────────────────┴─────────────┴───────┘
```

## `to_timestamp`

```python
to_timestamp(date_or_timestamp, time=None, *, date_format=None,
             timezone='UTC', day_first=False)
```

Converts a column of timestamps to UTC. The column may hold dates, datetimes, text, or whole numbers of seconds since 1970.

**Parameters**

- `date_or_timestamp` (`polars.Series`): the timestamp column, or the date column when `time` is given.
- `time` (`polars.Series`, optional): a time-of-day column. It is joined to the date with a space before parsing.
- `date_format` (`str`, optional): `strptime` format for text values, for example `"%d/%m/%Y %H:%M"`. When given, no other format is tried.
- `timezone` (`str`): time zone of values that carry none, for example `"America/Los_Angeles"`. Values that carry a time zone or an offset keep it.
- `day_first` (`bool`): prefer day before month when the fallback formats are tried. See the note below.

How each column type is read:

| Column type | Reading |
|---|---|
| Date, datetime | Used as is |
| Whole numbers | Time since 1970. The unit (seconds, milliseconds, microseconds or nanoseconds) is chosen from the size of the numbers |
| Text | With `date_format`, that format. Without it, the automatic parser of Polars is tried first; it reads ISO forms such as `2026-01-01T08:00:00Z` and `2026-01-01 08:00`. When it reads nothing, a list of common formats is tried and the one that reads the most values is used |

**Returns**

`polars.Series` of type `datetime[μs, UTC]` or another UTC datetime type. A value that cannot be read becomes null. A local time that does not exist or occurs twice because of a clock change also becomes null.

**Raises**

- `TypeError`: when the column type is not one of the above, for example a column of decimal numbers.

Note that one format is used for the whole column. Values in a different format become null.

Note that dates written with slashes, such as `01/02/2026 09:30`, are ambiguous. With Polars 1.44 the automatic parser accepts them and reads the day first, so this value becomes 1 February, with or without `day_first`. A column that also holds `12/25/2026 09:30` then loses that row. `day_first` only takes effect for forms the automatic parser does not read, such as `01/02/2026 09:30 AM`. Set `date_format` for any source that writes dates with slashes.

**Example**

```python
aq.to_timestamp(pl.Series("t", [1767225600, 1767225660])).to_list()
```

```text
[datetime.datetime(2026, 1, 1, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='UTC')), datetime.datetime(2026, 1, 1, 0, 1, tzinfo=zoneinfo.ZoneInfo(key='UTC'))]
```

A date column and a time column in local time.

```python
aq.to_timestamp(
    pl.Series("d", ["2026-01-01"]), pl.Series("c", ["08:00:00"]),
    timezone="America/Los_Angeles",
).to_list()
```

```text
[datetime.datetime(2026, 1, 1, 16, 0, tzinfo=zoneinfo.ZoneInfo(key='UTC'))]
```

An explicit format.

```python
aq.to_timestamp(pl.Series("d", ["13/02/2026"]), date_format="%d/%m/%Y")
```

```text
shape: (1,)
Series: 'd' [datetime[μs, UTC]]
[
	2026-02-13 00:00:00 UTC
]
```
