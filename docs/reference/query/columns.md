---
title: Columns
---

The metadata table of a query has one column per node. These methods add attribute columns, hide node columns and report the column types.
The examples use the [example setup](_index.md#example-setup).

| Method | Description |
| --- | --- |
| [`Query.include`](#queryinclude) | Add attribute columns, or show a hidden node again |
| [`Query.drop`](#querydrop) | Hide a node column, or remove an attribute column |
| [`Query.with_columns`](#querywith_columns) | Include and drop in one call |
| [`Query.schema`](#queryschema) | The column names and types `metadata()` will return |

A column is named in one of these ways:

- An attribute name, such as `"unit"`. It refers to the current node, or to the node given with `of=`.
- `"alias.attribute"`, such as `"pump.label"`. It refers to the attribute of that node.
- A node alias, such as `"pump"`. It refers to the column of the node itself.
- An attribute path from [`acq.attr`](filtering.md#acquiriumattr), such as `acq.attr.product_info.year`.

When a name is both an attribute and an alias, it is read as the attribute.

## `Query.include`

```python
Query.include(*names, of=None, required=False, type=None, strict=True)
```

Adds attribute columns to the metadata table. The column is named `<alias>.<attribute>`. Given a node alias, it shows a node hidden with [`Query.drop`](#querydrop) again.

**Parameters**

- `*names` (`str`): the columns to add. `"all"` adds the usual attributes of the node. It leaves out `type`, `cp_type`, `id` and `is_disconnected`, and also `app` and `label` on a measurement node.
- `of` (`str | None`, default `None`): the alias of the node the attribute names refer to. The default is the current node.
- `required` (`bool`, default `False`): `False` keeps every row and puts null where a node has no value. `True` keeps only the rows that have a value.
- `type` (`str | type | None`, default `None`): cast the named columns to this type. One of `"string"`, `"int"`, `"float"`, `"bool"`, `"date"`, `"datetime"`, `"struct"`, `"object"`, or the matching Python type or polars dtype. Lists are cast element by element.
- `strict` (`bool`, default `True`): with `type`, `True` raises an error on a value that cannot be cast, and `False` puts null in its place. The error is raised when the query runs.

Column types come from the stored values:

- A concept, such as a unit or a class, is a string that holds a CURIE.
- Other values keep their stored type: an integer is `Int64`, a date is `Date`, a boolean is `Boolean`, text is `String`.
- A list is one `List` cell per node.
- A group of keys, such as `product_info`, is one `Struct` cell per node.
- A column whose values do not share one type is `Object`. Each cell holds the value as it was written. Use `type=` to get one type.

A node with several values of a built-in attribute, such as two media, gives one row per value.

**Returns**

`Query`: a new query with the columns added.

**Raises**

- `ValueError`: when no name is given.
- `ValueError`: when a name is not an attribute or an alias of the query, or `of` is not an alias of the query.
- `ValueError`: when an attribute does not apply to a measurement node.
- `ValueError`: when `type` is not a known type.

**Example**

```python
q = acq.query().entity("Pump", alias="pump")
q.include("label", "product_info.manufacturer").metadata()
```

```text
shape: (3, 3)
┌────────────┬─────────────────────────┬────────────────────────────────┐
│ pump       ┆ pump.label              ┆ pump.product_info.manufacturer │
│ ---        ┆ ---                     ┆ ---                            │
│ str        ┆ str                     ┆ str                            │
╞════════════╪═════════════════════════╪════════════════════════════════╡
│ wbs:intake ┆ Seawater Onshore Intake ┆ null                           │
│ wbs:P2     ┆ Booster Pump P2         ┆ Siemens                        │
│ wbs:P1     ┆ High-pressure Pump P1   ┆ Grundfos                       │
└────────────┴─────────────────────────┴────────────────────────────────┘
```

A list, on the rows that have one:

```python
q.include("tags", required=True).metadata()
```

```text
shape: (2, 2)
┌────────┬───────────────────────────────┐
│ pump   ┆ pump.tags                     │
│ ---    ┆ ---                           │
│ str    ┆ list[str]                     │
╞════════╪═══════════════════════════════╡
│ wbs:P2 ┆ ["booster"]                   │
│ wbs:P1 ┆ ["critical", "high-pressure"] │
└────────┴───────────────────────────────┘
```

A group of keys:

```python
q.include("product_info").metadata()
```

```text
shape: (3, 2)
┌────────────┬───────────────────┐
│ pump       ┆ pump.product_info │
│ ---        ┆ ---               │
│ str        ┆ struct[2]         │
╞════════════╪═══════════════════╡
│ wbs:intake ┆ null              │
│ wbs:P2     ┆ {"Siemens",2012}  │
│ wbs:P1     ┆ {"Grundfos",2019} │
└────────────┴───────────────────┘
```

A cast:

```python
q.include("product_info.year", type="float").metadata()
```

```text
shape: (3, 2)
┌────────────┬────────────────────────┐
│ pump       ┆ pump.product_info.year │
│ ---        ┆ ---                    │
│ str        ┆ f64                    │
╞════════════╪════════════════════════╡
│ wbs:intake ┆ null                   │
│ wbs:P2     ┆ 2012.0                 │
│ wbs:P1     ┆ 2019.0                 │
└────────────┴────────────────────────┘
```

An attribute of another node:

```python
q2 = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
q2.include("unit", "pump.label").metadata()
```

```text
shape: (2, 5)
┌────────┬───────────────────────┬─────────────────────────┬─────────────────────┬────────┐
│ pump   ┆ pump.label            ┆ m                       ┆ m.label             ┆ m.unit │
│ ---    ┆ ---                   ┆ ---                     ┆ ---                 ┆ ---    │
│ str    ┆ str                   ┆ str                     ┆ str                 ┆ str    │
╞════════╪═══════════════════════╪═════════════════════════╪═════════════════════╪════════╡
│ wbs:P2 ┆ Booster Pump P2       ┆ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ unit:W │
│ wbs:P1 ┆ High-pressure Pump P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ unit:W │
└────────┴───────────────────────┴─────────────────────────┴─────────────────────┴────────┘
```

## `Query.drop`

```python
Query.drop(*names)
```

Hides a node column or removes an attribute column. A hidden node stays part of the pattern and still limits the matches. Rows that differed only in the hidden column become one row.

**Parameters**

- `*names` (`str`): the columns to drop. A node alias hides that node. An attribute name removes that column of the current node, and `"alias.attribute"` removes it from another node. With no names, the current node is hidden.

**Returns**

`Query`: a new query with the columns removed.

**Raises**

- `ValueError`: when a name is not an attribute or an alias of the query.
- `ValueError`: when no name is given and the query has no nodes.

**Example**

```python
q2 = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
q2.drop("pump").metadata()
```

```text
shape: (2, 2)
┌─────────────────────────┬─────────────────────┐
│ m                       ┆ m.label             │
│ ---                     ┆ ---                 │
│ str                     ┆ str                 │
╞═════════════════════════╪═════════════════════╡
│ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1-mechanical-power ┆ P1 mechanical power │
└─────────────────────────┴─────────────────────┘
```

## `Query.with_columns`

```python
Query.with_columns(*specs, of=None, required=False, type=None, strict=True)
```

Includes and drops columns in one call. A plain name is passed to [`Query.include`](#queryinclude). A name with a leading `"-"` is passed to [`Query.drop`](#querydrop).

**Parameters**

- `*specs` (`str`): column names, each with an optional leading `"-"`.
- `of`, `required`, `type`, `strict`: passed to `include` for the names without `"-"`.

**Returns**

`Query`: a new query with the columns changed.

**Raises**

- `ValueError`: when no spec is given.
- The errors of `include` and `drop`.

**Example**

```python
q2.with_columns("unit", "pump.label", "-pump").metadata()
```

```text
shape: (2, 4)
┌───────────────────────┬─────────────────────────┬─────────────────────┬────────┐
│ pump.label            ┆ m                       ┆ m.label             ┆ m.unit │
│ ---                   ┆ ---                     ┆ ---                 ┆ ---    │
│ str                   ┆ str                     ┆ str                 ┆ str    │
╞═══════════════════════╪═════════════════════════╪═════════════════════╪════════╡
│ Booster Pump P2       ┆ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ unit:W │
│ High-pressure Pump P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ unit:W │
└───────────────────────┴─────────────────────────┴─────────────────────┴────────┘
```

## `Query.schema`

```python
Query.schema()
```

Returns the column names and types that [`Query.metadata`](reading.md#querymetadata) will return, without running the query.
The two agree with one exception: `metadata()` leaves out an `<alias>.label` column when no matched property has a label.

**Parameters**

None.

**Returns**

`polars.Schema`: column names mapped to polars types. Node columns are `String`. A measurement node also has an `<alias>.label` column.

**Example**

```python
q = acq.query().entity("Pump", alias="pump")
q.include("product_info", "tags").schema()
```

```text
Schema({'pump': String, 'pump.product_info': Struct({'manufacturer': String, 'year': Int64}), 'pump.tags': List(String)})
```
