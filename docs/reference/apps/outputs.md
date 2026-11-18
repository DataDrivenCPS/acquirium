---
title: Outputs
---

An app declares its output streams in the class attribute `outputs` and fills them inside `transform`. Each entry of `outputs` has a port name, which is the key used in `transform`, and a declaration made with `aq.output.stream(...)` or `aq.output.named(...)`.

| Item | Purpose |
|---|---|
| [`output.stream`](#outputstream) | Declare an output whose stream name is generated |
| [`output.named`](#outputnamed) | Declare an output with a stream name you choose |
| [`OutputSpec`](#outputspec) | The declaration object both functions return |
| [`OutputBuilder`](#outputbuilder) | The `output` argument of `transform` |
| [How output streams are stored](#how-output-streams-are-stored) | `source_id`, `stream_name`, label |

```python
import acquirium as aq


class Example(aq.App):
    grouping = "all_matches"
    outputs = {
        "celsius": aq.output.stream(value_kind="numeric", unit="http://qudt.org/vocab/unit/DEG_C"),
        "total": aq.output.named("plant-total", value_kind="numeric"),
    }
```

## `output.stream`

```python
output.stream(**kwargs)
```

Declares an output whose `stream_name` is generated from the app name, the port name and the input streams of the call. With `grouping = "per_match"` every match gets its own output stream.

**Parameters**

- `value_kind` (`str`): required. `"numeric"` or `"text"`.
- `property_uri` (`str`, optional): URI of an existing property in the graph that the output belongs to. When omitted, a property is created for the output.
- `label` (`str`, optional): readable name. When omitted, a label is generated, see [How output streams are stored](#how-output-streams-are-stored).
- `unit` (`str`, optional): unit, as a URI.
- `quantity_kind` (`str`, optional): quantity kind, as a URI.
- `medium` (`str`, optional): medium, as a URI.
- `substance` (`str`, optional): substance, as a URI.
- `data_source` (`str`, optional): a tag that queries can filter on with `measurement(data_source=...)`.
- `properties` (`dict`, optional): extra graph statements for the output, as a mapping of a predicate URI to a tuple of object URIs.

Note that `unit`, `quantity_kind`, `medium` and `substance` are written to the graph as given. Unlike [`declare`](../drivers/helpers.md#ingestdriverdeclare) in a driver, plain text such as `"psi"` is not matched to a URI.

**Returns**

[`OutputSpec`](#outputspec).

**Raises**

- `TypeError`: when `stream_name` is passed. Use `output.named` for that.
- `TypeError`: when `value_kind` is missing.
- `ValueError`: when `value_kind` is not `"numeric"` or `"text"`.

**Example**

```python
aq.output.stream(value_kind="numeric", unit="http://qudt.org/vocab/unit/DEG_C")
```

```text
OutputSpec(value_kind='numeric', property_uri=None, label=None, unit='http://qudt.org/vocab/unit/DEG_C', quantity_kind=None, medium=None, substance=None, data_source=None, properties=None, stream_name=None)
```

## `output.named`

```python
output.named(stream_name, **kwargs)
```

Declares an output with a fixed `stream_name`. The stream keeps this name when the inputs of the app change. This is useful for a single result such as a plant total.

**Parameters**

- `stream_name` (`str`): name of the output stream under the `source_id` `derived:<app name>`.
- `**kwargs`: the same keyword arguments as [`output.stream`](#outputstream). `value_kind` is required.

**Returns**

[`OutputSpec`](#outputspec).

**Raises**

- `ValueError`: when `stream_name` is empty, or `value_kind` is not `"numeric"` or `"text"`.
- `TypeError`: when `value_kind` is missing.

Note that a named output is one stream with one writer. Two ports of an app cannot use the same name. An app with `grouping = "per_match"` and a named output cannot be deployed when its query has more than one match. Compute such a result in a second app with `grouping = "all_matches"`.

**Example**

```python
aq.output.named("plant-total", value_kind="numeric")
```

```text
OutputSpec(value_kind='numeric', property_uri=None, label=None, unit=None, quantity_kind=None, medium=None, substance=None, data_source=None, properties=None, stream_name='plant-total')
```

## `OutputSpec`

```python
OutputSpec(value_kind, property_uri=None, label=None, unit=None,
           quantity_kind=None, medium=None, substance=None,
           data_source=None, properties=None, stream_name=None)
```

The declaration of one output stream. `output.stream` and `output.named` create it; calling the class directly gives the same result. Instances cannot be changed after creation.

**Parameters**

The parameters are described under [`output.stream`](#outputstream). `stream_name` is described under [`output.named`](#outputnamed).

The old name `point_uri` is accepted for `property_uri`.

**Raises**

- `ValueError`: when `value_kind` is not `"numeric"` or `"text"`, or `stream_name` is an empty string.

## `OutputBuilder`

```python
output[port] = table
```

The `output` argument of [`transform`](app-class.md#apptransform). Assign one table to each port that this call computes. The app does not create this object.

**Parameters**

- `port` (`str`): a key of the class attribute `outputs`.
- `table`: a Polars dataframe, a pandas dataframe, a PyArrow table, a PyArrow record batch, or a list of record batches. It must have exactly two columns, `time` and `value`.

Rules for the table:

| Column | Rule |
|---|---|
| `time` | Timestamps with a time zone. No empty values. No repeated times |
| `value` | No empty values. Numbers for a `"numeric"` output, strings for a `"text"` output |

What an assignment does when the app is deployed:

| In `transform` | Effect on the stored output |
|---|---|
| A table with rows is assigned | Stored values in [`context.output_window`](app-class.md#time-windows-of-a-call) are replaced by the rows. Rows outside the window are dropped |
| An empty table is assigned | Stored values in the window are removed |
| The port is not assigned | Stored values are left as they are |

**Raises**

- `KeyError`: when `port` is not declared in `outputs`.
- `ValueError`: when the same port is assigned twice in one call.
- `TypeError`: when the value is not a table with exactly the columns `time` and `value`, or the values do not fit the declared `value_kind`.
- `ValueError`: when a time is missing, has no time zone or is repeated, or a value is missing.

**Example**

```python
def transform(self, inputs, output, context):
    df = inputs["temp"].df()
    output["celsius"] = df.select("time", "value")
```

Assigning a wrong table raises inside `transform`. The message names the port, for example:

```text
TypeError: output 'doubled': numeric output requires numeric values
ValueError: output 'doubled': output timestamps must be unique
KeyError: "output 'nope' is not declared in this app's outputs (declared: 'sum')"
```

## How output streams are stored

Output streams are ordinary streams. They can be read and queried like any other, see [Streams](../client/streams.md) and [Building queries](../query/building.md).

| Property of the stream | Value |
|---|---|
| `source_id` | `derived:<app name>` |
| `stream_name`, from `output.named` | The name given |
| `stream_name`, from `output.stream` | `<app name>:<port>:<hash of the input streams>`. It stays the same when the code of the app changes, and changes when the input streams change |
| Label, when `label` is given | That label |
| Label, when `property_uri` is given without `label` | None is written; the property keeps the label it has |
| Label of a named output without `label` | The stream name |
| Label, when a call has one input stream that has a label | `<input label> (<app name>[<port>])` |
| Label, otherwise | `<app name>[<port>]` |
| Stored type | Numeric values as 64-bit floats, text values as strings. Times in UTC |

Each output is tagged with the name of its app. A query selects the outputs of one app with `measurement(app="<app name>")`:

```python
acq.query().measurement(app="doubler").metadata()
```

```text
shape: (2, 2)
┌─────────────────────────────────┬─────────────────────────────────┐
│ data                            ┆ data.label                      │
│ ---                             ┆ ---                             │
│ str                             ┆ str                             │
╞═════════════════════════════════╪═════════════════════════════════╡
│ ns1:3f944465-d988-5dde-9466-ca… ┆ Tank temperature (doubler[doub… │
│ ns1:49a59b2a-533d-5475-a281-c3… ┆ flow (doubler[doubled])         │
└─────────────────────────────────┴─────────────────────────────────┘
```
