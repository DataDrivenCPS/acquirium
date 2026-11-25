---
title: Query
---

A `Query` describes a pattern of nodes in the plant model: entities, properties and the streams connected to them.
Builder methods add nodes and filters and return a new `Query`. Reading methods run the pattern and return tables, Python objects or a `DataObject` that holds the time series.
This section lists every public method of `Query`, the attribute expressions used in filters, and the objects a query returns.

## Pages

| Page | Contents |
| --- | --- |
| [Building a query](building.md) | Start a query and add entity, property and measurement nodes |
| [Filtering](filtering.md) | `where`, attribute expressions and the built-in attributes |
| [Columns](columns.md) | Choose the columns of the metadata table |
| [Options and facets](facets.md) | Count the attribute values of the matched nodes |
| [Reading results](reading.md) | Run a query and read metadata or time series |
| [Inspecting a query](inspecting.md) | Print a query, see its SPARQL, export it |
| [Writing from a query](writing.md) | Write metadata and logbook notes on the matched nodes |
| [DataObject: values](data-object.md) | Read, group and convert the time series of a query |
| [DataObject: streams and statistics](data-object-info.md) | Describe the streams behind a `DataObject` |
| [Traversal settings](traversal.md) | Hidden predicates and named relations |

## Methods

### Building

| Method | Description |
| --- | --- |
| [`Acquirium.query`](building.md#acquiriumquery) | Create an empty query |
| [`Query.entity`](building.md#queryentity) | Add an entity node |
| [`Query.related`](building.md#queryrelated) | Add an entity related to an existing node |
| [`Query.measurement`](building.md#querymeasurement) | Add a measurement node: properties that have a stream |
| [`Query.property`](building.md#queryproperty) | Add a property node, with or without a stream |
| [`Query.context`](building.md#querycontext) | Add the entity a measurement belongs to |
| [`Query.alias`](building.md#queryalias) | Name the current node |
| [`Query.refocus`](building.md#queryrefocus) | Make another node the current node |

### Filtering

| Name | Description |
| --- | --- |
| [`Query.where`](filtering.md#querywhere) | Filter a node by its attributes |
| [`Acquirium.attr`](filtering.md#acquiriumattr) | Start an attribute expression |
| [Comparisons](filtering.md#comparisons) | `==`, `!=`, `<`, `<=`, `>`, `>=` on an attribute |
| [`is_in`](filtering.md#is_in) | The attribute has one of several values |
| [`exists`](filtering.md#exists) | The node has the attribute |
| [`&`, `\|`, `~`](filtering.md#combining-expressions) | Combine expressions with and, or, not |
| [`Not`](filtering.md#not) | Negate a keyword filter |
| [Built-in attributes](filtering.md#built-in-attributes) | The attributes every instance knows |

### Columns

| Method | Description |
| --- | --- |
| [`Query.include`](columns.md#queryinclude) | Add attribute columns, or show a hidden node again |
| [`Query.drop`](columns.md#querydrop) | Hide a node column, or remove an attribute column |
| [`Query.with_columns`](columns.md#querywith_columns) | Include and drop in one call |
| [`Query.schema`](columns.md#queryschema) | The column names and types `metadata()` will return |

### Options and facets

| Name | Description |
| --- | --- |
| [`Query.options`](facets.md#queryoptions) | Values of one attribute on the matched nodes, with counts |
| [`Query.facets`](facets.md#queryfacets) | Value counts for every attribute of a node |
| [`FacetSummary`](facets.md#facetsummary) | The object `facets()` returns |

### Reading results

| Method | Description |
| --- | --- |
| [`Query.metadata`](reading.md#querymetadata) | The matches as a table |
| [`Query.metadata_objects`](reading.md#querymetadata_objects) | The matches as Python dicts |
| [`Query.resolved_nodes`](reading.md#queryresolved_nodes) | The URIs of the matched nodes |
| [`Query.data`](reading.md#querydata) | A `DataObject` for the matched streams |
| [`Query.dataframe`](reading.md#querydataframe) | The time series of the matched streams as one table |

### Inspecting

| Method | Description |
| --- | --- |
| [`Query.summary`](inspecting.md#querysummary) | One line per node of the query |
| [`print(q)`](inspecting.md#printing-a-query) | The summary and the first five matches |
| [`Query.to_sparql`](inspecting.md#queryto_sparql) | The SPARQL text of the query |
| [`Query.execute`](inspecting.md#queryexecute) | The raw result of the SPARQL query |
| [`Query.to_dict`](inspecting.md#queryto_dict) | The query as a JSON-serializable dict |

### Writing

| Method | Description |
| --- | --- |
| [`Query.insert_metadata`](writing.md#queryinsert_metadata) | Write the same metadata on every matched node |
| [`Query.insert_log`](writing.md#queryinsert_log) | Write a logbook note on every matched node |
| [`Query.read_logs`](writing.md#queryread_logs) | Read the logbook notes of the matched nodes |

### DataObject

| Name | Description |
| --- | --- |
| [`DataObject.aliases`](data-object.md#dataobjectaliases) | The data aliases in the object |
| [`DataObject[alias]`](data-object.md#dataobjectalias) | The time series of one alias |
| [`DataObject.dataframe`](data-object.md#dataobjectdataframe) | All time series as one table |
| [`DataObject.latest`](data-object.md#dataobjectlatest) | The newest reading of each property |
| [`DataObject.by`](data-object.md#dataobjectby) | Split the object by entity |
| [`DataObject.units`](data-object.md#dataobjectunits) | The units of each alias |
| [`DataObject.convert_to`](data-object.md#dataobjectconvert_to) | Convert values to another unit |
| [`DataObject.metadata`](data-object-info.md#dataobjectmetadata) | One row per matched property |
| [`DataObject.stream_info`](data-object-info.md#dataobjectstream_info) | One row per stream |
| [`DataObject.entity_aliases`](data-object-info.md#dataobjectentity_aliases) | The entity aliases of the query |
| [`DataObject.is_empty`](data-object-info.md#dataobjectis_empty) | Whether the object has any rows |
| [`DataObject.total_rows`](data-object-info.md#dataobjecttotal_rows) | Number of stored rows |
| [`DataObject.time_range`](data-object-info.md#dataobjecttime_range) | Earliest and latest stored timestamps |
| [`DataObject.bindings`](data-object-info.md#dataobjectbindings) | One record per matched stream |
| [`DataObject.refresh_stats`](data-object-info.md#dataobjectrefresh_stats) | Forget the cached statistics |

### Traversal settings

| Function | Description |
| --- | --- |
| [`hide`](traversal.md#hide) | Stop `related()` from following a predicate |
| [`unhide`](traversal.md#unhide) | Let `related()` follow a predicate again |
| [`hidden_predicates`](traversal.md#hidden_predicates) | The predicates currently hidden |
| [`hidden_prefixes`](traversal.md#hidden_prefixes) | The namespaces currently hidden |
| [`register_relation`](traversal.md#register_relation) | Add a named relation for `context(via=)` |
| [`unregister_relation`](traversal.md#unregister_relation) | Remove a named relation |
| [`reset_relations`](traversal.md#reset_relations) | Restore the default relations |

## Terms

- **Node**: one part of the query pattern. A query has entity nodes, property nodes and measurement nodes.
- **Alias**: the name of a node. It is the column name in the metadata table and the handle other methods take in `frm=`, `of=` and `target=`.
- **Current node**: the node added last. Methods act on it unless another alias is given. [`Query.refocus`](building.md#queryrefocus) changes it.
- **Measurement node**: a node that matches properties with a stream connected. Only measurement nodes return time series.
- **Attribute**: a named value of a node, such as `unit` or `label`. See [Built-in attributes](filtering.md#built-in-attributes).

Every builder method returns a new `Query` and leaves the original unchanged. A query can be kept in a variable and extended in several ways.

## Example setup

The examples in this section use the example plant that ships with the package, with one day of generated data.
Stream registration and inserts are documented in [streams](../client/streams.md), [timeseries](../client/timeseries.md), [graph](../client/graph.md) and [metadata](../client/metadata.md).

```python
from datetime import datetime, timezone

import acquirium as aq
from acquirium.examples import seawater_ro

acq = aq.init()
acq.insert_graph_file(seawater_ro.model_path, source_id="plant")

start = datetime(2026, 1, 1, tzinfo=timezone.utc)
end = datetime(2026, 1, 2, tzinfo=timezone.utc)
data = seawater_ro.generate(start, end)

acq.register_streams([
    {"source_id": "plant-data", "stream_name": name, "property_uri": "urn:swro/" + name}
    for name in data
])
acq.insert_timeseries_batch("plant-data", data)

acq.insert_metadata("wbs:P1", {
    "product_info": {"manufacturer": "Grundfos", "year": 2019},
    "tags": ["critical", "high-pressure"],
})
acq.insert_metadata("wbs:P2", {
    "product_info": {"manufacturer": "Siemens", "year": 2012},
    "tags": ["booster"],
})
```

The model has three pumps (`wbs:intake`, `wbs:P1`, `wbs:P2`) and 32 properties, each with one stream.
The order of rows in a metadata table is not fixed. It can differ from the outputs shown here.
