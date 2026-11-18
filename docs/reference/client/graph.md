---
title: Graph
---

The graph holds the plant model and everything Acquirium knows about streams. It is stored as RDF. Every write names a `source_id`, the owner of the part of the graph that is changed. Use `"plant"` for the plant model. Drivers and stream registrations write under their own `source_id`, so replacing the plant model does not remove them.

| Item | Purpose |
|---|---|
| [`Acquirium.insert_graph`](#acquiriuminsert_graph) | Insert RDF text |
| [`Acquirium.insert_graph_file`](#acquiriuminsert_graph_file) | Insert an RDF file |
| [`Acquirium.sparql_update`](#acquiriumsparql_update) | Run a SPARQL update |
| [`Acquirium.validate_graph`](#acquiriumvalidate_graph) | Check the graph against the ontology shapes |
| [`Acquirium.graph_version`](#acquiriumgraph_version) | Version number of the graph |
| [`Acquirium.graph_status`](#acquiriumgraph_status) | Version numbers and rebuild state of the graph |
| [Reading the graph](#reading-the-graph) | `acq.query()` and `acq.client.sparql_query` |

## `Acquirium.insert_graph`

```python
Acquirium.insert_graph(rdf_graph, format='turtle', replace=True, *, source_id)
```

Inserts RDF text into the part of the graph owned by `source_id`.

**Parameters**

- `rdf_graph` (`str`): the RDF content itself, not a file path. For a file, use [`insert_graph_file`](#acquiriuminsert_graph_file).
- `format` (`str`, default `'turtle'`): format of the text: `"turtle"`, `"n3"`, `"xml"` or `"trix"`.
- `replace` (`bool`, default `True`): when `True`, everything this `source_id` wrote before is removed first. When `False`, the content is added to it.
- `source_id` (`str`, required, keyword only): owner of the content. Use `"plant"` for the plant model.

**Returns**

`None`.

**Raises**

- `TypeError`: when `rdf_graph` is not a string, or `source_id` is not given.
- `requests.exceptions.HTTPError`: when the server cannot parse the content. The message includes the parser's error.

**Example**

```python
acq.insert_graph("""
@prefix ex: <urn:ex:> .
@prefix s223: <http://data.ashrae.org/standard223#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:pump-1 a s223:Pump ;
    rdfs:label "Pump 1" ;
    s223:hasProperty ex:pump-1-flow .
ex:pump-1-flow a s223:QuantifiableObservableProperty ;
    rdfs:label "Pump 1 flow" .
""", source_id="plant")
```

## `Acquirium.insert_graph_file`

```python
Acquirium.insert_graph_file(path, format=None, replace=True, *, source_id)
```

Reads an RDF file on this computer and inserts its content, as [`insert_graph`](#acquiriuminsert_graph) does.

**Parameters**

- `path` (`str` or `Path`): path of the file.
- `format` (`str`, optional): format of the file. When omitted, it is taken from the file extension: `.ttl` is `"turtle"`, `.n3` is `"n3"`, `.xml` and `.rdf` are `"xml"`, `.trix` is `"trix"`.
- `replace` (`bool`, default `True`): when `True`, everything this `source_id` wrote before is removed first.
- `source_id` (`str`, required, keyword only): owner of the content. Use `"plant"` for the plant model.

**Returns**

`None`.

**Raises**

- `FileNotFoundError`: when `path` is not a file.
- `ValueError`: when `format` is omitted and the extension is not one of those listed.
- `requests.exceptions.HTTPError`: when the server cannot parse the content.

**Example**

```python
acq.insert_graph_file("plant.ttl", source_id="plant")
acq.insert_graph_file("plant.txt", source_id="plant")
```

```text
ValueError: cannot infer RDF format from '.txt'; pass format explicitly
```

## `Acquirium.sparql_update`

```python
Acquirium.sparql_update(update, *, source_id)
```

Runs a SPARQL update, such as `INSERT DATA` or `DELETE WHERE`, on the part of the graph owned by `source_id`. Statements owned by other sources and the ontologies are not changed.

**Parameters**

- `update` (`str`): the SPARQL update text.
- `source_id` (`str`, required, keyword only): owner of the part of the graph to change.

**Returns**

A `dict`. Note that `changed` is `True` for every update that runs, including one that matches nothing.

```python
{'ok': True, 'message': 'update applied', 'changed': True}
```

**Raises**

- `requests.exceptions.HTTPError`: when the update text is not valid SPARQL.

**Example**

```python
acq.sparql_update(
    'INSERT DATA { <urn:ex:pump-1> '
    '<http://www.w3.org/2000/01/rdf-schema#comment> "Main feed pump" }',
    source_id="plant",
)
```

```text
{'ok': True, 'message': 'update applied', 'changed': True}
```

## `Acquirium.validate_graph`

```python
Acquirium.validate_graph()
```

Checks all the data in the graph against the SHACL shapes of the loaded ontologies. This can take a while on a large plant model.

**Returns**

A `dict` with three keys:

- `conforms` (`bool`): `True` when no shape is violated.
- `results_text` (`str`): the report as readable text.
- `report` (`str`): the report as RDF in Turtle format.

**Example**

```python
result = acq.validate_graph()
print(result["conforms"])
print(result["results_text"])
```

```text
True
Validation Report
Conforms: True
```

## `Acquirium.graph_version`

```python
Acquirium.graph_version()
```

Returns the version number of the graph. The number goes up with every write to the graph, so comparing two readings shows whether the graph changed in between.

**Returns**

An `int`. It is the `source_version` of [`graph_status`](#acquiriumgraph_status).

**Example**

```python
print(acq.graph_version())
acq.insert_graph_file("plant.ttl", source_id="plant")
print(acq.graph_version())
```

```text
15
16
```

## `Acquirium.graph_status`

```python
Acquirium.graph_status()
```

Returns the version of the graph and whether queries already see it. After a write, the server rebuilds the copy of the graph that queries read. Until the rebuild finishes, queries see the previous version.

**Returns**

A `dict`:

```python
{'source_version': 16, 'published_version': 16, 'is_current': True,
 'rebuild_in_progress': False}
```

- `source_version` (`int`): version of the stored graph.
- `published_version` (`int`): version that queries read.
- `is_current` (`bool`): `True` when queries read the latest version.
- `rebuild_in_progress` (`bool`): `True` while the server is rebuilding.

## Reading the graph

The client reads the graph through the query builder that [`Acquirium.query`](querying.md#acquiriumquery) returns. See the [query reference](../query/_index.md).

`Acquirium` has no method for a raw SPARQL query. The lower-level client has one:

```python
acq.client.sparql_query(sparql, include_dependencies=True, *, wait_for_fresh=False)
```

- `sparql` (`str`): a `SELECT` or `ASK` query.
- `include_dependencies` (`bool`, default `True`): when `True`, the query also sees the ontologies. When `False`, it sees only the data.
- `wait_for_fresh` (`bool`, default `False`): when `True`, wait for a rebuild in progress so that the query sees the latest write.

It returns a `dict` with the column names and one list per row. An `ASK` query returns one row that holds `True` or `False`.

```python
acq.client.sparql_query(
    'SELECT ?label WHERE { <urn:ex:pump-1> '
    '<http://www.w3.org/2000/01/rdf-schema#label> ?label }'
)
```

```text
{'columns': ['label'], 'rows': [['Pump 1']], 'datatypes': [[None]]}
```
