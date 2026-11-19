---
title: Inspecting a query
---

These methods show what a query is made of: a text summary, the SPARQL it runs, the raw result and a dict that can be stored.
The examples use the [example setup](_index.md#example-setup) and this query:

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m", quantity_kind="power")
```

| Method | Description |
| --- | --- |
| [`Query.summary`](#querysummary) | One line per node of the query |
| [`print(q)`](#printing-a-query) | The summary and the first five matches |
| [`Query.to_sparql`](#queryto_sparql) | The SPARQL text of the query |
| [`Query.execute`](#queryexecute) | The raw result of the SPARQL query |
| [`Query.to_dict`](#queryto_dict) | The query as a JSON-serializable dict |

## `Query.summary`

```python
Query.summary()
```

Returns a text description of the query, one line per node. It does not contact the server.
Each line has the alias, the kind of node (`entity`, `property` or `measurement`), its class or URI, the node it is attached to, its keyword filters and the number of expression filters.

**Parameters**

None.

**Returns**

`str`. An empty query gives `"Query (empty)"`.

**Example**

```python
print(q.summary())
```

```text
Query
  pump: entity, Pump
  m: measurement, of pump, quantity_kind=Power
```

## Printing a query

```python
repr(q)
print(q)
```

`repr(q)`, which is what an interactive prompt shows, is the summary. `print(q)` and `str(q)` add the number of matches and the first five rows of [`Query.metadata`](reading.md#querymetadata).
A notebook cell that ends with a query shows the same as `print(q)`.

`print(q)` runs the query. When the query cannot run, the summary is followed by `(preview unavailable: <error name>)` and no error is raised.

**Example**

```python
print(q)
```

```text
Query
  pump: entity, Pump
  m: measurement, of pump, quantity_kind=Power
2 matches:
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ m                       ┆ m.label             │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

## `Query.to_sparql`

```python
Query.to_sparql()
```

Returns the SPARQL text of the query. Node `n` of the query is the variable `?v<n>`, in the order the nodes were added.

For a `related()` step with `via="any"` or a single predicate, the client finds the matches itself when the query runs. The query that is sent to the server then differs from the text returned here.

**Parameters**

None.

**Returns**

`str`.

**Example**

```python
print(acq.query().entity("Pump").to_sparql())
```

```text
SELECT DISTINCT ?v0
WHERE {
  ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v0_typ .
  { SELECT DISTINCT ?v0_typ WHERE { ?v0_typ <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <https://watermetadata.org/ontology/watr#Pump> . } }
}
```

## `Query.execute`

```python
Query.execute(include_dependencies=True)
```

Runs the query and returns the raw result. [`Query.metadata`](reading.md#querymetadata) builds its table from this result. The result is kept on the `Query` object, so a second call does not ask the server again.

**Parameters**

- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.

**Returns**

`dict` with these keys:

- `"columns"`: the variable names. `v<n>` is node `n`. For a measurement node there are also `ext<n>` (stream URI), `unit<n>` and `extunit<n>` (units of the property and the stream), `lbl<n>` (label), `extsrc<n>` (`source_id`) and `extname<n>` (`stream_name`).
- `"rows"`: one list per match, with full URIs.
- `"datatypes"`: one list per row. `"iri"` marks a node. Other entries are the datatype of the value, or `None`.

**Example**

```python
acq.query().entity("Pump").execute()
```

```text
{'columns': ['v0'], 'rows': [['urn:swro/intake'], ['urn:swro/P2'], ['urn:swro/P1']], 'datatypes': [['iri'], ['iri'], ['iri']]}
```

## `Query.to_dict`

```python
Query.to_dict()
```

Returns the query as a dict of plain values that can be written as JSON. It does not contact the server. Class names and filter values appear as the URIs they were matched to.

**Parameters**

None.

**Returns**

`dict` with the keys `"nodes"`, `"edges"`, `"aliases"`, `"aliases_reverse"`, `"current_pointer"`, `"selects"`, `"casts"` and `"data_nodes"`. A filter value wrapped in `Not` is written as `{"not": value}`.

There is no method that builds a `Query` back from this dict.

**Example**

```python
acq.query().entity("Pump", alias="pump", label="Booster Pump P2").to_dict()
```

```text
{'nodes': [{'id': 0, 'rdf_class': 'https://watermetadata.org/ontology/watr#Pump', 'alias': 'pump', 'constraints': {'rdf_class': 'https://watermetadata.org/ontology/watr#Pump', 'attrs': {'label': 'Booster Pump P2'}}}],
 'edges': [],
 'aliases': {'pump': 0},
 'aliases_reverse': {0: 'pump'},
 'current_pointer': 0,
 'selects': [],
 'casts': [],
 'data_nodes': []}
```
