---
title: Acquirium Client API
---

<!-- TODO: intro -->

This is the reference for the Python client: every public method, with its
signature and a one-line description.
The four sections follow the objects you hold: `Acquirium`, the `Query` it
builds, the `DataObject` a query returns, and the lower-level
`AcquiriumClient` behind all three.
Type hints are shortened; `pl` is polars, `pa` is pyarrow.

## Acquirium

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000, use_ssl=False,
                insert_batch_rows=50000, health_timeout=60.0)
```

The constructor waits for `GET /health` for up to `health_timeout` seconds.
`acq.client` is the underlying [`AcquiriumClient`](#acquiriumclient).

### Querying

| method | description |
|---|---|
| `query() -> Query` | Create a new empty `Query` bound to this instance. |
| `explore() -> Query` | Alias of `query()`. |

### Graph

| method | description |
|---|---|
| `insert_graph(rdf_graph: str, format="turtle", replace=True, *, source_id: str) -> None` | Insert RDF into the graph owned by `source_id`; `replace=True` clears that graph first. |
| `insert_graph_file(path, format=None, replace=True, *, source_id: str) -> None` | Read RDF from a file and insert it into the graph owned by `source_id`; the format is taken from the extension when omitted. |
| `sparql_update(update: str, *, source_id: str) -> dict` | Execute a SPARQL UPDATE against one owned data graph. |
| `validate_graph() -> dict` | Validate all deployment data against the ontology shapes. |
| `graph_version() -> int` | The server's current source-data generation. |
| `graph_status() -> dict` | Source and derived-query cache generations (`source_version`, `published_version`, `is_current`, `rebuild_in_progress`). |

### Streams and timeseries

| method | description |
|---|---|
| `register_datasource(source_id: str) -> str` | Register a datasource in the graph; idempotent. |
| `register_streams(streams: Iterable[dict]) -> None` | Declare one or more streams' identity and semantic metadata in one graph insert; see the [lifecycle guide](../explanation/stream-lifecycle.md). |
| `reference_uri(source_id: str, ref_name: str) -> URIRef` | The canonical stream URI for a `(source_id, ref_name)` pair. |
| `resolve_point_metadata(fields: dict, min_score=0.6) -> dict[str, str \| None]` | Resolve `unit`, `quantity_kind`, `medium`, `substance` text to URIs jointly. |
| `insert_timeseries(source_id, ref_name, rows: list[tuple[datetime, Any]], *, point_uri=None, replace=False) -> dict` | Insert rows for one stream. |
| `insert_timeseries_batch(source_id, streams: dict[str, list[tuple[datetime, Any]]]) -> dict` | Insert rows for several streams; chunked by `insert_batch_rows`. |
| `insert_timeseries_arrow(source_id, table: pa.Table) -> dict` | Insert a `(ts, ref_name, value)` Arrow table; the path drivers use. |

### Logbook

| method | description |
|---|---|
| `insert_log(message, log_time=None, observation_start=None, observation_end=None) -> dict` | Insert a plant-level log entry. |
| `read_logs(log_time_start=None, log_time_end=None, observation_start=None, observation_end=None) -> pl.DataFrame` | Read plant-level log entries. |
| `delete_logs() -> dict` | Delete all plant-level log entries. |

### Apps

| method | description |
|---|---|
| `check_app(target: type[App], *, parameters=None, limit=None) -> dict` | Dry-run an app against stored data and return what it computed; nothing is deployed or saved. Every computed row comes back unless `limit` heads each output. |
| `deploy_app(target: type[App], *, parameters=None) -> dict` | Persist and deploy an importable app class; `parameters` are passed to its constructor. |
| `remove_app(name: str) -> dict` | Remove a durable app deployment by name. |
| `app_dag() -> nx.DiGraph` | Return the compiled binding DAG; nodes describe concrete inputs, outputs, policies, and revision progress. |

See the [app reference](apps.md) for the transformation class contract.

### Deprecated

| method | description |
|---|---|
| `find_entity(*, _class=None, alias=None, uri=None) -> Q` | The previous query builder; see [Coming from the old interface](#coming-from-the-old-interface). |
| `find_all_data(*, _class=None, uri=None) -> Q` | The previous query builder |
| `generate_grafana_dashboard(grafana_server, api_key)` | Build a Grafana dashboard from the plant model. |

## Query

`Query` is immutable: every verb returns a new query and leaves the old one
usable.
Terminals execute it.
See the [querying tutorial](../tutorials/querying.md) and
[the query model](../explanation/query-model.md).

### Verbs

| method | description |
|---|---|
| `entity(cls=None, *, uri=None, alias=None, **attrs) -> Query` | Add an entity node for a class (URI or free text) or one instance (`uri=`, CURIEs accepted); keyword attributes filter inline. |
| `related(cls=None, *, uri=None, alias=None, frm=None, via="any", direction=None, max_depth=None, nearest=None, **attrs) -> Query` | Add an entity connected to `frm` (default: the current node); `via=` restricts predicates, `direction=` walks the piping topology; `max_depth` defaults to 3 (1 for predicate lists), `nearest` to `True` for plain `via="any"`. |
| `measurement(*, frm=None, alias=None, direction=None, max_depth=3, nearest=False, include_connection_points=True, **attrs) -> Query` | Attach the measurement points of `frm` (default: the current node; `"*"` for every entity, or a list of aliases); on an empty query, every registered stream. |
| `where(target=None, **attrs) -> Query` | Filter a node (`target=` by alias, default the current node) by attribute; values are URIs, free text, lists (OR) or `Not(value)`. |
| `include(*names, of=None, required=False) -> Query` | Add `alias.attr` columns for a node, or un-drop a node; `required=True` drops rows lacking the attribute. |
| `drop(*names) -> Query` | Hide a node's column or un-include an attribute; with no arguments, drop the current node. |
| `with_columns(*specs, of=None, required=False) -> Query` | `include()` and `drop()` in one call: plain specs include, `"-"`-prefixed specs drop, `"alias.attr"` targets any node. |
| `alias(name) -> Query` | Name the current node. |
| `refocus(alias) -> Query` | Move the pointer back to an existing node. |

Attributes accepted by `where()`, `include()`, `options()` and the inline
keywords: `type`, `process`, `cp_type`, `medium`, `substance`,
`quantity_kind`, `unit`, `enumeration_kind`, `data_source`.

### Terminals

| method | description |
|---|---|
| `metadata(*, include_internals=False, include_dependencies=True) -> pl.DataFrame` | The pattern matches, one column per node plus `alias.attr` and `alias.label` columns. |
| `data(*, start=None, end=None, limit=None, order="asc", include_dependencies=True, cast_value="float", value_mode="default") -> DataObject` | A lazy `DataObject` over the matched streams. |
| `dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc", include_dependencies=True, cast_value="str", value_mode="default", include_ref=False, compact=True) -> pl.DataFrame` | `data(...).dataframe(...)` in one call. |
| `options(attr_name, *, of=None, include_dependencies=True) -> pl.DataFrame` | Distinct values of one attribute across the matches, with counts. |
| `facets(*, of=None, include_dependencies=True) -> FacetSummary` | Value counts for every attribute that applies to a node; prints compactly, indexes like a dict, `attrs()` lists the attributes. |
| `resolved_nodes(*, alias=None, only_data_nodes=False, include_dependencies=True) -> list[str]` | The URIs the pattern currently matches. |
| `execute(include_dependencies=True) -> dict` | Run the compiled SPARQL and return raw `{"columns", "rows"}`. |
| `to_sparql() -> str` | The SPARQL the query compiles to, without running it. |
| `to_dict() -> dict` | A JSON-serializable form of the query graph. |

### Helpers

```python
from acquirium.Client.explore import (
    Not,                                     # where(medium=Not("brine"))
    hidden_predicates, hide, unhide,         # the set via="any" never follows
    UPSTREAM_EQUIPMENT, DOWNSTREAM_EQUIPMENT,
    UPSTREAM_PROPERTY, DOWNSTREAM_PROPERTY,  # the step patterns behind direction=
)
```

## Data

`DataObject` is returned by `Query.data()`.
It is lazy: row counts and time ranges come from stream statistics, and values
are fetched on the first call that needs them.
See the [data tutorial](../tutorials/data.md).

| method | description |
|---|---|
| `dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc", include_ref=False, compact=True) -> pl.DataFrame` | The values as one frame; the window filters the fetched rows (`limit` per stream). |
| `d["alias"] -> pl.DataFrame` | One alias as `[time, value]`, plus `point_uri` when the alias covers several points. |
| `metadata(*, include_ref_uris=False) -> pl.DataFrame` | Unique `(data_alias, point_label, point_uri, entity__*)` rows. |
| `units() -> dict[str, str \| None]` | The effective unit URI per alias. |
| `convert_to(to_unit, *, from_unit=None, alias=None) -> DataObject` | A new object with values converted to `to_unit`; `from_unit=` for points with no unit, `alias=` to convert one alias. |
| `iter(alias) -> Iterator[tuple[str, pl.DataFrame]]` | `(point_uri, frame)` per point of an alias. |
| `by(entity_alias) -> Iterator[tuple[str, DataObject]]` | A sub-object per entity. |
| `latest(alias) -> pl.DataFrame` | The newest row of an alias (fetches the window first). |
| `is_empty() -> bool` | Whether any rows exist. |
| `ref_info(alias) -> list[tuple[int, str]]` | `(index, ref_uri)` per stream behind an alias. |
| `aliases`, `entity_aliases` | Properties: the data and entity aliases present. |
| `total_rows`, `time_range` | Properties: row count and `(earliest, latest)` from stream statistics, without fetching. |
| `bindings` | Property: read-only binding metadata per stream. |

### Parameters shared by data() and dataframe()

| parameter | meaning |
|---|---|
| `start`, `end` | time window; on `Query` it bounds the fetch, on `DataObject` it filters fetched rows |
| `limit`, `order` | rows per stream, `"asc"` or `"desc"` |
| `shape` | `"wide"` (one column per point) or `"narrow"` (one row per reading) |
| `compact` | `True` renders URIs as CURIEs and identifies points by `point_id`; `False` returns raw `point_uri` and `ref_uri` columns |
| `include_ref` | add the reference URI column in the compact layout |
| `cast_value` | client-side cast: `"float"`, `"int"`, anything else leaves values as sent (`Query` only) |
| `value_mode` | `"default"`, `"numeric"`, `"text"` or `"coalesce"` (`Query` only) |
| `include_dependencies` | query the ontology and shape triples alongside the plant data (`Query` only) |

## AcquiriumClient

```python
from acquirium.Client.client import AcquiriumClient

client = AcquiriumClient(server_url="localhost", server_port=8000, use_ssl=False)
```

`Acquirium` delegates to this class; the methods it shares (`insert_graph`,
`register_streams`, `insert_timeseries*`, the app methods) behave the same
and are listed once above.

### Health and status

| method | description |
|---|---|
| `health(timeout=3.0) -> dict` | `GET /health`; raises on failure. |
| `graph_version() -> int`, `graph_status() -> dict` | As on `Acquirium`. |
| `embedding_status() -> dict` | State of the two embedding indexes. |
| `validate_graph() -> dict` | As on `Acquirium`. |

### Graph

| method | description |
|---|---|
| `sparql_query(sparql, include_dependencies=True, *, wait_for_fresh=False) -> dict` | Run a SPARQL query; `{"columns", "rows"}`. |
| `sparql_update(update, *, source_id) -> dict` | Run a SPARQL UPDATE against one owned graph. |
| `insert_graph(...)`, `insert_graph_file(...)` | As on `Acquirium`. |
| `namespace_manager() -> NamespaceManager` | The prefix table bound on the server, cached. |
| `compact_uri(item) -> str` | URI to `prefix:local`. |
| `expand_uri(text) -> str` | `prefix:local` to URI; full URIs pass through. |

### Resolution

| method | description |
|---|---|
| `resolve(query, kind=None, *, top_k=1, min_score=0.5, context=None)` | Free text to a URI (`str`), ranked candidates (`top_k > 1`), or a dict of fields resolved jointly. |
| `resolve_unit(identifier) -> dict` | The QUDT record behind a unit text, with conversion factors. |
| `resolve_conversion(from_unit, to_unit, *, top_k=5, min_score=0.5) -> dict` | Resolve both sides to a convertible pair plus factors. |
| `resolve_point_metadata(fields, min_score=0.6) -> dict` | As on `Acquirium`. |
| `get_conversion_factors(from_unit, to_unit) -> dict` | Multipliers and offsets between two unit URIs. |

### Streams and timeseries

| method | description |
|---|---|
| `register_datasource(source_id) -> str`, `register_streams(streams) -> None` | As on `Acquirium`. |
| `insert_timeseries(*, source_id, ref_name, rows, point_uri=None, replace=False) -> dict` | Keyword-only form of the `Acquirium` method. |
| `insert_timeseries_batch(source_id, streams) -> dict` | One HTTP request for several streams (unchunked). |
| `insert_timeseries_arrow(source_id, table) -> dict` | As on `Acquirium`. |
| `timeseries_df(uri, start=None, end=None, limit=None, order="asc", timeout=60.0, *, value_mode="default") -> pl.DataFrame` | All rows of one stream by `ref_uri`. |
| `timeseries_batches(uri, start=None, end=None, limit=None, order="asc", *, value_mode="default", timeout=60.0) -> Iterator[pl.DataFrame]` | The same, one frame per Arrow record batch. |
| `timeseries_info_batch(uris: list[str]) -> dict` | `row_count`, `earliest`, `latest` for several streams in one request. |

### Logbook

| method | description |
|---|---|
| `insert_log(point_uri=None, log_time=None, observation_start=None, observation_end=None, log_message="") -> dict` | Insert a log entry for a point (default: the plant). |
| `query_logs(point_uri=None, log_time_start=None, log_time_end=None, observation_start=None, observation_end=None) -> list[LogEntry]` | Log entries for a point within optional intervals. |
| `delete_logs(point_uri=None) -> dict` | Delete the log entries of a point. |

### Apps

| method | description |
|---|---|
| `check_app(definition: dict, limit=None) -> dict` | Raw HTTP form behind `Acquirium.check_app`. |
| `deploy_app(definition: dict) -> dict` | Raw HTTP form behind `Acquirium.deploy_app`; the high-level client builds the definition from a class. |
| `remove_app(name: str) -> dict` | Remove a deployment. |
| `materialization_dag() -> dict` | Return the server's raw binding-DAG payload. |

### Grafana

`generate_grafana_dashboard(server, api_key)`, `add_gauge_panel(prop_dict)`,
`add_time_series_panel(title, prop_dicts)`.

## Coming from the old interface

Optional. Skip this unless you have queries written against the previous
`find_*` methods.

Those methods still exist, reachable through `acq.find_entity()` and
`acq.find_all_data()`, but they are deprecated and will not gain features.
The rewrite is usually mechanical: one verb per old method, one `where()` in
place of the `filter_by_*` family.

| old | new |
|---|---|
| `find_entity(_class=, uri=, alias=)` | `entity(cls, uri=, alias=)` |
| `find_entity(process=...)` | `entity(cls, process=...)` |
| `find_related(_class=, _from=, hops=, predicates=)` | `related(cls, frm=, max_depth=, via=)` |
| `find_related(direction=)` | `related(direction=)` |
| `find_data(_from=, alias=, filters_dict=)` | `measurement(frm=, alias=, **attrs)` |
| `find_all_data()` | `measurement(frm="*")`, or `measurement()` on an empty query |
| `find_related_data(...)` | `related(...).measurement(...)` |
| `filter_by_unit(v)` | `where(unit=v)` |
| `filter_by_medium(v)` | `where(medium=v)` |
| `filter_by_substance(v)` | `where(substance=v)` |
| `filter_by_quantity_kind(v)` | `where(quantity_kind=v)` |
| `filter_by_enumeration_kind(v)` | `where(enumeration_kind=v)` |
| `filter_data_nodes(predicate=, value=, _from=)` | `where(attr=value, target=)` |
| `filter_by_*(..., exclude=True)` | `where(attr=Not(value))` |
| `show_query_graph()` | `to_sparql()` |

Keyword names changed with them: `_class` is `cls`, `_from` is `frm`, `hops`
is `max_depth`, and `predicates` is `via`.
`metadata()`, `data()`, `dataframe()`, `execute()`, `to_sparql()`,
`insert_log()` and `read_logs()` kept their names and behave the same.

Two behavior differences matter when porting.

Defaults changed.
`related()` returns nearest matches where `find_related` returned everything
reachable, so add `nearest=False` if you relied on the old behavior.

Filtering is one vocabulary now.
Anywhere you passed a predicate URI to `filter_data_nodes`, you now pass a
named attribute to `where()`, and the same names work as inline keyword
arguments on `entity()`, `related()` and `measurement()`.
