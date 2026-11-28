---
title: Client
---

The client is the Python object that talks to an Acquirium server. `acquirium.init()` returns one, and every other call on these pages is a method of it. The examples use `acq` for the client.

```python
import acquirium as aq

acq = aq.init()
```

| Page | Covers |
|---|---|
| [Starting and stopping](starting.md) | `init`, `shutdown`, the `Acquirium` class and its attributes |
| [Streams](streams.md) | Registering streams, and the `Stream` and `StreamInterval` handles that read them |
| [Time series](timeseries.md) | Inserting values into streams |
| [Graph](graph.md) | Inserting RDF, SPARQL updates, validation and the graph version |
| [Metadata](metadata.md) | Attaching metadata to nodes and matching text to URIs |
| [Querying](querying.md) | The methods that start a query |
| [Logs](logs.md) | Plant log entries |
| [Apps](apps.md) | Deploying and inspecting apps |

The query builder returned by `acq.query()` is described in the [query reference](../query/_index.md). Apps are described in the [app reference](../apps/_index.md) and experiments in the [experiment reference](../experiments/_index.md).

## All items

| Item | Kind | Purpose |
|---|---|---|
| [`acquirium.init`](starting.md#acquiriuminit) | function | Return a client, starting a local server when needed |
| [`acquirium.shutdown`](starting.md#acquiriumshutdown) | function | Disconnect, and stop the server this process started |
| [`Acquirium`](starting.md#acquirium) | class | Connect to a server that is already running |
| [`Acquirium.address`](starting.md#acquiriumaddress) | property | Address of the server |
| [`Acquirium.client`](starting.md#other-attributes) | attribute | The lower-level HTTP client |
| [`Acquirium.study`](starting.md#other-attributes) | attribute | Entry point for experiments |
| [`Acquirium.insert_batch_rows`](starting.md#other-attributes) | attribute | Largest number of rows sent in one insert request |
| [`Acquirium.register_streams`](streams.md#acquiriumregister_streams) | method | Register streams and their metadata |
| [`Acquirium.stream`](streams.md#acquiriumstream) | method | Return a handle for one stream |
| [`Acquirium.reference_uri`](streams.md#acquiriumreference_uri) | method | The `stream_uri` of a `source_id` and `stream_name` pair |
| [`Stream`](streams.md#stream) | class | Handle for one stream |
| [`Stream.between`](streams.md#streambetween) | method | Select a time range of a stream |
| [`Stream.dataframe`](streams.md#streamdataframe) | method | Read the values of a stream |
| [`StreamInterval`](streams.md#streaminterval) | class | A stream between two times |
| [`StreamInterval.dataframe`](streams.md#streamintervaldataframe) | method | Read the values in the interval |
| [`Acquirium.insert_timeseries`](timeseries.md#acquiriuminsert_timeseries) | method | Insert values into one stream |
| [`Acquirium.insert_timeseries_batch`](timeseries.md#acquiriuminsert_timeseries_batch) | method | Insert values into several streams |
| [`Acquirium.insert_timeseries_arrow`](timeseries.md#acquiriuminsert_timeseries_arrow) | method | Insert an Arrow table of values |
| [`Acquirium.insert_graph`](graph.md#acquiriuminsert_graph) | method | Insert RDF text |
| [`Acquirium.insert_graph_file`](graph.md#acquiriuminsert_graph_file) | method | Insert an RDF file |
| [`Acquirium.sparql_update`](graph.md#acquiriumsparql_update) | method | Run a SPARQL update |
| [`Acquirium.validate_graph`](graph.md#acquiriumvalidate_graph) | method | Check the graph against the ontology shapes |
| [`Acquirium.graph_version`](graph.md#acquiriumgraph_version) | method | Version number of the graph |
| [`Acquirium.graph_status`](graph.md#acquiriumgraph_status) | method | Version numbers and rebuild state of the graph |
| [`Acquirium.insert_metadata`](metadata.md#acquiriuminsert_metadata) | method | Attach metadata to one node |
| [`Acquirium.attr`](metadata.md#acquiriumattr) | property | Attribute names for `Query.where` |
| [`Acquirium.resolve_property_metadata`](metadata.md#acquiriumresolve_property_metadata) | method | Match the metadata text of one property to URIs |
| [`Acquirium.resolve`](metadata.md#acquiriumresolve) | method | Match one text to a URI |
| [`Acquirium.point`](metadata.md#acquiriumpoint) | method | Wrap a property URI for an experiment variable |
| [`Acquirium.query`](querying.md#acquiriumquery) | method | Start an empty query |
| [`Acquirium.explore`](querying.md#acquiriumexplore) | method | Same as `query` |
| [`Acquirium.find_all_data`](querying.md#acquiriumfind_all_data) | method | Start a query over every property that has a stream |
| [`Acquirium.insert_log`](logs.md#acquiriuminsert_log) | method | Add a plant log entry |
| [`Acquirium.read_logs`](logs.md#acquiriumread_logs) | method | Read plant log entries |
| [`Acquirium.delete_logs`](logs.md#acquiriumdelete_logs) | method | Delete all plant log entries |
| [`Acquirium.deploy_app`](apps.md) | method | Deploy an app |
| [`Acquirium.list_apps`](apps.md) | method | List deployed apps |
| [`Acquirium.inspect_app`](apps.md) | method | Details of one deployed app |
| [`Acquirium.remove_app`](apps.md) | method | Remove a deployed app |
