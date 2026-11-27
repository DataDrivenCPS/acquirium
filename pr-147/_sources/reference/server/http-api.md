---
title: HTTP API
---

This is a reference for the HTTP endpoints the acquirium server exposes
(default `http://127.0.0.1:8000`).
The Python client covers all of these; use the raw endpoints when scripting
against the server directly.

Timestamps are ISO 8601 strings (e.g. `2026-01-01T00:00:00Z`). Errors return
HTTP 400 with a JSON body `{"detail": "<message>"}`. Unknown drivers, apps,
and experiment runs return 404.

The server does not check who sends a request. To require an access token,
place a reverse proxy in front of it. The Python client sends its `token` as
the header `Authorization: Bearer <token>`.

---

## System

### `GET /health`

Returns `{"ok": true}` when the server is running.

Note that the drivers and apps of the config file are started after this endpoint answers, so they may still be starting. [`GET /drivers/list`](#get-driverslist) lists the drivers and [`GET /materialization/dag`](#get-materializationdag) shows the state of the apps.

### `GET /graph_version`

Returns the store-owned source-data generation and the state of the derived
query cache.
The cache is fresh when `published_version` equals `source_version` and
`is_current` is true.

```json
{
  "source_version": 42,
  "published_version": 41,
  "is_current": false,
  "rebuild_in_progress": true
}
```

Long-running clients can poll `source_version` to invalidate local query
state.
Note that this endpoint is informational; a query that must see the current
inferred data should pass `wait_for_fresh=true` itself.

### `GET /embedding_status`

Returns the current state of the semantic embedding indexes.

```json
{
  "semantic": true,
  "graph": {"state": "ready", "concepts": 120, "surfaces": 340, "error": null, "last_built": "...", "duration_s": 1.2},
  "qudt":  {"state": "ready", "concepts": 80,  "surfaces": 210, "error": null, "last_built": "...", "duration_s": 0.8}
}
```

`state` is one of `"idle"`, `"building"`, `"ready"`, or `"error"`.

`semantic` is `false` when the server runs with
[`exact_only`](configuration.md#exact-only-matching): both indexes still
build and report `"ready"`, but they answer exact matches only.

---

## Knowledge graph

### `POST /insert_graph`

Insert or replace an RDF graph.

**Request body**

```json
{
  "rdf_graph": "<file path or RDF text>",
  "format": "turtle",
  "replace": true,
  "source_id": "plant"
}
```

`format` accepts `turtle`, `n3`, `xml`, `trig`, `nquads`. `source_id` is required: use the reserved `plant` source for the shared plant model, otherwise use the owning driver or app source ID. `replace: true` replaces only that owner's graph.

**Response** `{"ok": true, "embedding_ready": true}`

### `GET /export_graph`

Export the RDF graph as serialized RDF.

| Parameter | Default | Description |
| --- | --- | --- |
| `include_dependencies` | `true` | Include all registered deployment/source graphs plus imported ontology/shape dependencies. `false` returns all deployment/source graphs without those dependencies. |
| `format` | `turtle` | Serialization format (`turtle`, `n3`, `xml`, `trig`, `nquads`) |

Returns the RDF document with the appropriate content type (e.g. `text/turtle`).

### `POST /validate_graph`

Validate all registered deployment data against the ontology shapes and SHACL rules. Takes no parameters.

**Response**

```json
{
  "conforms": false,
  "report": "@prefix sh: <http://www.w3.org/ns/shacl#> . ...",
  "results_text": "Validation Report\nConforms: False\nResults (364): ..."
}
```

`report` is the SHACL validation report as Turtle; `results_text` is the same report rendered for reading. Validation runs against the source data and the shapes directly, so it does not wait for a derived-graph rebuild.

### `GET /namespace/list`

Returns the server's prefix bindings as a `{prefix: uri}` map: acquirium's own
prefixes (`acq`, `point`, `qudt`, `unit`, `quantitykind`, `s223`, `g36`, `watr`,
`brick`, `ref`), the prefixes inserted models declared for their own
vocabularies, and rdflib's defaults. Generated names (`ns1`, `ns2`, ...) are
never bound, so a URI compacts to the same CURIE in every client and after a
restart.

---

## SPARQL

### `GET /sparql`

Read-only [SPARQL 1.1 Protocol](https://www.w3.org/TR/sparql11-protocol/) endpoint. It accepts `SELECT`, `ASK`, `CONSTRUCT`, and `DESCRIBE` queries through the required `query` URL parameter. SPARQL Update and POST protocol forms are not implemented yet.

| Parameter | Default | Description |
| --- | --- | --- |
| `query` | required | SPARQL query string |
| `include_dependencies` | `true` | Query inferred deployment data plus resolved ontology/shape triples. `false` queries inferred deployment data without those triples. |
| `wait_for_fresh` | `false` | Wait for inference after graph writes. `false` uses the last complete published derived graph. |

Use HTTP `Accept` to select a response serialization. The default is `application/sparql-results+json` for `SELECT`/`ASK` and `text/turtle` for `CONSTRUCT`/`DESCRIBE`.

| Query form | Supported response media types |
| --- | --- |
| `SELECT`, `ASK` | `application/sparql-results+json`, `application/sparql-results+xml`, `text/csv`, `text/tab-separated-values` |
| `CONSTRUCT`, `DESCRIBE` | `text/turtle`, `application/n-triples`, `application/rdf+xml`, `application/ld+json` |

**SELECT response (`application/sparql-results+json`)**

```json
{
  "head": {"vars": ["s", "p", "o"]},
  "results": {
    "bindings": [
      {"s": {"type": "uri", "value": "urn:example#foo"}, ...}
    ]
  }
}
```

Term types: `uri`, `literal`, `bnode`. Literals may include `"xml:lang"` or `"datatype"` fields.

**ASK response**

```json
{"head": {}, "boolean": true}
```

### `GET /sparql_json`

Legacy endpoint. Executes a SPARQL SELECT query and returns results in an internal `{columns, rows, datatypes}` format. Prefer `/sparql` for new code.

`rows` holds every cell as text. `datatypes` is a matrix of the same shape naming what each cell is: `"iri"` for a node, the XSD datatype IRI of a typed literal (`http://www.w3.org/2001/XMLSchema#integer`, `...#date`, ...), `null` for a plain string or an unbound cell. The Python client's `metadata()` parses typed values from it.

| Parameter | Default | Description |
| --- | --- | --- |
| `query` | required | SPARQL query string |
| `include_dependencies` | `true` | Include ontology/shape triples in the query graph |
| `wait_for_fresh` | `false` | If true, wait for pending inference after graph writes. Otherwise return the last complete published graph while rebuilding. |

Also accepts POST with the same fields as a JSON body. The Python client always POSTs, because a resolved traversal query exceeds URL length limits.

### `POST /sparql_update`

Execute a SPARQL UPDATE against one owned data graph. `/sparql` is read-only; this is the write path.

**Request body**

```json
{
  "update": "INSERT DATA { <urn:swro/P1> <http://www.w3.org/2000/01/rdf-schema#label> \"Feed pump 1\" }",
  "source_id": "plant"
}
```

| Field | Required | Description |
| --- | --- | --- |
| `update` | yes | SPARQL UPDATE (`INSERT`/`DELETE`) statement |
| `source_id` | yes | Owner of the data graph to update; use the reserved `plant` for the shared plant model |

The update is scoped to that owner's graph, so it cannot touch the plant model or another source's triples unless `source_id` names it. Drivers and apps use their own `sparql_update()` helper, which supplies the owner for them. The client's `insert_metadata()` is one such update, sent with the reserved `metadata` source; there is no separate endpoint for it.

The server compares the graph's `owl:imports` and `owl:Ontology` triples before and after the update and recomputes the ontology closure only when they changed. After the update it removes, from the `metadata` source graph, every triple whose subject or URI object no longer appears in any other graph.

**Response** `{"ok": true, ...}`

---

## Readings and epoch engine

| Endpoint | Contract |
| --- | --- |
| `POST /resolve_storage_keys` | Property and stream URIs to the selected streams; ambiguous properties fail |
| `POST /stream_views` | `{uris: [...], epoch: optional}` returns `{epoch, streams}` with each reference's `current` and `raw` streams at the pinned epoch and the replacement head's unit, if any |
| `POST /insert_timeseries` | JSON list of `{source_id, stream_name, request_id, values, replace: false}`; items sharing a `request_id` form one atomic request, and `replace` makes that stream hold exactly `values` |
| `POST /insert_timeseries_arrow` | Arrow `source_id, stream_name, ts, value`; required `X-Acquirium-Request-Id` |
| `POST /delete_timeseries` | `{source_id, stream_name, timestamps: [UTC-ns], request_id}` |
| `GET /timeseries` | Arrow `ts: timestamp[ns, UTC], value: float64, uri: utf8`; `after=<sequence>` waits until that receipt is published |
| `POST /timeseries_info` | `{uris: [...], view: "current"}` to current counts and UTC bounds |
| `POST /timeseries_exists` | Same read selectors; returns `{uris: [...]}` with rows present |
| `GET /engine/status` | Accepted/applied frontiers, backlog, per-operator failures, processor error, timings, worker queue/RSS, server PID and peak RSS |
| `GET /engine/snapshot` | Published epoch, applied sequence, complete active plan, per-operator failures. Returns once the plan reflects the metadata graph as of the request; while an app install is under way it waits up to 30 s, and `plan_current` is `false` if the plan is still behind |
| `GET /engine/changes` | Bounded signed change-feed pages with exclusive epoch/ordinal cursor and complete-epoch frontier |
| `POST /engine/retry` | Request another attempt for failed operators |
`GET /timeseries` uses query parameters; `/resolve_storage_keys`,
`GET /engine/status` reports the published epoch, admitted and applied
where available. `GET /engine/snapshot` reports the plan and stream failures

Timeseries reads accept `view="current"` (the last replacement), `"raw"`
(the chain's original input), or `"stored"` (the requested physical stream).
`GET /timeseries` uses query parameters; `/resolve_storage_keys`,
`/timeseries_info`, and `/timeseries_exists` accept `view` in their JSON
bodies. Reads and `/resolve_storage_keys` also accept an optional `epoch`:
resolution and rows use the same published epoch, including across
pagination. Without an epoch, the read captures the current one after waiting
for `after`, if supplied. An explicit epoch must include that receipt. Counts
and existence checks resolve and count against the current snapshot. See [replacement outputs](../apps/outputs.md#replacement-outputs).

JSON values contain timestamp/value pairs. Integer timestamps are UTC nanoseconds;
ISO datetime strings are also accepted. A timestamp without a time zone is interpreted as UTC. Values are numbers or text,
each stored as its stream's `value_kind` ([values](../../explanation/values.md));
null, blank and non-finite values are dropped. Arrow timestamps must be timezone-aware. Duplicate keys keep the last
row before submission to the engine; source/request IDs and normalized payloads identify
retries within the bounded retry window.

JSON rows sharing a source/request ID commit as one request. Different sources
or IDs commit independently. Ingestion's `wait` query parameter is a nonnegative
publication timeout in seconds (default 30, zero for immediate durable admission).
Responses include database-scoped receipt sequences and `accepted`/`published`
state. `rows_inserted` counts normalized observations after missing values
are dropped and duplicate keys are collapsed. Failed or timed-out computation
after admission remains accepted/pending;
inspect the receipt and engine status before retrying with the same identity.
Admission capacity rejection returns HTTP 503 with `Retry-After: 1`.

The change-feed endpoint accepts `epoch=0`, `ordinal=0`, and `limit=1000`
(maximum 10,000), and returns:

| Field | Meaning |
| --- | --- |
| `changes` | signed rows, in epoch and ordinal order |
| `cursor` | exclusive resume position, either the last row in a partial epoch or the end of the last completed epoch (ordinal `18446744073709551615`) |
| `published_epoch` | the publication frontier when the page was cut |
| `complete_through` | every change up to this epoch has now been returned |
| `has_more` | more rows remain at or below `published_epoch` |

Pages can split epochs and correction pairs; see
[consume streaming changes](../../how-to/consume-streaming-changes.md) before
checkpointing or publishing a consumer view.

When retention expires a cursor, rebuild from a current snapshot. To follow
changes strictly after the rebuilt epoch, send that epoch with ordinal
`18446744073709551615`.

`GET /engine/status` reports the published epoch, admitted and applied
request sequences, current backlog, retention point, and any engine fault.
`process.peak_rss_bytes` reports the server process's peak resident memory
where available. `GET /engine/snapshot` reports the plan and stream failures
at the captured epoch.

Timeseries parameters include `uri`, optional `start` and `end` datetime strings or
`start_ns` and `end_ns` integers, optional `limit`, `order=asc|desc`, and an optional
published `epoch`. Bounds are half-open. Without a limit, paging returns the
complete range. Related reads should use the same captured epoch. `value_mode` is
`default` (the stream's declared kind), `numeric`, `text` or `coalesce` (every
reading as a string). The Arrow `value` column is `float64` for numeric reads
and `string` otherwise.


## Semantic resolution

### `GET /resolve_text`

Resolve a free-text string to matching semantic concepts using the embedding index.

| Parameter | Default | Description |
| --- | --- | --- |
| `text` | required | Text to resolve |
| `kind` | — | Filter by concept kind (e.g. `"quantity"`, `"unit"`) |
| `top_k` | `5` | Maximum number of matches to return |
| `min_score` | `0.5` | Minimum similarity score (0–1) |
| `context` | — | Repeated query param; additional context URIs to bias matching |

**Response** `{"matches": [{"uri": "...", "score": 0.92, ...}, ...]}`

### `POST /resolve_record`

Resolve multiple fields of a structured record to semantic concepts in a single call.

**Request body**

```json
{
  "fields": [
    {"name": "temperature", "text": "supply air temp", "kind": "quantity"},
    {"name": "unit",        "text": "degF",             "kind": "unit"}
  ],
  "top_k": 5,
  "min_score": 0.5,
  "context": ["urn:example#AHU-1"]
}
```

**Response** `{"matches": {"temperature": [...], "unit": [...]}}`

---

## Unit conversion

Every unit is expressed as a multiplier and an offset against the base unit of its dimension, and a value moves between two units through that base:

```text
converted = (value + from_offset) * from_multiplier / to_multiplier - to_offset
```

### `POST /resolve_unit`

Look up a unit by URI, label, symbol, or UCUM code.

**Request body** `{"identifier": "psi"}`

**Response**

```json
{
  "uri": "http://qudt.org/vocab/unit/PSI",
  "label": "Psi",
  "symbol": "psi",
  "quantity_kind": "http://qudt.org/vocab/quantitykind/VapourPressure",
  "multiplier": 6894.757293168362,
  "offset": 0.0
}
```

### `POST /resolve_conversion`

Resolve both sides of a conversion together and return a *convertible* pair. Candidates for each side are considered jointly, so a near-match that does not convert never shadows one that does. Use this when either side is free text; use `/conversion_factors` when both URIs are already known.

**Request body**

```json
{"from_unit": "psi", "to_unit": "bar", "top_k": 5, "min_score": 0.5}
```

| Field | Required | Description |
| --- | --- | --- |
| `from_unit`, `to_unit` | yes | Unit URI or free text |
| `top_k` | no (default `5`) | Candidates considered per side |
| `min_score` | no (default `0.5`) | Minimum resolver score |

**Response** the resolved unit record for each side plus the factors between them:

```json
{
  "from":    {"uri": ".../PSI", "label": "Psi", "symbol": "psi", "multiplier": 6894.757293168362, "offset": 0.0, ...},
  "to":      {"uri": ".../BAR", "label": "Bar", "symbol": "bar", "multiplier": 100000.0,          "offset": 0.0, ...},
  "factors": {"from_uri": ".../PSI", "to_uri": ".../BAR",
              "from_multiplier": 6894.757293168362, "from_offset": 0.0,
              "to_multiplier": 100000.0, "to_offset": 0.0, "compatible": true}
}
```

A request with no convertible pair among the candidates fails with HTTP 400 and both candidate lists in `detail`.

### `POST /conversion_factors`

Get the conversion factors between two units that are already identified by URI.

**Request body** `{"from_unit": "http://qudt.org/vocab/unit/PSI", "to_unit": "http://qudt.org/vocab/unit/BAR"}`

**Response**

```json
{
  "from_uri": "http://qudt.org/vocab/unit/PSI",
  "to_uri": "http://qudt.org/vocab/unit/BAR",
  "from_multiplier": 6894.757293168362,
  "from_offset": 0.0,
  "to_multiplier": 100000.0,
  "to_offset": 0.0,
  "compatible": true
}
```

`compatible` is the verdict of the dimension-vector check described in the [units guide](../../explanation/units.md#what-convertible-means).

---

## Logs

### `POST /insert_log`

Insert a log entry associated with a property URI and optional observation period. All parameters are query parameters.

| Parameter | Required | Description |
| --- | --- | --- |
| `log_timestamp` | yes | ISO 8601 timestamp for the log entry |
| `message` | yes | Log message text |
| `property_uri` | no | Semantic property URI (defaults to plant URI) |
| `observation_start` | no | ISO 8601 start of the observed period |
| `observation_end` | no | ISO 8601 end of the observed period |

**Response** `{"ok": true}`

### `GET /query_logs`

Query log entries by property URI and time range. All parameters are query parameters.

| Parameter | Description |
| --- | --- |
| `property_uri` | Filter by property URI (defaults to plant URI) |
| `log_time_start` | Earliest log timestamp (inclusive) |
| `log_time_end` | Latest log timestamp (inclusive) |
| `observation_start` | Filter by observation period start |
| `observation_end` | Filter by observation period end |

**Response** Array of log entry objects.

### `DELETE /delete_logs`

Delete all log entries for a property URI.

| Parameter | Description |
| --- | --- |
| `property_uri` | Property URI to delete logs for (defaults to plant URI) |

**Response** `{"ok": true}`

---

## Drivers

The `acquirium driver` CLI group is a thin wrapper over these three. See the [driver reference](../drivers/_index.md) for what a driver is and how it is configured.

### `POST /drivers/start`

Start a driver as a Ray actor on this server.

**Request body**

```json
{
  "spec": "acquirium.Drivers.BuiltInDrivers.csv_ingest:CSVIngestDriver",
  "config": {"driver": {"source_id": "ro-skid", "watch_dir": "./data", "glob": "*.csv", "format": "wide"}},
  "name": "ro-skid",
  "interval": 60.0
}
```

| Field | Required | Description |
| --- | --- | --- |
| `spec` | yes | `module.path:ClassName`, or `path/to/file.py:ClassName` resolved against the config's directory **on the server host** |
| `config` | no (default `{}`) | The full merged acquirium config; the driver reads its own entry as `self.config["driver"]` |
| `name` | no | Registry name; defaults to the spec's class name. Starting a second driver under a name already in use fails |
| `interval` | no | Tick interval in seconds; falls back to `[driver] interval`, then `10.0` |

The driver's `setup()` runs before this returns, so a slow setup means a slow response, and a setup that raises fails the request rather than leaving a registered driver behind.

**Response** `{"ok": true, "driver": {"name": ..., "spec": ..., "interval": ..., "started_at": ..., "status": "running"}}`

### `POST /drivers/stop`

Signal a driver to stop, wait up to 10 seconds for the current tick, run its `stop()`, flush what is buffered, then kill the actor.

**Request body** `{"name": "ro-skid"}`

**Response** `{"ok": true, "name": "ro-skid", "stopped": true}`

`stopped: false` with an `error` means the driver did not exit within the window and the actor was killed anyway. An unknown name returns 404.

### `GET /drivers/list`

List the drivers running on this server. Takes no parameters.

**Response**

```json
{
  "ok": true,
  "drivers": [
    {
      "name": "WaterTAPParquetDriver",
      "spec": "../../scripts/parquet_driver.py:WaterTAPParquetDriver",
      "interval": 120.0,
      "started_at": "2026-08-26T22:22:39.362884+00:00",
      "status": "running"
    }
  ]
}
```

`status` is `running`, `stopped`, or `failed: <error>`.

---

## Experiments

The experiment endpoints create Studies, declare variables, start runs, and
record values or stream references. The HTTP API calls a Study a `template`.
See the [experiment endpoint table](../experiments/_index.md) for the full list
and the [recording guide](../../how-to/record-experiments.md) for Python examples.

`GET /experiments/templates/{template_id}/runs` and
`GET /experiments/templates/{template_id}/observations` accept `status`,
`started_after`, `started_before`, and a legacy `metadata` filter encoded as a
JSON object string. Observation reads also accept `run_id` and `variable_id`.

The shared query builder selects graph nodes first. The Python client passes
the selected run IDs to these endpoints:

- `POST /experiments/templates/{template_id}/runs/query`
- `POST /experiments/templates/{template_id}/observations/query`

Example request body:

```json
{
  "run_ids": ["saved-run-id"],
  "status": "succeeded"
}
```

Both accept the same lifecycle and legacy metadata filters as their GET forms.
Observation selection bodies can also include `run_id` and `variable_id`.
Responses are the corresponding lists of run or observation objects. An empty
ID list returns an empty list. IDs are scoped to the specified Study.

Time-series observations contain stream references and recorded intervals.
Fetch the sample values through `/timeseries`; they are not embedded in the
observation response.

`POST /experiments/runs/{run_id}/record` accepts a JSON array of discriminated
entries for values, artifact bytes or digests, sample rows, and existing stream
intervals. It returns ordered observation receipts and owned sample stream URIs.
Registration, samples, revisions, artifact ownership, observations, and resolved
graph intent commit together. Invalid entries reject the whole SQL batch.
See [batch recording forms](../experiments/_index.md).

`POST /experiments/metadata/sync` takes `{}` and returns
`{"ok": true, "unpublished": []}` after delivering the durable outbox and
completing scheduled declaration upgrades. `unpublished` is an always-empty
compatibility field. Delivery and document-read failures propagate; the server
does not rebuild acknowledged metadata from SQL. Restore missing authoritative
graphs with their matching backup. The Python interface synchronizes
automatically. See [storage migration and recovery](storage-migrations.md).

## Apps

- `GET /apps` lists declarations and their status.
- `GET /apps/{name}` inspects the declared app and its published operators.
- `PUT /apps/{name}?dry_run=true` previews a declaration. Without `dry_run`, the request deploys it.
- `DELETE /apps/{name}` removes an app. Remove any dependants first.

See the [app reference](../apps/_index.md) for declaration fields.
The server reports `pending` while an app activation is publishing.

## Complete metadata documents

`POST /metadata/documents` accepts a JSON array of full subject URIs and returns
an object mapping each URI to its complete document in the internal typed JSON
encoding. `AcquiriumClient.metadata_documents(subjects)` decodes it to ordinary
Python values, preserving dates, null positions, and empty containers. User JSON
inside the `metadata` namespace retains arbitrary string keys. This endpoint
reads asserted source documents independently of inference and delivers pending
experiment metadata writes first. Unknown subjects have empty documents.

`POST /metadata/asserted` accepts the same subject list and returns the property values and their asserting sources.
