---
title: HTTP API
---

This is a reference for the HTTP endpoints the acquirium server exposes
(default `http://localhost:8000`).
The Python client covers all of these; use the raw endpoints when scripting
against the server directly.

Timestamps are ISO 8601 strings (e.g. `2026-01-01T00:00:00Z`). Errors return HTTP 400 with a JSON body `{"detail": "<message>"}`. 404 is reserved for an unknown driver or app, and 409 for registering an app name that already exists.

---

## System

### `GET /health`

Returns `{"ok": true}` when the server is running.

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
[`exact_only`](server-config.md#exact-only-resolution): both indexes still
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

Returns all namespace prefix bindings in the union graph as a `{prefix: uri}` map.

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

Legacy endpoint. Executes a SPARQL SELECT query and returns results in an internal `{columns, rows}` format. Prefer `/sparql` for new code.

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

The update is scoped to that owner's graph, so it cannot touch the plant model or another source's triples unless `source_id` names it. Drivers and apps use their own `sparql_update()` helper, which supplies the owner for them.

**Response** `{"ok": true, ...}`

---

## Timeseries ingestion

### `POST /register_datasource`

Register a named datasource. The `source_id` scopes stream `ref_name`s so two sources with the same name never collide.

**Request body** `{"source_id": "my-source"}`

**Response** `{"ok": true, "source_id": "my-source"}`

### `POST /insert_timeseries`

Insert observations for one or more streams. Request body is a JSON array; a single-stream insert is a one-element list.

**Request body**

```json
[
  {
    "source_id": "plant-historian",
    "ref_name": "TI-101",
    "point_uri": null,
    "replace": false,
    "values": [
      ["2026-01-01T00:00:00Z", 21.5],
      ["2026-01-01T00:01:00Z", "Manual Control"]
    ]
  }
]
```

| Field | Required | Description |
| --- | --- | --- |
| `source_id` | yes | Registered datasource identifier |
| `ref_name` | yes | Source-local stream name |
| `point_uri` | no | Override the semantic point URI |
| `replace` | no (default `false`) | If true, delete existing rows before inserting |
| `values` | yes | List of `[timestamp, value]` pairs |

**Response** `{"ok": true, "rows_inserted": 42}`

### `POST /insert_timeseries_arrow`

High-throughput insert via Apache Arrow IPC stream. The request body is a binary Arrow stream with columns `source_id`, `ref_name`, `ts`, and `value`. Rows are partitioned by `source_id` and dispatched in bulk.

**Content-Type** `application/vnd.apache.arrow.stream`

**Response** `{"ok": true, "rows_inserted": 42}`

---

## Timeseries query

### `GET /timeseries`

Read timeseries data for a single stream. Returns an Apache Arrow IPC stream (`application/vnd.apache.arrow.stream`).

| Parameter | Default | Description |
| --- | --- | --- |
| `uri` | required | Ref URI or point URI of the stream |
| `start` | — | ISO 8601 start timestamp (inclusive) |
| `end` | — | ISO 8601 end timestamp (exclusive) |
| `limit` | — | Maximum number of rows to return |
| `order` | `asc` | Row order: `asc` or `desc` |
| `value_mode` | `default` | Value projection mode (see below) |

The Arrow schema is `(ts: timestamp[us, UTC], value: float64 or utf8, uri: utf8)`.

See the [values guide](../explanation/values.md#numbers-and-text) for a full description of the
`value_mode` options.

### `POST /timeseries_info`

Return metadata for a list of stream URIs in one request. This is what makes a `DataObject` able to report its row count and time range without fetching any values.

**Request body** `{"uris": ["urn:acquirium#399ce39c-...", ...]}`

**Response** a map of URI → stream info:

```json
{
  "urn:acquirium#399ce39c-...": {
    "table": "timeseries",
    "row_count": 5892,
    "earliest": "2026-08-05T16:28:39.137381+00:00",
    "latest": "2026-08-07T22:53:38.669786+00:00"
  }
}
```

A stream that was never written to reports `row_count: 0` with null bounds.

---

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

`compatible` is the verdict of the dimension-vector check described in the [units guide](../explanation/units.md#what-convertible-means).

---

## Logs

### `POST /insert_log`

Insert a log entry associated with a point URI and optional observation period. All parameters are query parameters.

| Parameter | Required | Description |
| --- | --- | --- |
| `log_timestamp` | yes | ISO 8601 timestamp for the log entry |
| `message` | yes | Log message text |
| `point_uri` | no | Semantic point URI (defaults to plant URI) |
| `observation_start` | no | ISO 8601 start of the observed period |
| `observation_end` | no | ISO 8601 end of the observed period |

**Response** `{"ok": true}`

### `GET /query_logs`

Query log entries by point URI and time range. All parameters are query parameters.

| Parameter | Description |
| --- | --- |
| `point_uri` | Filter by point URI (defaults to plant URI) |
| `log_time_start` | Earliest log timestamp (inclusive) |
| `log_time_end` | Latest log timestamp (inclusive) |
| `observation_start` | Filter by observation period start |
| `observation_end` | Filter by observation period end |

**Response** Array of log entry objects.

### `DELETE /delete_logs`

Delete all log entries for a point URI.

| Parameter | Description |
| --- | --- |
| `point_uri` | Point URI to delete logs for (defaults to plant URI) |

**Response** `{"ok": true}`

---

## Drivers

The `acquirium driver` CLI group is a thin wrapper over these three. See the [driver reference](drivers.md) for what a driver is and how it is configured.

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

## Apps

### `POST /apps/register`

Register an app spec. See `AppSpec` in `internals/models.py` for the full field list. An optional `replace` query parameter (default `false`) tears the existing app down and re-registers it; without it, a name that already exists returns 409.

**Response** `{"ok": true, ...}` with the registration info.

### `POST /apps/delete`

Gracefully delete a registered app: stop it, strip its registration triples from its source graph, kill its actor, and remove its persisted source.

**Request body** `{"app_id": "my-app"}`

**Response** `{"ok": true, "name": "my-app", "deleted": true}`

An unknown `app_id` returns 404.

### `POST /apps/run`

Start an app run.

**Request body**

```json
{
  "app_id": "my-app",
  "start": "2026-01-01T00:00:00Z",
  "end": "2026-02-01T00:00:00Z",
  "params": {},
  "keep_alive": false,
  "interval": 10.0
}
```

**Response** `{"ok": true, "run_id": "<uuid>"}`

### `POST /apps/stop`

Stop a running app. Provide `run_id` or `app_id`.

**Request body** `{"run_id": "<uuid>"}` or `{"app_id": "my-app"}`

**Response** `{"ok": true, ...}`

### `GET /apps/list`

List app runs. Optional `app_id` query parameter filters by app.

**Response** `{"ok": true, "runs": [...]}`
