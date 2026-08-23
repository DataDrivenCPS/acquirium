# HTTP API Reference

All endpoints are served by the Acquirium FastAPI server (default `http://localhost:8000`).

Timestamps are ISO 8601 strings (e.g. `2026-01-01T00:00:00Z`). Errors return HTTP 400 with a JSON body `{"detail": "<message>"}`.

---

## System

### `GET /health`

Returns `{"ok": true}` when the server is running.

### `GET /graph_version`

Returns the store-owned source-data generation and the state of the derived
query cache. A cache is fresh exactly when `published_version` equals
`source_version` and `is_current` is true.

```json
{
  "source_version": 42,
  "published_version": 41,
  "is_current": false,
  "rebuild_in_progress": true
}
```

Long-running clients may poll `source_version` to invalidate local query state. A
caller that needs to observe current inferred data should issue its query with
`wait_for_fresh=true`; the status endpoint is informational and does not make
the following query atomic.

### `GET /embedding_status`

Returns the current state of the semantic embedding indexes.

```json
{
  "graph": {"state": "ready", "concepts": 120, "surfaces": 340, "error": null, "last_built": "...", "duration_s": 1.2},
  "qudt":  {"state": "ready", "concepts": 80,  "surfaces": 210, "error": null, "last_built": "...", "duration_s": 0.8}
}
```

`state` is one of `"idle"`, `"building"`, `"ready"`, or `"error"`.

---

## Knowledge Graph

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

---

## Timeseries Ingestion

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
| `publication_id` | no | Stable id for this atomic mutation set. Reusing it on a retry (same rows) replays the original receipt instead of re-applying; reusing it with different rows is rejected as a conflict |

**Response** `{"ok": true, "rows_inserted": 42}`

### `POST /insert_timeseries_arrow`

High-throughput insert via Apache Arrow IPC stream. The request body is a binary Arrow stream with columns `source_id`, `ref_name`, `ts`, and `value`. Rows are partitioned by `source_id` and dispatched in bulk.

**Content-Type** `application/vnd.apache.arrow.stream`

**Response** `{"ok": true, "rows_inserted": 42}`

Supply an `X-Acquirium-Publication-Id` header to make a retried flush idempotent. A request spanning multiple `source_id`s publishes one atomic set per source, namespacing the base id per source.

---

## Timeseries Query

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

See [data-api.md](data-api.md#value_mode) for a full description of `value_mode` options.

### `POST /timeseries_info`

Return metadata for a list of stream URIs.

**Request body** `{"uris": ["urn:acquirium#...", ...]}`

**Response** A map of URI → stream info object.

---

## Materialization

Durable transformations, services, experiments, artifacts, and state revisions.
Declarations are immutable; registration returns the durable identity. See
`docs/materialization_stateful.md` and `docs/materialization_services.md`.

### Transformations

| Endpoint | Purpose |
| --- | --- |
| `PUT /transformations/{name}` | Validate and deploy an immutable transformation definition under a stable name |
| `GET /materialization/epochs` | Inspect the current immutable topology epoch and its persisted bindings |
| `GET /materialization/epochs/{epoch_id}` | Inspect one topology epoch |

`register` body: `name`, `source_digest`, `entrypoint`, `inputs`/`bind`,
`outputs`, `impact`, `parameters_schema`.

### Services

| Endpoint | Purpose |
| --- | --- |
| `POST /services/register` | Register a service definition (no stream ownership) |
| `GET /services/{name}` | Service status and health |
| `POST /services/{name}/start` | Start the service |
| `POST /services/{name}/stop` | Stop the service |

### Experiments

| Endpoint | Purpose |
| --- | --- |
| `POST /experiments/runs` | Start a run from a frozen snapshot |
| `GET /experiments/runs` | List runs (optional `status`, `metadata_key`, `metadata_value`) |
| `GET /experiments/runs/{run_id}` | One run record |
| `POST /experiments/runs/{run_id}/execute` | Run the entrypoint on the shared executor |
| `POST /experiments/runs/{run_id}/rerun/{new_run_id}` | Clone a run's frozen inputs under a new id |
| `POST /experiments/runs/{run_id}/finish` | Mark succeeded/failed/cancelled |
| `POST /experiments/runs/{run_id}/metrics/{name}` | Record a metric (`{"value": ...}`) |
| `GET /experiments/runs/{run_id}/metrics` | Read recorded metrics |
| `POST /experiments/runs/{run_id}/artifacts` | Attach a produced artifact by digest |
| `GET /experiments/runs/{run_id}/artifacts` | List attached artifacts |
| `POST /experiments/runs/{run_id}/keep` | Mark a run kept (`{"reason": ...}`) |
| `POST /experiments/runs/{run_id}/collect` | Collect an unkept run, keeping its tombstone |

Reusing a `run_id` is an idempotent replay only when every frozen input
matches; otherwise the request is rejected.

### Artifacts and state revisions

| Endpoint | Purpose |
| --- | --- |
| `POST /artifact-requests` | Submit a durable artifact-production request |
| `POST /artifact-requests/lease` | Lease the next pending request |
| `POST /artifact-requests/{request_id}/complete` | Complete a lease with base64 artifact bytes |
| `POST /artifact-requests/{request_id}/fail` | Fail a leased request |
| `POST /state-revisions/{revision_id}/promote` | Promote a candidate revision (`prospective`, `recompute_all`, or `recompute_from`) |

---

## Semantic Resolution

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

## Unit Conversion

### `POST /resolve_unit`

Look up a unit by URI, label, symbol, or UCUM code.

**Request body** `{"identifier": "degF"}`

**Response** Unit metadata including URI, label, and conversion factors.

### `POST /conversion_factors`

Get multiplicative and additive conversion factors between two units.

**Request body** `{"from_unit": "degF", "to_unit": "degC"}`

**Response** `{"multiplier": ..., "offset": ...}`

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
