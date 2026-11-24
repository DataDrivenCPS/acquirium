---
title: HTTP API
---

The server exposes semantic graph operations and one Escapement timeseries engine.

| Endpoint | Contract |
| --- | --- |
| `GET /health` | Server readiness |
| `GET /graph_version` | Source/inferred graph generations |
| `POST /insert_graph` | RDF text/file, format, source owner, replace/append that source's graph |
| `POST /sparql_update` | Update one explicitly owned data graph; `insert_metadata()` uses it with the reserved `metadata` source |
| `POST /metadata/documents` | Full subject URIs to their complete typed metadata documents, read from asserted source data |
| `GET /sparql` | SPARQL protocol read endpoint |
| `GET/POST /sparql_json` | SPARQL SELECT as `{columns, rows, datatypes}`: `datatypes` names each cell's kind (`"iri"`, an XSD datatype IRI, or null) |
| `GET /export_graph` | Serialized RDF; optional dependencies |
| `POST /validate_graph` | SHACL validation report |
| `GET /namespace/list` | Prefix map |
| `POST /register_datasource` | Source identity in RDF |
| `POST /resolve_storage_keys` | Point/reference URIs to canonical references; ambiguous points fail |
| `POST /stream_views` | `{uris: [...]}` to each reference's current `current` and `raw` streams and the unit a replacement head reports, or null |
| `POST /insert_timeseries` | JSON list of `{source_id, ref_name, request_id, values, replace: false}`; items sharing a `request_id` form one atomic request, and `replace` makes that stream hold exactly `values` |
| `POST /insert_timeseries_arrow` | Arrow `source_id, ref_name, ts, value`; required `X-Acquirium-Request-Id` |
| `POST /delete_timeseries` | `{source_id, ref_name, timestamps: [UTC-ns], request_id}` |
| `GET /timeseries` | Arrow `ts: timestamp[ns, UTC], value: float64, uri: utf8`; `after=<sequence>` waits until that receipt is published |
| `POST /timeseries_info` | `{uris: [...], view: "current"}` to current counts and UTC bounds |
| `POST /timeseries_exists` | Same read selectors; returns `{uris: [...]}` with rows present |
| `GET /engine/status` | Accepted/applied frontiers, backlog, per-operator failures, processor error, timings, worker queue/RSS, server PID and peak RSS |
| `GET /engine/snapshot` | Published epoch, applied sequence, complete active plan, per-operator failures. Returns once the plan reflects the metadata graph as of the request; while an app install is under way it waits up to 30 s, and `plan_current` is `false` if the plan is still behind |
| `GET /engine/changes` | Bounded signed change-feed pages with exclusive epoch/ordinal cursor and complete-epoch frontier |
| `POST /engine/retry` | Request another attempt for failed operators |
| `GET /apps` | Desired declarations and active operators |
| `GET/PUT/DELETE /apps/{name}` | Inspect, deploy an app declaration, or remove |
| `GET /resolve_text`, `POST /resolve_record` | Semantic text resolution |
| `POST /resolve_unit`, `/resolve_conversion`, `/conversion_factors` | Unit resolution and conversion factors |
| `GET /embedding_status` | Exact/semantic index state |
| `GET /drivers/list`, `POST /drivers/start`, `/drivers/stop` | Driver lifecycle |
| `/experiments/...` | Studies (`templates`), variables, runs, recordings, artifacts, and stream annotations; see the [experiment reference](experiments.md#http-api). Unknown runs return 404 |

Timeseries reads accept `view="current"` (the last replacement), `"raw"`
(the chain's original input), or `"stored"` (the requested physical stream).
`GET /timeseries` uses query parameters; `/resolve_storage_keys`,
`/timeseries_info`, and `/timeseries_exists` accept `view` in their JSON
bodies. Reads and `/resolve_storage_keys` also accept an optional `epoch`:
resolution and rows use the same published epoch, including across
pagination. Without an epoch, the read captures the current one after waiting
for `after`, if supplied. An explicit epoch must include that receipt. Counts
and existence checks resolve and count against the current snapshot. See [replacement outputs](apps.md#replacement-outputs).

JSON values contain timestamp/value pairs. Integer timestamps are UTC nanoseconds;
timezone-aware datetime strings are also accepted. Values are numbers or text,
each stored as its stream's `value_kind` ([values](../explanation/values.md));
null, blank and non-finite values are dropped. Arrow timestamps must be timezone-aware. Duplicate keys keep the last
row before submission to the engine; source/request IDs and normalized payloads identify
retries within the bounded retry window.

JSON rows sharing a source/request ID commit as one request. Different sources
or IDs commit independently. Ingestion's `wait` query parameter is a nonnegative
publication timeout in seconds (default 30, zero for immediate durable admission).
Responses include database-scoped receipt sequences and `accepted`/`published`
state. Failed or timed-out computation after admission remains accepted/pending;
inspect the receipt and engine status before retrying with the same identity.
Admission capacity rejection returns HTTP 503 with `Retry-After: 1`.

The change-feed endpoint accepts `epoch=0`, `ordinal=0`, and `limit=1000`
(maximum 10,000), and returns:

| Field | Meaning |
| --- | --- |
| `changes` | signed rows, in epoch and ordinal order |
| `cursor` | the `epoch` and `ordinal` of the last row returned, exclusive; an empty page returns the cursor it was given |
| `published_epoch` | the publication frontier when the page was cut |
| `complete_through` | every change up to this epoch has now been returned |
| `has_more` | more rows remain at or below `published_epoch` |

Pages can split epochs and correction pairs; see
[consume streaming changes](../how-to/consume-streaming-changes.md) before
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
