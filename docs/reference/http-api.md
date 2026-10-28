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
| `POST /insert_timeseries` | JSON list of `{source_id, ref_name, request_id, values}` |
| `POST /insert_timeseries_arrow` | Arrow `source_id, ref_name, ts, value`; required `X-Acquirium-Request-Id` |
| `POST /delete_timeseries` | `{source_id, ref_name, timestamps: [UTC-ns], request_id}` |
| `GET /timeseries` | Arrow `ts: timestamp[ns, UTC], value: float64, uri: utf8`; `after=<sequence>` waits until that receipt is published |
| `POST /timeseries_info` | `{uris: [...]}` to counts and UTC bounds |
| `GET /engine/status` | Accepted/applied frontiers, backlog, paused error, timings, worker queue/RSS, server PID and peak RSS |
| `GET /engine/snapshot` | Published epoch, applied sequence, complete active plan |
| `GET /engine/changes` | Bounded signed native feed pages with exclusive epoch/ordinal cursor and complete-epoch frontier |
| `POST /engine/retry` | Explicitly retry a paused epoch |
| `GET /apps` | Desired declarations and active operators |
| `GET/PUT/DELETE /apps/{name}` | Inspect, deploy a native semantic declaration, or remove |
| `GET /resolve_text`, `POST /resolve_record` | Semantic text resolution |
| `POST /resolve_unit`, `/resolve_conversion`, `/conversion_factors` | Unit resolution and conversion factors |
| `GET /embedding_status` | Exact/semantic index state |
| `GET /drivers/list`, `POST /drivers/start`, `/drivers/stop` | Driver lifecycle |
| `/experiments/...` | Studies (`templates`), variables, runs, recordings, artifacts, and stream annotations; see the [experiment reference](experiments.md#http-api). Unknown runs return 404 |

JSON values contain timestamp/value pairs. Integer timestamps are UTC nanoseconds;
timezone-aware datetime strings are also accepted. Values are numbers or text,
each stored as its stream's `value_kind` ([values](../explanation/values.md));
null, blank and non-finite values are dropped. Arrow timestamps must be timezone-aware. Duplicate keys keep the last
row before native submission; source/request IDs and normalized payloads identify
retries within the bounded native retry window.

JSON rows sharing a source/request ID commit as one request. Different sources
or IDs commit independently. Ingestion's `wait` query parameter is a nonnegative
publication timeout in seconds (default 30, zero for immediate durable admission).
Responses include database-scoped receipt sequences and `accepted`/`published`
state. Failed or timed-out computation after admission remains accepted/pending;
inspect the receipt and native status before retrying with the same identity.
Admission capacity rejection returns HTTP 503 with `Retry-After: 1`.

The change-feed endpoint accepts `epoch=0`, `ordinal=0`, and `limit=1000`
(maximum 10,000). It returns `changes`, `cursor`, `published_epoch`,
`complete_through`, and `has_more`. Pages can split epochs and correction pairs;
see [consume streaming changes](../how-to/consume-streaming-changes.md) before
checkpointing or publishing a consumer view. Status `process.peak_rss_bytes`
reports the server's process high-water RSS where supported, otherwise null;
Python worker processes are excluded. Status `python_workers` exposes native
process-local pool counters: `spawned` (successful handshakes), `reused`,
`discarded`, `failures`, `resets`, `resident`, `idle`, `active`, and `peak_active`.
Resident capacity includes starting/leased workers; active leases are not CPU
utilization. Counters reset with the native owner and are not durable progress.
`failures` counts pool startup/lease failures; `resets` also covers native
validation or publication failures after a worker has returned.

Pool status also reports `queued`, `peak_queued`, `oldest_queue_age_ms`, and
`queue_timeouts`. This queue counts ready operator jobs waiting for a process,
separately from pending source requests and DAG dependencies. Nested `telemetry`
provides bounded timing histograms and live worker PIDs/artifacts with one last
job's operator, epoch, affected group count, and timing. One job can process many
groups serially. All measurements are process-local diagnostics.

`prepare` covers native artifact validation and Arrow/context writing; `queue_wait`
covers lease acquisition; `cold_start` covers spawn/import/model initialization;
`execution` covers the leased job through native Arrow decoding and port checks.
`warm_service` and `cold_service` sum preparation and execution, excluding pool
waiting and cold initialization. `total` adds those two excluded phases. These
job timings exclude scheduler input construction, final native semantic
validation, publication, worker retirement, and scratch cleanup. Failed leased
jobs contribute timing samples too. `returned_jobs` means decoded worker results,
and does not imply publication; startup failures and queue timeouts are separate.
`p50_upper_ms` and `p95_upper_ms` are fixed histogram bucket upper bounds; null
means no samples or overflow (inspect count/buckets).

Worker RSS uses kernel sampling on macOS/Linux. `telemetry.workers[].rss_bytes`
is current root-process RSS; descendants and the server are excluded, and shared
pages count in each worker. Aggregate `rss_bytes` is null unless every resident
slot has a registered, readable PID. `rss_sampled_bytes` sums known values and
`rss_complete` exposes coverage. `rss_peak_observed_bytes` is the highest sampled
sum, not an OS lifetime peak or memory cap. Registry and occupancy transitions
are adjacent rather than one atomic snapshot. Unsupported platforms return null
RSS. Sampling occurs on stats reads and worker removal, not each completed job.

Timeseries parameters include `uri`, optional `start`/`end` datetime strings or
`start_ns`/`end_ns` integers, optional `limit`, `order=asc|desc`, and optional
published `epoch`. Bounds are half-open. Without a limit, paging returns the
complete range. Related reads should use the same captured epoch. `value_mode` is
`default` (the stream's declared kind), `numeric`, `text` or `coalesce` (every
reading as a string); the Arrow `value` column is `float64` for numeric reads
and `string` otherwise.
