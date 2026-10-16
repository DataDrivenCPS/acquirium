---
title: HTTP API
---

The server exposes semantic graph operations and one Escapement timeseries engine.

| Endpoint | Contract |
| --- | --- |
| `GET /health` | Server readiness |
| `GET /graph_version` | Source/inferred graph generations |
| `POST /insert_graph` | RDF text/file, format, source owner, replace/append that source's graph |
| `POST /sparql_update` | Update one explicitly owned data graph |
| `GET /sparql` | SPARQL protocol read endpoint |
| `GET/POST /sparql_json` | SPARQL SELECT as `{columns, rows}` |
| `GET /export_graph` | Serialized RDF; optional dependencies |
| `POST /validate_graph` | SHACL validation report |
| `GET /namespace/list` | Prefix map |
| `POST /register_datasource` | Source identity in RDF |
| `POST /resolve_storage_keys` | Point/reference URIs to canonical references; ambiguous points fail |
| `POST /insert_timeseries` | JSON list of `{source_id, ref_name, request_id, values}` |
| `POST /insert_timeseries_arrow` | Arrow `source_id, ref_name, ts, value`; required `X-Acquirium-Request-Id` |
| `POST /delete_timeseries` | `{source_id, ref_name, timestamps: [UTC-ns], request_id}` |
| `GET /timeseries` | Arrow `ts: timestamp[ns, UTC], value: float64, uri: utf8` |
| `POST /timeseries_info` | `{uris: [...]}` to counts and UTC bounds |
| `GET /engine/status` | Accepted/applied frontiers, backlog, paused error, diagnostic timings |
| `GET /engine/snapshot` | Published epoch, applied sequence, complete active plan |
| `POST /engine/retry` | Explicitly retry a paused epoch |
| `GET /apps` | Desired declarations and active operators |
| `GET/PUT/DELETE /apps/{name}` | Inspect, deploy a native semantic declaration, or remove |
| `GET /resolve_text`, `POST /resolve_record` | Semantic text resolution |
| `POST /resolve_unit`, `/resolve_conversion`, `/conversion_factors` | Unit resolution and conversion factors |
| `GET /embedding_status` | Exact/semantic index state |
| `GET /drivers/list`, `POST /drivers/start`, `/drivers/stop` | Driver lifecycle |

JSON values contain timestamp/value pairs. Integer timestamps are UTC nanoseconds;
timezone-aware datetime strings are also accepted. Values must be finite non-null
numbers. Arrow timestamps must be timezone-aware. Duplicate keys keep the last
row before native submission; source/request IDs and normalized payloads identify
retries within the bounded native retry window.

JSON rows sharing a source/request ID commit as one request. Different sources
or IDs commit independently. Ingestion's `wait` query parameter is a nonnegative
publication timeout in seconds (default 30, zero for immediate durable admission).
Responses include database-scoped receipt sequences and `accepted`/`published`
state. Failed or timed-out computation after admission remains accepted/pending;
inspect the receipt and native status before retrying with the same identity.

Timeseries parameters include `uri`, optional `start`/`end` datetime strings or
`start_ns`/`end_ns` integers, optional `limit`, `order=asc|desc`, and optional
published `epoch`. Bounds are half-open. Without a limit, paging returns the
complete range. Related reads should use the same captured epoch. Numeric values
are the only projection; `value_mode` accepts `default` or `numeric`.

Legacy SQL/logbook, Timescale, whole-stream replacement, class-based app
registration, reprocessing, and app-check endpoints have been removed.
