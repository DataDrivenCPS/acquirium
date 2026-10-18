---
title: Server configuration
---

All servers use one embedded Escapement engine and one embedded Oxigraph graph.
Paths resolve relative to the configuration file.

```toml
[server]
host = "127.0.0.1"
port = 8000
data_dir = ".acquirium-native"
exact_only = true
recreate = false
engine_workers = 2
epoch_requests = 128
max_pending_requests = 10000
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `data_dir` | `.acquirium` | Graph, indexes, deployment manifest, and default engine database |
| `engine_path` | `data_dir/escapement.duckdb` | Native DuckDB persistence |
| `engine_workers` | 2 | Maximum concurrent native operator executions |
| `python_pool_size` | `engine_workers` | Maximum resident Python workers across function artifacts; zero uses fresh processes for profiling |
| `python_max_worker_jobs` | 10000 | Recycle a process after this many jobs |
| `python_max_worker_age` | 1800 | Recycle expired processes between jobs, in seconds |
| `epoch_requests` | 128 | Maximum accepted requests processed per epoch |
| `max_pending_requests` | 10000 | Native admission/backpressure bound |
| `graph_path` | `data_dir/.oxigraph` | Embedded RDF storage |
| `exact_only` | false | Resolve exact ontology surfaces without downloading an embedding model |
| `recreate` | false | Explicitly erase the configured data directory at startup |
| `read_batch_size` | 50000 | Arrow response page size |
| `workers` | 1 | HTTP processes; must remain one |
| `host`, `port` | `0.0.0.0`, 8000 | Standalone bind address |

`aq.init()` uses loopback, an automatically selected port, persistent storage,
and exact-only resolution by default. Its first caller owns the server lifetime.
Changing the stored app/function definitions uses native plan activation rather
than an Acquirium materialization polling interval.

Environment overrides include `ACQUIRIUM_DATA_DIR`, `ACQUIRIUM_ENGINE_PATH`,
`ACQUIRIUM_ENGINE_WORKERS`, `ACQUIRIUM_PYTHON_POOL_SIZE`,
`ACQUIRIUM_PYTHON_MAX_WORKER_JOBS`, `ACQUIRIUM_PYTHON_MAX_WORKER_AGE`, `ACQUIRIUM_EPOCH_REQUESTS`,
`ACQUIRIUM_MAX_PENDING_REQUESTS`, `ACQUIRIUM_GRAPH_PATH`,
`ACQUIRIUM_EXACT_ONLY`, `ACQUIRIUM_EMBEDDING_MODEL`, `ACQUIRIUM_RECREATE`, and
`ACQUIRIUM_WORKERS`. Backend selection and PostgreSQL DSNs have been removed.

`[ontologies] sources` still accepts RDF paths/URLs and `{source, as}` records for
canonical IRI overrides. `[[drivers]]` entries name driver specs and source-specific
configuration. `[[apps]]` entries name a declaration or `(client, parameters)`
factory, for example:

```toml
[[apps]]
spec = "examples/transformation/temperature_conversion.py:fahrenheit_to_celsius"
```

Configured drivers start before configured app factories so their plant metadata
is available for semantic selection. See the [driver reference](drivers.md) and
[native app reference](apps.md).
