---
title: Server configuration
---

All servers use one embedded Escapement engine and one embedded Oxigraph graph.
Paths resolve relative to the configuration file.

```toml
[server]
host = "127.0.0.1"
port = 8000
data_dir = ".acquirium"
exact_only = true
recreate = false
engine_workers = 2
epoch_requests = 128
max_pending_requests = 100000
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `data_dir` | `.acquirium` | Graph, indexes, cached calculation artifacts, and default engine database |
| `engine_path` | `data_dir/escapement-epoch.duckdb` | Escapement store, either a DuckDB file or a PostgreSQL URL |
| `engine_schema` | URL `search_path` when provided, otherwise `acquirium_epoch` | PostgreSQL schema for Escapement |
| `experiments_path` | `engine_path` if it is a `postgres://` URL, else `data_dir/experiments.duckdb` | Experiment ledger: a DuckDB file or a `postgres://` URL. Holds Studies, runs, observations, and artifact bytes; their samples are Escapement streams |
| `engine_workers` | 2 | Python workers available to synchronous app calculations |
| `epoch_requests` | 128 | Maximum accepted requests processed per epoch |
| `max_pending_requests` | 100000 | Maximum accepted requests awaiting publication. A full queue returns HTTP 503 for new requests |
| `history_epochs` | unlimited | Optional number of recent epochs to retain. Older snapshots and feed cursors can expire, including epochs referenced by experiments |
| `drain_timeout` | 10 | Seconds shutdown waits for pending writes to publish; what's left is processed at the next start |
| `graph_path` | `data_dir/.oxigraph` | Embedded RDF storage |
| `exact_only` | false | Resolve exact ontology surfaces without downloading an embedding model |
| `recreate` | false | Explicitly erase the configured data directory and the experiment ledger at startup |
| `read_batch_size` | 50000 | Arrow response page size |
| `workers` | 1 | HTTP processes; must remain one |
| `host`, `port` | `0.0.0.0`, 8000 | Standalone bind address |

`aq.init()` uses loopback, an automatically selected port, persistent storage,
and exact-only resolution by default. Its first caller owns the server lifetime.

Environment overrides include `ACQUIRIUM_DATA_DIR`, `ACQUIRIUM_ENGINE_PATH`, `ACQUIRIUM_ENGINE_SCHEMA`,
`ACQUIRIUM_EXPERIMENTS_PATH`,
`ACQUIRIUM_ENGINE_WORKERS`,
`ACQUIRIUM_EPOCH_REQUESTS`,
`ACQUIRIUM_MAX_PENDING_REQUESTS`, `ACQUIRIUM_HISTORY_EPOCHS`, `ACQUIRIUM_DRAIN_TIMEOUT`, `ACQUIRIUM_GRAPH_PATH`,
`ACQUIRIUM_EXACT_ONLY`, `ACQUIRIUM_EMBEDDING_MODEL`, `ACQUIRIUM_RECREATE`, and
`ACQUIRIUM_WORKERS`.

With `recreate = false`, the experiment ledger's schema upgrades run
automatically through the [versioned migration ledger](storage-migrations.md);
existing records are retained.

## Text resolution

`exact_only=true` resolves exact ontology names and symbols without downloading
an embedding model. With `false`, unmatched text uses semantic similarity.
See [resolve text](../how-to/resolve-text.md) for diagnostics and unit conversion.

## Ontologies

`[ontologies] sources` accepts RDF paths/URLs and `{source, as}` records for
canonical IRI overrides.

## Drivers and apps

`[[drivers]]` entries name driver specs and source-specific configuration.
`[[apps]]` entries name a declaration or `(client, parameters)` factory:


```toml
[[apps]]
spec = "examples/transformation/threshold_app.py:high_wave"
```

A file spec imports under its name without `.py`, so that name must be unique:
a file named like an installed module, or like another app file in a different
directory, is rejected.

Configured drivers start before configured app factories so their plant metadata
is available for semantic selection. See the [driver reference](drivers.md) and
[app reference](apps.md).
