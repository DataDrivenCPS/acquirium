---
title: Server internals
---

One server process owns:

- **The Escapement engine**, which stores raw and derived readings and runs app
  calculations, up to `engine_workers` operators at once. Python functions run
  in a pool of at most `python_pool_size` worker processes. See
  [the core architecture](escapement-core.md).
- **The graph store**: Oxigraph holding source-owned RDF, the ontologies and a
  published inferred view. Queries can wait for a fresh view. See
  [the graph backend](graph-backend.md).
- **The experiment ledger**: DuckDB or PostgreSQL at `experiments_path`. It
  defaults to the engine's PostgreSQL database when the engine uses one, and to
  `experiments.duckdb` under the data directory otherwise.
- **The embedding indexes** for free-text resolution, built from the ontologies
  only and cached under the data directory. Plant instances are not embedded.
  `exact_only=true` builds exact matches without downloading a model.
- **Drivers.** Each `[[drivers]]` entry runs as a child process
  (`python -m acquirium.Drivers.runner`) that connects back over HTTP and stops
  with the server.
