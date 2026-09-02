---
title: Getting started
---

<!-- TODO: intro -->

```bash
pip install acquirium          # extras: acquirium[mqtt], [xlsx], [watertap]

acquirium server --config acquirium.toml
```

The first start builds the text-resolution indexes and can take 5-10 minutes;
later starts reuse the cache under `data_dir/embedding_cache`.
See [the embedding indexes](../explanation/server-internals.md#the-embedding-indexes)
for what is being built and when it is rebuilt.

The server answers on `http://localhost:8000` (`GET /health`) once the core
is up.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
acq.query().entity("pump").metadata()
```

A fresh server starts with no model loaded.

The examples in these tutorials use the public WaterTAP seawater-ro
model, which produces simulated data.

To run WaterTAP, follow the [WaterTAP deployment guide](https://github.com/DataDrivenCPS/acquirium/blob/main/deployments/WATERTAP/readme.md):

clone the repo, install the `watertap` extra, and start the server against
`deployments/WATERTAP/models/seawater-ro/acquirium.toml`.
