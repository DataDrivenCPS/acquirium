---
title: Getting started
---

<!-- TODO: intro -->

For a script or notebook, you can let Acquirium start a local server:

```python
import acquirium as aq

acq = aq.init()  # loads ./acquirium.toml if present
# Load a model and use acq through the usual client interface.
aq.shutdown()
```

Pass `aq.init("my-custom-config.toml")` to select another config file.
Install the package with `pip install acquirium` first. See
[starting from a script](../how-to/local-runtime.md) for sharing and lifetime
rules. To run a server independently of a script, use the CLI:

```bash
pip install acquirium          # extras: acquirium[mqtt], [xlsx], [watertap]

acquirium server --config acquirium.toml
```

The first start builds the text-resolution indexes and can take 5-10 minutes;
later starts reuse the cache under `data_dir/embedding_cache`.
See [the embedding indexes](../explanation/server-internals.md#the-embedding-indexes)
for what is being built and when it is rebuilt.
The server answers on `http://127.0.0.1:8000` (`GET /health`) once the core
is up.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
acq.query().entity("pump").metadata()
```

A fresh server starts with no model loaded.
The examples throughout these docs run on the public WaterTAP seawater-ro
model.
Getting one running is the [WaterTAP deployment guide](https://github.com/DataDrivenCPS/acquirium/blob/main/deployments/WATERTAP/readme.md):
clone the repo, install the `watertap` extra, and start the server against
`deployments/WATERTAP/models/seawater-ro/acquirium.toml`.
