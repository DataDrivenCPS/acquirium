---
title: Installation and startup
---

Acquirium runs as a server. Drivers feed it readings from the plant, and your
scripts and notebooks ask it questions through a Python client. Install the
package first:

```bash
pip install acquirium          # extras add drivers: acquirium[mqtt], [xlsx], [watertap]
```

There are two ways to get a server running. Which one you want depends on how
long it needs to live.

## Server in a Python script

When you are exploring in a notebook, let the client start a server for you:

```python
import acquirium as aq

acq = aq.init()
acq.query().measurement().metadata()   # every stream the server knows about
aq.shutdown()
```

`aq.init()` reads `acquirium.toml` from the current directory if there is one,
starts a server in the background, and returns a client connected to it.
`aq.init("plant.toml")` uses a different config file. When the script ends, so
does the server. [Starting from a script](../how-to/local-runtime.md) explains
how several scripts can share one server.

## Standalone server

Drivers that poll a SCADA export overnight, or a plant model that several
people query, need a server that outlives any one script. Start it from the
command line:

```bash
acquirium server --config acquirium.toml
```

and connect to it from Python:

```python
from acquirium import Acquirium

acq = Acquirium(server_url="127.0.0.1", server_port=8000)
```

The two servers differ in one respect. When you ask for `"pump"`, the server
has to work out which concept in the plant vocabulary you mean. A server
started by `aq.init()` matches names exactly, so it starts at once. A
standalone server also matches by meaning, which needs an embedding model: the
first start downloads it and builds an index, and later starts reuse the
cache. [Text resolution](../reference/server-config.md#text-resolution) covers
the setting.

## Related tutorials

A new server is empty. It has no plant model and no streams, so the tutorials
fill it in one of two ways.

The [querying](querying.md) and [data](data.md) tutorials use a simulated
seawater reverse-osmosis plant from WaterTAP, which comes with a plant model
and a steady supply of readings. Start it by following the
[WaterTAP deployment guide](https://github.com/DataDrivenCPS/acquirium/blob/main/deployments/WATERTAP/readme.md).

To begin with your own data instead, start with
[File ingestion driver](first-driver.md), which reads a folder of CSV files and
needs no plant model at all.
