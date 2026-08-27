---
title: Getting started
---

<!-- TODO: intro -->

```bash
pip install acquirium          # extras: acquirium[mqtt], [xlsx], [watertap]

acquirium server --config acquirium.toml
```

The first start builds the text-resolution indexes and can take 3-5 minutes;
later starts reuse the cache.
The server answers on `http://localhost:8000` (`GET /health`) once the core
is up.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
acq.query().entity("pump").metadata()
```

A fresh server starts with no model loaded.
The examples throughout these docs run on the public WaterTAP seawater-ro
model.
<!-- FT1 placeholder: link the seawater-ro run guide here once it exists.
     Until then: deployments/WATERTAP/readme.md in the repo. -->
