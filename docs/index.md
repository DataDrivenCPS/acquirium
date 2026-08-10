# Acquirium documentation

Acquirium is a data platform for water treatment plants.
The server holds two things about a plant: a semantic model (equipment,
piping, sensors and their meaning, described with the ASHRAE 223 and NAWI
water ontologies) and the timeseries of every measured point.
Queries describe what you are looking for in plant terms; the server finds
the matching points and returns their data.

## Architecture

```text
your code ── Acquirium client ──HTTP──▶ acquirium server
                                          ├─ graph store        (semantic model + ontologies)
                                          ├─ timeseries store   (duckdb or timescale)
                                          ├─ text resolution    (embedding indexes)
                                          └─ Ray actors         (drivers and apps)

drivers: feed data in, on a schedule        apps: compute on data, write results back
```

One process hosts everything.
Drivers and apps run inside it as actors and use the same client API your
code uses.

## Install and first run

```bash
pip install acquirium          # extras: acquirium[mqtt], [xlsx], [watertap]

acquirium server --config acquirium.toml
```

The first start builds the text-resolution indexes and can take minutes;
later starts reuse the cache.
The server answers on `http://localhost:8000` (`GET /health`) once the core
is up.

```python
from acquirium import Acquirium

acq = Acquirium(server_url="localhost", server_port=8000)
acq.query().entity("pump").metadata()
```

A fresh server has no plant in it.
The examples throughout these docs run on the public WaterTAP seawater-ro
model.
<!-- FT1 placeholder: link the seawater-ro run guide here once it exists.
     Until then: deployments/WATERTAP/readme.md in the repo. -->

## The guides

| guide | covers |
|---|---|
| [Querying](querying.md) | finding equipment, topology and measurements; the `Query` interface |
| [Working with data](data.md) | fetching timeseries, shapes, units, writing data |
| [Building drivers](drivers.md) | feeding plant data in on a schedule |
| [The data stream lifecycle](data-stream-lifecycle.md) | how streams are identified, stored and found again |
| [Running the server](server.md) | config, storage backends, ontologies, HTTP API |
| [Text resolution](resolution.md) | free text to URIs; unit conversion |
| [Apps](apps.md) | computing on plant data server-side |

The `docs/agents/` directory holds compact per-topic references written for
coding agents; point your agent at them instead of the prose guides.

## Glossary

| term | meaning |
|---|---|
| model | the RDF description of one plant: equipment, piping, points |
| ontology | the shared vocabulary the model is written in (s223, NAWI water, QUDT) |
| entity | a thing in the plant: equipment, a system, a connection point |
| point | a node representing one measured or computed quantity |
| stream | the timeseries behind a point, identified by `(source_id, ref_name)` |
| reference node | the RDF node linking a point to its stream (`ref:hasExternalReference`) |
| datasource | who writes a set of streams: a driver, an import, an app |
| driver | a class the server runs on a schedule to ingest data |
| app | a class the server runs to compute on data and write results back |
| union graph | the model plus the ontology closure; what queries run against |
