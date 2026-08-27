# Building apps

This is a guide to authoring apps.
Apps are the components that compute on plant data server-side and write the
results back into acquirium.
Drivers feed data in ([drivers guide](drivers.md)); apps read it back out and
derive new values from it.

Every example here runs on the public WaterTAP seawater-ro model.
<!-- FT1 placeholder: link the seawater-ro run guide here once it exists.
     Until then: deployments/WATERTAP/readme.md in the repo. -->

## Introduction

An app is a Python class that declares what data it needs as a query, computes
something on that data, and returns outputs.
Soft sensors, threshold monitors and alerting rules are all apps.

Outputs are either written back as timeseries streams (a computed value, an
event) or sent out as an HTTP webhook (a trigger).
Written outputs are ordinary streams: a query finds them the same way it finds
sensor data.

There is no CLI command and no config section for apps.
An app is deployed from Python with `acq.register_app(...)`.

## Where apps run

Apps do not run in your process.
`register_app` reads the source file of the app class and ships it to the
server.
The server writes it under `<data_dir>/apps/<app name>/`, spawns one Ray actor
per app on the server host, and imports the class there.
The actor talks back to the server with an `Acquirium` client.

Be aware that the app runs with the server's Python environment and
privileges; it is not sandboxed.
Any package the app imports must be installed on the server side.
Only the entry file is shipped: an app that imports sibling modules from your
project will not load.

## The app lifecycle

```text
register_app()       ship the source, write the registration graph
setup                build_query()  ->  build_app()        once, at registration
run_app()            one-shot: one run
                     keep-alive: run now, then every `interval` seconds
                         graph changed since last run?  ->  build_query() again
stop_app()           end the keep-alive loop
delete_app()         remove the app, its graph triples and its source
```

On a server restart every registered app is restored to the state after
`setup`: registered and built, but not running.

## The contract

```python
from acquirium import Acquirium, App, AppContext, Output

class MyApp(App):
    name = "my_app"                       # unique; the handle for run/stop/delete
    version = "0.1"
    app_type = "soft_sensor"
    outputs = [{"kind": "timeseries", "point_uri": "urn:derived:my_value"}]

    def build_query(self, aq: Acquirium):
        ...                               # what data the app needs

    def build_app(self, ctx: AppContext):
        ...                               # optional, runs once

    def run(self, ctx: AppContext) -> list[Output]:
        ...                               # runs on every execution
```

`build_query` and `run` are required.
`build_app` is optional and returns `None` by default.

| attribute | meaning |
|---|---|
| `name` | unique id, no default |
| `version` | stored with the registration, default `"0.0"` |
| `app_type` | `soft_sensor` (default), `threshold`, `alarm`, `report`, or any string or URI; becomes the app's RDF class |
| `outputs` | list of output specs, see [Outputs](#outputs) |
| `source_code`, `entry_file` | override what `register_app` ships; normally left unset |

### build_query()

`build_query(aq)` receives the `Acquirium` client and returns a `Query`, or a
dict of named queries.

```python
def build_query(self, aq):
    return aq.query().entity(uri="wbs:RO", alias="ro").measurement(alias="flow", quantity_kind="mass flow rate")
```

A single query is available in `run` as `ctx.query`.
A dict is available as `ctx.queries`, and `ctx.query` is the entry named
`"default"` (or the first one when there is no default).

```python
def build_query(self, aq):
    ro = aq.query().entity(uri="wbs:RO", alias="ro")
    return {
        "pressure": ro.measurement(alias="p", quantity_kind="pressure"),
        "flow":     ro.measurement(alias="f", quantity_kind="mass flow rate"),
    }
```

`build_query` runs at three points: on the client at registration (to
serialize the query and compute what the app depends on), on the server at
setup, and again on the server whenever the graph changes while the app runs
keep-alive.
This is why the query is a description and not a result: the app picks up new
equipment or new points without being re-registered.

### build_app()

`build_app(ctx)` runs once, at setup.
This is where a stateful app does its expensive work, for instance training a
model on a baseline window.
Whatever it returns is kept on the actor and handed to every run as
`ctx.state`.
`ctx.params` here are the params given to `register_app(params=...)`.

Note that `build_app` is not re-run when the graph changes; only
`build_query` is.
State built against an old query keeps being used.

### run()

`run(ctx)` returns a list of `Output`.
`ctx.params` here are the params given to `run_app(params=...)`.
Each run executes as a separate Ray task, so the app instance must not rely on
attributes set during a previous run; keep run-to-run state in `ctx.state`.
An exception in `run` marks that run `failed` and is recorded in the run
history; the keep-alive loop continues.

### AppContext

| field | meaning |
|---|---|
| `app_id` | the app name |
| `started_at` | when this run was dispatched |
| `start`, `end` | the window given to `run_app(start=, end=)`, or `None`; the app decides how to use them |
| `query` | the single query, or the default one |
| `queries` | the dict of named queries (`{"default": q}` for a single query) |
| `params` | build params in `build_app`, run params in `run` |
| `state` | what `build_app` returned |
| `data` | reserved; currently always `None` |

## Outputs

`run` returns `Output` objects built with three keyword-only factories.

| factory | what happens |
|---|---|
| `Output.timeseries(point_uri=, rows=)` | rows are inserted into the point's stream; `rows` is a list of `(datetime, value)`, or pass `series=` with a `time_index=` |
| `Output.event(point_uri=, severity=, message=, ts=None, data=None)` | one text row is inserted, a JSON object `{"severity", "message", "data"}` at `ts` (default now) |
| `Output.trigger(url=, message=, point_uri=None, ts=None, headers=None, timeout=None)` | `POST url` with a JSON body `{"message", "ts"}` (+`"point_uri"` if given); a schemeless URL gets `http://`; timeout defaults to 5 s; a non-2xx response fails the run |

Every written output goes to a stream whose `source_id` is `app:<name>` and
whose `ref_name` is the point URI.
See the [lifecycle guide](data-stream-lifecycle.md#app-outputs-are-streams-too).

The `outputs` attribute declares these points ahead of time so they exist in
the graph before the first run.
Each entry needs `kind` (`timeseries`, `event`, `trigger`) and `point_uri`;
`quantity_kind`, `unit`, `data_source` and `storage_backend` are optional
and are written onto the point.
`timeseries` outputs register a numeric stream, `event` and `trigger` a text
one.
Note that a trigger does not write to its stream.
The declared point only makes the webhook visible in the graph as an output
of the app.

## Registering

```python
acq = Acquirium(server_url="localhost", server_port=8000)
acq.register_app(MyApp(), params={"k_sigma": 4.0}, replace=True)
```

`register_app` does the following, in order:

1. Calls `build_query` and serializes the result.
2. Executes each query to derive `depends_on`, the URIs the app reads.
   Pass `depends_on=[...]` to set them yourself, or
   `resolve_dependencies=False` to skip this step and its SPARQL cost.
3. Reads the class's source file.
4. Sends everything to `POST /apps/register`.

On the server the app is written to disk, its registration graph is inserted,
and `setup` runs (`build_query`, then `build_app`).
A failure in `setup` does not fail the registration.
The app stays registered with build status `pending`, and the first `run_app`
retries `setup` before running.
Check `acq.list_app_runs(app_id="my_app")["build"]` after registering an app
with a build phase.

Registering a name that already exists fails with `409` unless
`replace=True`, which stops and deregisters the old app first.

## Running

```python
acq.run_app("my_app")                                   # one run
acq.run_app("my_app", keep_alive=True, interval=60)      # a run now, then every 60 s
acq.run_app("my_app", start=t0, end=t1, params={"window": 48})
```

A one-shot run dispatches once and returns a `run_id`.
Keep-alive runs immediately, then every `interval` seconds until stopped.
Before each iteration the actor checks the server's graph version; if it
changed, `build_query` is called again.
Starting keep-alive on an app already in keep-alive raises an error.

`acq.list_app_runs()` lists the apps with their running state.
`acq.list_app_runs(app_id="my_app")` returns the build status, the query
names, the state type, and the last 50 runs with `status`
(`running`/`done`/`failed`), the output count and the error message.

## Stopping, deleting, restarts

`acq.stop_app(app_id="my_app")` ends the keep-alive loop after the current
iteration.
It is a no-op for an app that is not running keep-alive.

`acq.delete_app("my_app")` stops the app, deletes its registration triples
(the app node, its produced points and their reference nodes), kills the
actor and removes the source directory.
Note that the timeseries rows the app wrote are not deleted.

On a server restart, registered apps are rebuilt from the graph and the source
directory and go through `setup` again.
Be aware of three limits:

- Keep-alive is not persisted. A restored app is not running until you call
  `run_app` again.
- An app whose source directory is missing is not restored; the server logs an
  error and skips it.
- Trigger outputs come back as `event` outputs after a restore. This only
  matters for the graph registration, which the restore does not rewrite.

## A minimal app

A monitor that reads the latest RO mass flow and posts it to a webhook.
`scripts/watertap/monitor_gui.py` is a small dashboard that accepts
`POST /alerts` on port 10000 and renders the messages.

```python
from acquirium import Acquirium, App, AppContext, Output


class ROFlowMonitor(App):
    name = "ro_flow_monitor"
    version = "0.1"
    app_type = "threshold"
    outputs = [{"kind": "trigger", "point_uri": "urn:derived:ro_flow_alert"}]

    def build_query(self, aq: Acquirium):
        return (aq.query().entity(uri="wbs:RO", alias="ro")
                  .measurement(alias="flow", quantity_kind="mass flow rate"))

    def run(self, ctx: AppContext) -> list[Output]:
        d = ctx.query.data()
        latest = d.latest("flow")
        if latest.is_empty():
            return []
        value = float(latest["value"][0])
        unit = d.units()["flow"].rsplit("/", 1)[-1]
        return [Output.trigger(
            url="localhost:10000/alerts",
            message={"text": f"RO mass flow is {value:.2f} {unit}"},
            point_uri=self.outputs[0]["point_uri"],
        )]


if __name__ == "__main__":
    acq = Acquirium(server_url="localhost", server_port=8000)
    acq.register_app(ROFlowMonitor(), replace=True)
    acq.run_app("ro_flow_monitor", keep_alive=True, interval=10)
```

<!-- pending live verification: server was down when this was written -->

`scripts/watertap/ml-workload.py` is the larger example: a membrane-fouling
soft sensor with a `build_app` that trains on a baseline window
(`register_app(params=...)`), a dict of two queries, and run params for the
scoring window (`run_app(params=...)`).
`scripts/watertap/fouling_gui.py` is its dashboard on port 10001.
The `scripts/benicia/` directory has the same pair for the Benicia model.

## Notes

`build_query` may return the legacy query builder as well; anything with
`to_dict()` and `resolved_nodes()` works.
New apps should use `acq.query()`.

`register_app(queries={...})` skips calling `build_query` on the client and
ships the given dict instead.
The server still calls the class's `build_query` at setup, so the two should
agree.

The app storage root is `<data_dir>/apps`, or `$ACQUIRIUM_APP_STORAGE_ROOT`.

`ctx.query.data()` and `ctx.query.dataframe()` in `run` behave exactly as in
the [data guide](tutorials/data.md); the query is a normal `Query` bound to the actor's
client.

### What registration writes to the graph

These must change for robust provenance

```turtle
<urn:acquirium#app/my_app>
    a               acq:App, acq:SoftSensor ;
    rdfs:label      "my_app" ;
    acq:hasVersion  "0.1" ;
    acq:querySpec   "{...json...}" ;
    acq:paramSpec   "{...json...}" ;
    acq:dependsOn   <urn:swro/RO-in-flow> ;
    acq:produces    <urn:derived:my_value> .

<urn:derived:my_value>
    a                        acq:VirtualPoint ;
    ref:hasExternalReference <urn:acquirium#...> ;
    acq:isCalculatedFrom     <urn:swro/RO-in-flow> .

<urn:acquirium#...>
    a             acq:Stream, acq:TimeseriesStream ;
    acq:sourceId       "app:my_app" ;
    acq:refName        "urn:derived:my_value" ;
    acq:valueKind      "numeric" ;
    acq:storageBackend "timescale" .
```

The dependency links are how provenance is recorded: `acq:isCalculatedFrom`
points every produced point at the points the app read.

All of this lives in the app's own graph, owned by `source_id="app:<name>"`.
An app can add to that graph from `build_app` or `run` with
`self.insert_graph(turtle)`, `self.insert_graph_file(path)` and
`self.sparql_update(update)`.
These always target the app's graph; they cannot write to the plant model or
another app's graph.