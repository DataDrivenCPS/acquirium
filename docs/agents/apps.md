---
name: acquirium-apps
description: Write, register, run and debug Acquirium apps — the App contract, outputs, params, lifecycle.
load_when: The task involves subclassing App, register_app/run_app/stop_app/delete_app, app outputs, or an app that does not run or write.
human_doc: ../apps.md
---

# Acquirium apps

An app = `App` subclass with `build_query` (what data), optional `build_app`
(one-time state), `run` (returns `list[Output]`). Runs server-side in a Ray
actor; source file is shipped by `register_app`.

## Signatures

```python
from acquirium import Acquirium, App, AppContext, Output

class X(App):
    name: str                       # required, unique
    version = "0.0"
    app_type = "soft_sensor"        # | "threshold" | "alarm" | "report" | any str/URI
    outputs = [{"kind": "timeseries|event|trigger", "point_uri": "...",
                # optional: "quantity_kind", "unit", "data_source", "storage_backend"
               }]
    def build_query(self, aq: Acquirium) -> Query | dict[str, Query]: ...
    def build_app(self, ctx: AppContext) -> Any: ...        # optional; return -> ctx.state
    def run(self, ctx: AppContext) -> list[Output]: ...

Output.timeseries(*, point_uri, rows=[(datetime, value), ...])      # or series=, time_index=
Output.event(*, point_uri, severity, message, ts=None, data=None)
Output.trigger(*, url, message, point_uri=None, ts=None, headers=None, timeout=None)

acq.register_app(app, *, params=None, replace=False, depends_on=None,
                 resolve_dependencies=True, queries=None, outputs=None, app_type=None)
acq.run_app(app_id, *, start=None, end=None, params=None, keep_alive=False, interval=10.0)
acq.stop_app(app_id=...)            # keyword-only
acq.delete_app(app_id)
acq.list_app_runs(app_id=None)      # None -> all apps; app_id -> build/runs status
```

`AppContext`: `app_id, started_at, start, end, query, queries, params, state, data(always None)`.

## Decision rules

- One query → return a `Query`; it is `ctx.query`. Several → return a dict;
  `ctx.queries[name]`, `ctx.query` = `"default"` entry (else the first).
- Expensive setup (training, baselines) → `build_app`, config via
  `register_app(params=)`. Per-run knobs → `run_app(params=)`. Same
  `ctx.params` attribute, different source per phase.
- Value to store → `Output.timeseries` (numeric stream). Discrete happening
  → `Output.event` (text stream, JSON `{severity,message,data}`). Notify
  something outside → `Output.trigger` (HTTP POST, nothing stored).
- Continuous → `keep_alive=True, interval=`. One evaluation → default.
- Cheap latest value inside `run`: `ctx.query.dataframe(limit=1, order="desc")`,
  not `data().latest()` (fetches the whole window).

## Hard rules

- `build_query` MUST return `acq.query()`-built `Query` objects (or a dict of
  them). Nothing else is serializable to the server.
- All `Output.*` factories are keyword-only. `Output.event` REQUIRES
  `point_uri`. `Output.trigger` REQUIRES `url`.
- Every written output's stream is `(source_id="app:<name>", ref_name=<point_uri>)`.
  Declare the point in `outputs` so it exists in the graph before the first run.
- Do NOT set instance attributes in `run` and expect them next run: each run
  is a separate Ray task on a pickled copy. State lives in `ctx.state`.
- `build_app` is NOT re-run on graph change; only `build_query` is (keep-alive).
- Imports must exist in the SERVER's environment. Only the entry file ships;
  sibling-module imports fail on the server.
- `self.insert_graph` / `self.insert_graph_file` / `self.sparql_update`
  inside an app write ONLY the app's own graph (`source_id="app:<name>"`);
  never the plant model. They take no owner argument, by design.
- `replace=True` on re-register, else 409. `stop_app` is a no-op unless the
  app is in keep-alive.
- After a server restart apps are registered+built but NOT running; call
  `run_app` again. An app whose `<data_dir>/apps/<name>/` dir is gone is not
  restored.
- `delete_app` removes graph triples and source; the timeseries rows stay.

## Error → cause

| symptom | cause |
|---|---|
| `409 ... already registered; pass replace=True` | name exists, no `replace=True` |
| `TypeError: ... got an unexpected keyword` / positional on `Output.*` | factories are keyword-only |
| `AttributeError: ... has no attribute 'to_dict'` at register | `build_query` returned something other than a `Query` |
| `list_app_runs(app_id=)["build"] == "pending"` | `setup` failed at registration; see server log; first `run_app` retries |
| run `status: failed`, `error: 4xx/5xx` | trigger webhook returned non-2xx (`raise_for_status`) |
| run `status: failed`, `error: model not built` (or similar) | `ctx.state` is None: `build_app` raised or was skipped |
| `RuntimeError: ... already running keep-alive` | second `run_app(keep_alive=True)` without `stop_app` |
| `KeyError: Unknown app` / 404 | wrong `app_id`, or app not restored (missing source dir) |
| `ModuleNotFoundError` in the server log at setup | package/sibling missing on the server |
| app registered but no data at its point | `run` returned `[]`, or the run failed; check `list_app_runs(app_id=)` |

## Canonical snippets

Minimal trigger app (webhook every 10 s):

```python
from acquirium import Acquirium, App, AppContext, Output

class ROFlowMonitor(App):
    name = "ro_flow_monitor"
    app_type = "threshold"
    outputs = [{"kind": "trigger", "point_uri": "urn:derived:ro_flow_alert"}]

    def build_query(self, aq):
        return (aq.query().entity(uri="wbs:RO", alias="ro")
                  .measurement(alias="flow", quantity_kind="mass flow rate"))

    def run(self, ctx):
        df = ctx.query.dataframe(limit=1, order="desc")
        if df.is_empty():
            return []
        return [Output.trigger(url="localhost:10000/alerts",
                               message={"text": f"RO flow {df['value'][0]:.2f}"},
                               point_uri=self.outputs[0]["point_uri"])]

acq = Acquirium(server_url="localhost", server_port=8000)
acq.register_app(ROFlowMonitor(), replace=True)
acq.run_app("ro_flow_monitor", keep_alive=True, interval=10)
# later
acq.stop_app(app_id="ro_flow_monitor")
```

Stateful soft sensor skeleton (build once, score each run):

```python
class SoftSensor(App):
    name = "soft_sensor"
    outputs = [{"kind": "timeseries", "point_uri": "urn:derived:estimate",
                "quantity_kind": "mass flow rate", "unit": "kg/s"}]

    def build_query(self, aq):
        ro = aq.query().entity(uri="wbs:RO", alias="ro")
        return {"feed": ro.measurement(alias="p", quantity_kind="pressure"),
                "permeate": ro.measurement(alias="f", quantity_kind="mass flow rate")}

    def build_app(self, ctx):
        X = ctx.queries["feed"].dataframe(start=ctx.params["t0"], end=ctx.params["t1"], shape="wide")
        y = ctx.queries["permeate"].dataframe(start=ctx.params["t0"], end=ctx.params["t1"], shape="wide")
        return {"model": fit(X, y)}          # -> ctx.state

    def run(self, ctx):
        X = ctx.queries["feed"].dataframe(limit=ctx.params.get("window", 48), order="desc", shape="wide")
        yhat = ctx.state["model"].predict(X)
        return [Output.timeseries(point_uri=self.outputs[0]["point_uri"],
                                  rows=list(zip(X["time"], yhat)))]

acq.register_app(SoftSensor(), params={"t0": t0, "t1": t1}, replace=True)
acq.run_app("soft_sensor", keep_alive=True, interval=60, params={"window": 48})
```

## Anti-patterns

- Returning legacy `find_entity(...)` chains from `build_query` in new code.
- `Output.event(severity=..., message=...)` without `point_uri`.
- Training the model in `run`. Put it in `build_app`.
- Caching data on `self` across runs.
- Skipping `outputs` and inserting to an unregistered point from `run`
  (400: stream not registered).
- Calling `register_app` in a loop without `replace=True`.
- Expecting `run_app(keep_alive=True)` to survive a server restart.
