# Experiments

Experiments record script- or notebook-driven work without changing how that
work executes. Define a reusable study once, declare its variables at module
scope, then start one durable run for every scenario.

```python
import acquirium as aq

ac = aq.Acquirium()
study = ac.experiment.define("load-shift")

configuration = study.input("configuration").json()
total_cost = study.output("total operating cost").scalar(unit="USD")
tank_volume = study.output("tank volume").timeseries(
    observed=ac.point("urn:plant:tank-volume"), unit="M3"
)
solver_log = study.log("solver events")

for config in configurations:
    e = study.start(metadata={"scenario": config["name"]})
    try:
        configuration.set(config)
        result = solve(config)  # ordinary application code
        total_cost.set(result.cost)
        tank_volume.add(result.volume_rows)
        solver_log.append({"event": "solve-complete"})
        e.finish()
    except Exception as error:
        e.fail(error)
        raise
```

## Variables

`input`, `output`, and `log` are roles. The type method selects the
value form: `json()`, `text()`, `scalar(unit=...)`, `file(media_type=...)`,
`timeseries(observed=..., unit=...)`, or the append-only `log()` variable.

The label is required and unique within the study. The returned object is the
thing scripts interact with: `set()` records a single JSON/text/scalar value,
`append()` adds an event, `attach()` copies a file into Acquirium's
content-addressed artifact store, and `add()` writes normal Acquirium time
series data.

Every declaration and value mutation receives a server UTC timestamp and a
per-run sequence number. `append()`/`set()` can also provide `occurred_at` for
the time an external event actually happened. Time-series rows retain their
own sample timestamps.

## Time series and graph links

Use a time-series output when a run produces values that change over time: a
forecast, simulated tank level, control schedule, or optimizer trajectory.
It is still normal Acquirium data. The experiment API chooses a unique stream
for this run and writes it through Acquirium's usual storage path, so the
result can be queried or plotted the same way as data from a driver.

`observed` says *what physical or modeled thing the values describe*. It is the
URI of the observable property in the plant knowledge graph. For example, this
declares that the values are the storage tank's volume, not merely a column of
numbers named `volume`:

```python
tank_volume = study.output("tank volume").timeseries(
    observed=ac.resolve("tank volume"),
    unit="M3",
)
```

`ac.resolve("tank volume")` uses Acquirium's normal text resolver to find the
best matching graph resource. It is convenient in a notebook after the plant
graph has been loaded. For a reusable production script, prefer a stable URI;
`ac.point(uri)` wraps it in the same small graph-resource object. Passing the
URI string directly is equivalent:

```python
facility_load = study.output("facility net load").timeseries(
    observed="urn:flex-pse-example:pump-tank-battery#facility-net-load",
    unit="KiloW-HR",
)
```

After starting a run, add ordinary `(timestamp, value)` rows:

```python
e = study.start(metadata={"scenario": "baseline"})
tank_volume.add([
    (aq.timestamp("2025-07-01T00:00:00Z"), 200.0),
    (aq.timestamp("2025-07-01T01:00:00Z"), 245.0),
])
e.finish()
```

`aq.timestamp()` accepts an ISO-8601 string and returns a timezone-aware UTC
`datetime`. `add()` also accepts ISO strings directly, so the compact form
`("2025-07-01T00:00:00Z", 200.0)` works when no `datetime` object is needed.

The run receives a distinct source/ref identity, so a second scenario cannot
overwrite this one. Acquirium records the stream's point URI, which keeps the
result connected to the tank-volume property in the knowledge graph. The same
data can be fetched with the normal stream client once its reference URI is
known; experiment provenance additionally records which run wrote it and the
time range written.

Runs are terminal: after `finish()` or `fail()`, variable mutation is rejected.
Start the study again for the next scenario; the declared variable objects are
reused but each run's values remain isolated.
