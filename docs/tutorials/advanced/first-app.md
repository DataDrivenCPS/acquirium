---
title: Declaring and deploying apps
---

An app calculates streams from existing readings. This tutorial adds power in kilowatts to each pump in the [full example](../a-full-example.md). Run that tutorial first and use the same data folder.

The [example script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_apps.py) and [notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/advanced_apps.ipynb) use the declaration in [plant_apps.py](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/plant_apps.py).

## Declare the calculation

The query finds each pump's power measurement. `Selection.from_query` binds the matching stream for each pump to the named input `power`. The selection skips the app’s own outputs so that redeploying keeps the same inputs. The `Pointwise` window runs the function for each affected timestamp. The function emits one output in kilowatts for each input value.

```{literalinclude} ../../../examples/documentation/plant_apps.py
:language: python
```

## Preview and deploy

```python
import acquirium as aq
from plant_apps import pump_power_kw

acq = aq.init()
app = pump_power_kw(acq)
print(acq.deploy_app(app, dry_run=True))
print(acq.deploy_app(app))
```

The preview estimates the work before activation. Deployment computes existing history and keeps the outputs current as readings arrive or are corrected.

```python
readings = acq.query().entity("pump", alias="pump").measurement(app="pump-power-kw")
print(readings.dataframe(shape="narrow", limit=3))

acq.insert_timeseries(
    source_id="csv-data",
    stream_name="P1-mechanical-power",
    rows=[("2026-01-01", 1_200_000.0)],
)
print(readings.dataframe(shape="narrow", start="2026-01-01"))
```

The new reading produces 1,200 kW. A correction at the same timestamp will update the output. To deploy a changed calculation, rebuild the declaration from the edited file and call `deploy_app` again.

```python
acq.remove_app(app.name)
aq.shutdown()
```

See the [app reference](../../reference/apps/_index.md) for window and output options and [deploying and inspecting apps](../../how-to/check-deploy-apps.md) for recovery steps.
