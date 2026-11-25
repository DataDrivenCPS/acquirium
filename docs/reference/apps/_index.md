---
title: Apps
---

An app is a Python class that computes new streams from stored streams. It selects its inputs with a query and returns tables of values. The server runs it again whenever its inputs change, so the computed streams stay up to date.

| Page | Covers |
|---|---|
| [The App class](app-class.md) | `App`, its settings, and the methods an author writes |
| [Outputs](outputs.md) | Declaring output streams and assigning tables to them |
| [Inputs](inputs.md) | What `transform` receives: `StreamSet`, `InputBatch`, `TimeWindow`, and `align` |
| [Deploying](deploying.md) | Client methods to check, deploy, inspect and remove apps, and the `[[apps]]` configuration table |
| [Command line](cli.md) | The `acquirium app` commands |

## All items

| Item | Kind | Page |
|---|---|---|
| [`App`](app-class.md#app) | class | The App class |
| [`App.build_query`](app-class.md#appbuild_query) | method to write | The App class |
| [`App.transform`](app-class.md#apptransform) | method to write | The App class |
| [`App.output_window`](app-class.md#appoutput_window) | optional method | The App class |
| [`output.stream`](outputs.md#outputstream) | function | Outputs |
| [`output.named`](outputs.md#outputnamed) | function | Outputs |
| [`OutputSpec`](outputs.md#outputspec) | class | Outputs |
| [`OutputBuilder`](outputs.md#outputbuilder) | class | Outputs |
| [`StreamSet`](inputs.md#streamset) | class | Inputs |
| [`StreamSet.df`](inputs.md#streamsetdf) | method | Inputs |
| [`StreamSet.collect`](inputs.md#streamsetcollect) | method | Inputs |
| [`StreamSet.batches`](inputs.md#streamsetbatches) | method | Inputs |
| [`StreamSet.in_unit`](inputs.md#streamsetin_unit) | method | Inputs |
| [`StreamDescriptor`](inputs.md#streamdescriptor) | class | Inputs |
| [`InputBatch`](inputs.md#inputbatch) | class | Inputs |
| [`TimeWindow`](inputs.md#timewindow) | class | Inputs |
| [`align`](inputs.md#align) | function | Inputs |
| [`Acquirium.check_app`](deploying.md#acquiriumcheck_app) | client method | Deploying |
| [`Acquirium.deploy_app`](deploying.md#acquiriumdeploy_app) | client method | Deploying |
| [`Acquirium.list_apps`](deploying.md#acquiriumlist_apps) | client method | Deploying |
| [`Acquirium.inspect_app`](deploying.md#acquiriuminspect_app) | client method | Deploying |
| [`Acquirium.reprocess_app`](deploying.md#acquiriumreprocess_app) | client method | Deploying |
| [`Acquirium.remove_app`](deploying.md#acquiriumremove_app) | client method | Deploying |
| [`Acquirium.app_dag`](deploying.md#acquiriumapp_dag) | client method | Deploying |
| [`acquirium app check`](cli.md#acquirium-app-check) | command | Command line |
| [`acquirium app list`](cli.md#acquirium-app-list) | command | Command line |
| [`acquirium app inspect`](cli.md#acquirium-app-inspect) | command | Command line |

## Imports

```python
import acquirium as aq

aq.App, aq.output, aq.OutputSpec, aq.align
```

The other classes are passed to `transform` by the server. To name them in type hints, import them from `acquirium.Materialization`:

```python
from acquirium.Materialization import StreamSet, InputBatch, OutputBuilder, StreamDescriptor, TimeWindow
```

## A minimal app

This app writes a one-minute mean for every stream tagged `data_source="probe"`. Save it as a module, for example `tank_apps.py`.

```python
import polars as pl
import acquirium as aq


class TankAverage(aq.App):
    name = "tank-average"
    grouping = "per_match"
    every = "1m"
    backfill = True
    outputs = {"mean": aq.output.stream(value_kind="numeric", label="Tank mean")}

    def build_query(self, plant):
        return plant.query().measurement(alias="temp", data_source="probe")

    def transform(self, inputs, output, context):
        wide = aq.align(inputs)
        output["mean"] = wide.select("time", pl.col("temp").alias("value")).drop_nulls()
```

```python
import acquirium as aq
from tank_apps import TankAverage

acq = aq.init()
acq.check_app(TankAverage)    # run it once, save nothing
acq.deploy_app(TankAverage)   # keep it running on the server
```
