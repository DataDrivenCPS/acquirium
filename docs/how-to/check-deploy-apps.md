---
title: Check, deploy, and repair an app
---

In this article we'll describe how to read the result of a check, how to follow a deployed app, what to do when a check or a deployed app fails, how to apply a code change to old data, and how to remove an app.

The examples use the app `RORecovery` of [Calculate derived streams](calculate-derived-streams.md), saved as `ro_recovery.py`, and the plant of [5 - A Full Example](../tutorials/a-full-example.md).

## Read the result of a check

`check_app()` runs the app on all stored data and returns what it calculated.
Nothing is saved.
The result has one entry in `bindings` for each call of `transform()`:

```python
from pprint import pprint

import acquirium as aq
from ro_recovery import RORecovery

acq = aq.init()

result = acq.check_app(RORecovery, limit=2)
pprint(result["bindings"][0], sort_dicts=False, width=110)
```
```text
{'inputs': {'feed': [{'stream_uri': 'urn:acquirium#5d7ecdb5-062f-5af3-afbe-965da7bfc0a3',
                      'label': 'RO in flow mass water',
                      'unit': 'http://qudt.org/vocab/unit/KiloGM-PER-SEC'}],
            'permeate': [{'stream_uri': 'urn:acquirium#721679d0-45f5-5d08-8811-ddf25ffc7ce0',
                          'label': 'RO out flow mass water',
                          'unit': 'http://qudt.org/vocab/unit/KiloGM-PER-SEC'}]},
 'row': None,
 'outputs': {'recovery': {'stream': 'urn:acquirium#5cfb92d7-6ed5-5a39-a124-e54f6a9fe609',
                          'stream_name': 'ro-recovery',
                          'value_kind': 'numeric',
                          'rows': 8760,
                          'assigned': True,
                          'truncated': True,
                          'values': [{'time': '2025-01-01T00:00:00+00:00', 'value': 47.63232968002666},
                                     {'time': '2025-01-01T01:00:00+00:00', 'value': 48.358042017433824}]}},
 'error': None,
 'read_window': ['2025-01-01T00:00:00+00:00', '2025-12-31T23:59:59.999999+00:00'],
 'input_rows': {'feed': 8760, 'permeate': 8760}}
```

- `inputs` has the streams that the query matched, per alias. Check that these are the streams you expected.
- `row` is the match of this call. It is `None` for an app with `grouping = "all_matches"`.
- `input_rows` is the number of values that were read per alias.
- `outputs` has the calculated values per output. `rows` is the number of calculated values, and `values` has the first ones up to `limit`.
- `error` is `None` when the call succeeded.

## When a check fails

**An error in `transform()`.**
The error is in `error` of the binding, and `outputs` is empty.
Here the name of a column was misspelled in `ro_recovery.py`:

```python
result = acq.check_app(RORecovery, limit=2)
print(result["bindings"][0]["error"])
```
```text
ColumnNotFoundError: unable to find column "permeat"; valid columns: ["time", "feed", "permeate"]

Did you mean "permeate"?
```

Correct the file and check again.
The check always uses the current file.

**No bindings.**
When `result["bindings"]` is an empty list, the query of the app matched no stream.
Build the same query with `acq.query()` to find out why (see [Debug an empty query](debug-an-empty-query.md)).
Note that `acq.query()` accepts a prefix such as `wbs:RO`, but `build_query()` needs the full URI.

**No data.**
The error "no stored data for these inputs" means that the query matched, but the matched streams have no values.

**A class in the script itself.**
The app has to be in a file of its own that the server can import.
For a class that is defined in the script you run, `check_app()` and `deploy_app()` raise an error:

```text
ValueError: an app class defined in a script's __main__ module cannot be deployed; move it into an importable module the server can load
```

## Deploy and follow the app

```python
acq.deploy_app(RORecovery)
```
```text
{'ok': True, 'name': 'ro-recovery', 'status': 'deployed'}
```

The server works in the background.
The loop below prints the status every five seconds until the app has caught up:

```python
import time

while True:
    status = acq.list_apps()["apps"][0]["status"]
    print(status, end=" ")
    if status == "idle":
        break
    time.sleep(5)
```
```text
pending reprocessing reprocessing running reprocessing reprocessing idle
```

- `pending`: there is new data that the app has not looked at yet.
- `running`: a call of `transform()` is in progress.
- `reprocessing`: the app works through a long time range piece by piece. Here it is the year of stored data, because the app has `backfill = True`.
- `idle`: the app is up to date.
- `failed`: the app could not be loaded, or the last call of `transform()` raised an error.

The [reference](../reference/apps/deploying.md#status-values) lists all status values.

When the server cannot accept an app, `deploy_app()` raises an `HTTPError` and the message has the reason.
For instance, an app with `grouping = "per_match"` and a named output whose query matches two streams:

```text
HTTPError: 400 Client Error: Bad Request for url: http://127.0.0.1:49315/apps/pump-power-copy; response body: app 'pump-power-copy' declares named output(s) ['copy'] alongside per-row fan-out over 2 input groups; compute the aggregate in a second app whose query selects this app's derived streams
```

A rejected deployment changes nothing.
An app that was deployed before under the same name keeps running with its old definition.

## When a deployed app fails

`deploy_app()` does not call `transform()`, so a mistake in the calculation shows up later, when the server runs the app.
Here `ro_recovery.py` was deployed with the misspelled column name, and new values were inserted into both inputs:

```python
from datetime import datetime, timezone

t = datetime(2026, 1, 1, tzinfo=timezone.utc)
acq.insert_timeseries("csv-data", "RO-in-flow-mass-water", [(t, 300.0)])
acq.insert_timeseries("csv-data", "RO-out-flow-mass-water", [(t, 150.0)])
```

A few seconds later the app is `failed`.
`inspect_app()` has the error of each binding:

```python
print(acq.list_apps()["apps"][0]["status"])

binding = acq.inspect_app("ro-recovery")["app"]["bindings"][0]
print(binding["error"])
```
```text
failed
ColumnNotFoundError: unable to find column "permeat"; valid columns: ["time", "feed", "permeate"]

Did you mean "permeate"?
```

The values that were calculated before are not touched.
The new hour is missing from the output:

```python
q = acq.query().measurement().where(app="ro-recovery")
q.dataframe(start=datetime(2025, 12, 31, 23, tzinfo=timezone.utc))
```
```text
shape: (1, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__RO recovery │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2025-12-31 23:00:00 UTC ┆ 47.972396         │
└─────────────────────────┴───────────────────┘
```

To repair the app, correct the file and deploy it again.
The server then calculates what it missed:

```python
acq.deploy_app(RORecovery)
```
```text
{'ok': True, 'name': 'ro-recovery', 'status': 'deployed'}
```

```python
print(acq.list_apps()["apps"][0]["status"])
print(q.dataframe(start=datetime(2025, 12, 31, 23, tzinfo=timezone.utc)))
```
```text
idle
shape: (2, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__RO recovery │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2025-12-31 23:00:00 UTC ┆ 47.972396         │
│ 2026-01-01 00:00:00 UTC ┆ 50.0              │
└─────────────────────────┴───────────────────┘
```

**Note:** The server remembers the file an app was deployed with. When the file is changed and the app is not deployed again, the app becomes `failed` the next time the server loads it, with the error "entrypoint digest mismatch". Deploy the app again after every change of its file.

## Apply a code change to old data

After a change of the code, deploying the app again uses the new code for the values that arrive from then on.
The values that are already calculated stay as they are.

For instance, change the last lines of `transform()` in `ro_recovery.py` so that the recovery is rounded to one decimal, and deploy the app again:

```python
        output["recovery"] = df.select(
            "time", (100 * pl.col("permeate") / pl.col("feed")).round(1).alias("value")
        )
```

```python
acq.deploy_app(RORecovery)
q.dataframe(limit=3)
```
```text
shape: (3, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__RO recovery │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 47.63233          │
│ 2025-01-01 01:00:00 UTC ┆ 48.358042         │
│ 2025-01-01 02:00:00 UTC ┆ 48.642848         │
└─────────────────────────┴───────────────────┘
```

`reprocess_app()` calculates a time range again with the current code:

```python
acq.reprocess_app(
    "ro-recovery",
    datetime(2025, 1, 1, tzinfo=timezone.utc),
    datetime(2026, 1, 2, tzinfo=timezone.utc),
)
```
```text
{'ok': True, 'name': 'ro-recovery', 'status': 'reprocessing', 'bindings': 1}
```

The status of the app is `reprocessing` until the time range is done.
After that the old values are rounded too:

```python
q.dataframe(limit=3)
```
```text
shape: (3, 2)
┌─────────────────────────┬───────────────────┐
│ time                    ┆ data__RO recovery │
│ ---                     ┆ ---               │
│ datetime[μs, UTC]       ┆ f64               │
╞═════════════════════════╪═══════════════════╡
│ 2025-01-01 00:00:00 UTC ┆ 47.6              │
│ 2025-01-01 01:00:00 UTC ┆ 48.4              │
│ 2025-01-01 02:00:00 UTC ┆ 48.6              │
└─────────────────────────┴───────────────────┘
```

A second `reprocess_app()` for the same app is rejected while the first one is in progress, with the message "binding already has pending work; wait for it to finish".

**Note:** Be aware that the server checks the whole file of an app. When one file holds several deployed apps, a change of the file makes all of them `failed`, and deploying one of them again is rejected as long as the others are still deployed with the old file. Keeping each app in its own file avoids this.

## Remove an app

`remove_app()` stops an app:

```python
acq.remove_app("ro-recovery")
```
```text
{'ok': True, 'name': 'ro-recovery', 'status': 'removed'}
```

The server forgets the deployment and what the app had processed.
The values that were calculated stay in the store, and can still be read by `source_id` and `stream_name`:

```python
acq.stream("derived:ro-recovery", "ro-recovery").dataframe(limit=2)
```
```text
shape: (2, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2025-01-01 00:00:00 UTC ┆ 47.6  │
│ 2025-01-01 01:00:00 UTC ┆ 48.4  │
└─────────────────────────┴───────┘
```

A few seconds after the removal the output streams are no longer matched by queries:

```python
acq.query().measurement().where(app="ro-recovery").metadata()
```
```text
shape: (0, 1)
┌──────┐
│ data │
│ ---  │
│ str  │
╞══════╡
└──────┘
```

**Note:** A query object keeps the matches of its first run. Build the query again after removing an app.

When the app is deployed again later, it writes to the same stream.
With `backfill = True` it calculates the stored data from the start again.

## The same steps from the command line

With a server that was started by `acquirium server`, an app can be checked and followed from a terminal.
Run the commands in the folder of the `acquirium.toml` of the server, so that they find its address.

```bash
acquirium app check ro_recovery.py:RORecovery -n 2
```
```text
ro-recovery: 1 input group(s) matched

[1] inputs
      feed: 1 stream(s), 8760 rows read
        - RO in flow mass water
      permeate: 1 stream(s), 8760 rows read
        - RO out flow mass water
    output 'recovery' -> ro-recovery (numeric, 8760 rows)
        2025-01-01T00:00:00+00:00  47.63232968002666
        2025-01-01T01:00:00+00:00  48.358042017433824
        … 8758 more row(s); pass -n 0 for all of them
```

With `--local` the command runs `transform()` in the terminal and reads the data from the server.
This is useful for debugging, since a `breakpoint()` in `transform()` then opens in the terminal.

`acquirium app list` shows the deployed apps, and `acquirium app inspect` the details of one:

```bash
acquirium app list
```
```text
ro-recovery  idle  grouping=all_matches  bindings=1
```

```bash
acquirium app inspect ro-recovery
```
```text
ro-recovery: idle
Entrypoint: ro_recovery:RORecovery
Grouping: all_matches
Plan: graph revision 16, current=True
every: 3600s
lookback: 0s
lookahead: 0s
batch_delay: 0s
min_interval: disabled
backfill: True
parameters: {}
Declared outputs:
  recovery: numeric, unit=http://qudt.org/vocab/unit/PERCENT
    schema: time: timestamp[us, tz=UTC] (non-null), value: float64 (non-null)
    metadata: {"data_source": null, "label": "RO recovery", "medium": null, "properties": null, "property_uri": null, "quantity_kind": null, "stream_name": "ro-recovery", "substance": null, "unit": "http://qudt.org/vocab/unit/PERCENT", "value_kind": "numeric"}
Bindings: 1
  5b890601cf4f98b4bf71f200352b0cfc000a07838ab962ba98caa75056a407af: idle
    inputs: {"feed": ["urn:acquirium#5d7ecdb5-062f-5af3-afbe-965da7bfc0a3"], "permeate": ["urn:acquirium#721679d0-45f5-5d08-8811-ddf25ffc7ce0"]}
    outputs: {"recovery": "urn:acquirium#5cfb92d7-6ed5-5a39-a124-e54f6a9fe609"}
    revision: 1101 / 1101; last success: 2026-11-18T07:01:57.020389+00:00
```

`revision: 1101 / 1101` is how far the app has processed and the latest revision of the stored data.
Equal numbers mean the app is up to date.
There is no command to deploy or remove an app. Use `deploy_app()` and `remove_app()`, or the `[[apps]]` table of `acquirium.toml` (see the [reference](../reference/apps/deploying.md#the-apps-table)).

## Next

- [How apps work](../explanation/apps.md) describes how the server keeps the outputs up to date and what is stored.
- The [app reference](../reference/apps/deploying.md) has every option of the methods used on this page.
- The [command line reference](../reference/apps/cli.md) has the options of the `acquirium app` commands.
