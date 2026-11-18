---
title: How apps work
---

In this article we'll describe what the server does with an app: how it runs the app, how the outputs are kept up to date, and what is stored.
[Declaring and deploying Apps](../tutorials/advanced/first-app.md) shows how to write and deploy one.

## What the server does with an app

An app is deployed with `deploy_app()`.
The server does not receive the code of the app.
It receives the name of the module and the class, the parameters and the settings of the app, and it imports the module itself.
This is why the file of the app has to be in a folder the server can import from.

The server then runs `build_query()` on the plant model.
The matches of the query decide how the app is called:

- With `grouping = "per_match"` each match is calculated on its own. Each match has its own input stream and its own output streams.
- With `grouping = "all_matches"` all matches are calculated together, and there is one set of output streams.

The server calls each of these a binding.
The `binding_count` of `list_apps()` is the number of bindings of an app.

The query is run again when the plant model changes.
For instance, when a third pump with a power stream is added to the model, a `per_match` app over the pump power streams gets a third binding and a third output stream.

## How the outputs are kept up to date

Every write to the stored data gets a number, the revision.
For each binding the server remembers the revision it has processed.
A few times per second the server compares the two.
When an input stream has values that were written after the processed revision, the server does the following:

- It finds the time range of the values that changed.
- It reads the inputs for this time range. The settings `every`, `lookback` and `lookahead` make the range wider, see [Time windows of a call](../reference/apps/app-class.md#time-windows-of-a-call).
- It calls `transform()` with these inputs.
- It replaces the stored output values of this time range with the result, and saves the new revision of the binding. Both are saved together.

New values, late values and corrected values all take this path.
For instance, an app has `lookback = "10m"` for a ten-minute average, and a reading of 12:00 is corrected.
The correction changes the averages from 12:00 to 12:10, and these averages need the readings from 11:50 on.
The server reads the inputs from 11:50 to 12:10 and replaces the outputs from 12:00 to 12:10.

The result of `transform()` replaces what was stored in the time range:

- A value that was stored and is not in the new result is removed.
- An empty table removes all values of the output in the time range.
- An output that `transform()` does not assign is left as it is.

The calculation uses the values that are stored at that moment.
The server does not wait until all inputs have reported.
When a late value arrives, the same time range is calculated again.

**Note:** When a stream is written again with `replace=True`, the apps that read it calculate all of their outputs again.

### Data that was stored before the deployment

With `backfill = True` the first run of an app also processes the data that was already stored.
A long time range is processed in pieces of about one day, and the server remembers which pieces are done.
With `backfill = False` the app only processes values that are written after the deployment.

### Apps that read other apps

The outputs of an app are streams, so another app can select them, for instance with `measurement(app="ro-recovery")`.
The server runs an app before the apps that read its outputs.
A deployment is rejected when two apps would write the same stream, or when apps read the outputs of each other in a circle.

## When a calculation fails

When `transform()` raises an error, nothing is saved for this binding.
The stored outputs and the processed revision stay as they were, and the status of the binding becomes `failed`.
The server tries the binding again in its next round.
The other apps keep running.
An app that reads the outputs of a failed binding waits until that binding succeeds.

Be aware that the server can call `transform()` more than once for the same data, for instance after a failure or a restart.
`transform()` should only calculate.
It should not write files or send messages.

## What is stored

- **The output values.** They are in the same store as all other streams, under the `source_id` `derived:<app name>`. Only the current values are kept. Earlier results are overwritten.
- **The description of the output streams.** The label, the unit and the name of the app are written to the graph. This is what `where(app=...)` uses.
- **The deployment.** The module and class name, the parameters and the settings. With this the server starts the app again after a restart.
- **The progress.** The processed revision of each binding, and the unfinished pieces of a long time range. With this the server continues after a restart where it stopped.

The code of the app is not stored.
The server keeps a fingerprint of the file of the app and compares it each time it loads the app.
When the file was changed after the deployment, the app stops with a `digest mismatch` error until it is deployed again.

Deploying an app again keeps the progress.
The new code is used for the values that are written from then on, and the stored outputs are not calculated again.
`reprocess_app()` calculates a time range again with the new code.

`remove_app()` deletes the deployment and the progress.
The output values stay in the store.

## Next

- [Calculate derived streams](../how-to/calculate-derived-streams.md) has apps with several inputs, parameters and a rolling window.
- [Check, deploy, and repair an app](../how-to/check-deploy-apps.md) shows what to do when a check or a deployment fails.
- [Materialization internals](materialization-internals.md) describes the tables, the scheduling and the storage backends.
- The [app reference](../reference/apps/_index.md) lists every setting.
