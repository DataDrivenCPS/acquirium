---
title: Soft sensor actors
---

An Acquirium **app** updates a calculation as its inputs change. An
**actor** gives you control over when to run a calculation and whether to
publish a result.

For example, an app can estimate how much water passes through a membrane for
every pressure reading by invoking a model. Training the model takes a long
time (well, long enough that you don't want to retrain on *every sensor
reading*). Here, using an actor gives you some options:
- re-fit the model once an hour
- check recent prediction errors and retrain only when they exceed a threshold

Actors "react" to events such as timer ticks or changes to an input query's
results. A timer might trigger a turbidity check. A probe replacement (seen in
the graph) might prompt an actor to reset its baseline.

## Standalone turbidity alarm

On each timer tick, the actor reads the last 15 minutes of turbidity
readings and publishes an alarm if their average exceeds 0.5 NTU.
Acquirium calls `on_timer` with the scheduled time and a snapshot of the
published readings. A check scheduled for 10:30 reads from 10:15 up to 10:30.

```{literalinclude} ../../examples/actors/turbidity_alarm.py
:language: python
:end-before: alarm = aq.Actor(
```

The `Transition` tells Acquirium what to save. When the average is above
the limit, the actor writes a `1` at the scheduled time. Otherwise it
writes nothing. The actor uses the current window alone, so its state is
`None`.

## Put the alarm on a schedule

In the same file, create an `aq.Actor` with the handler class, its input
stream, its output, and the time between checks. The input key
`"turbidity"` is the name the handler passes to `tick.snapshot.read()`.

```{literalinclude} ../../examples/actors/turbidity_alarm.py
:language: python
:start-at: alarm = aq.Actor(
```

In `acquirium.toml`, point `spec` at the Python file and the `alarm`
variable. The server loads that declaration at startup.

```toml
[[actors]]
spec = "turbidity_alarm.py:alarm"
```

The example checks every four seconds so the result appears quickly.
Each check looks back 15 minutes.

From the repository root, start the example server:

```sh
cd examples/actors
uv run --locked acquirium server
```

The sample driver makes up a turbidity reading of 0.8 NTU every second, so
the alarm fires on every check that has readings. Open a Python session
with `uv run --locked python` in a second terminal to read the alarm.

```python
import acquirium as aq

client = aq.Acquirium()
client.stream("actor:turbidity-alarm", "alarm").dataframe().tail(3)
```

## Fit a model on a timer and predict on each reading

The membrane model predicts flux, the rate of water flow per unit of
membrane area. It multiplies pressure by a coefficient called
*permeability*. At a pressure of 3, a coefficient of 2 predicts a flux
of 6. To estimate the coefficient, the actor pairs pressure and measured
flux by timestamp within the training window.

The actor keeps a count of successful fits in its state. `initial_state()`
sets the count to zero. Each successful fit returns the updated count and
the new coefficient together.

```{literalinclude} ../../examples/actors/membrane_fit.py
:language: python
:pyobject: MembraneFit
```

Acquirium saves the state as JSON with the output and restores it to
`self.state` before the next handler call, including after a restart.
That means the state has to be JSON: dicts, lists, strings, numbers,
booleans and `None`, with no NaN. A fitted model object won't fit there.
Publish its parameters as outputs, as this actor does, or save the model
to a file and keep the file's path in the state.

The actor timestamps each model it publishes, and that timestamp decides
which readings are run against the new model. Here the model is a single
coefficient, published to the `permeability` stream. The prediction app reads
it as an *as-of input*: it uses a model for every reading at or after its
timestamp, until a newer one arrives. The two-hour limit lets the app keep
using a recent model while the next fit runs. The same file declares the
actor and the app:

```{literalinclude} ../../examples/actors/membrane_fit.py
:language: python
:start-at: fit = aq.Actor(
```

The app reads the actor's output like any other stream, under the source
`actor:membrane-fit`.

This actor stamps a model when the fit finishes, so the new model only
applies to readings from then on. To also run the readings that arrived
during the fit against the new model, stamp it with the time the fit started:

```python
effective_at = tick.scheduled_at
```

For a fit that starts at 10:00 and finishes at 10:20, Acquirium then
recalculates the predictions for 10:00–10:20 with the new model.

On the example server, pressure stays at 3 and measured flux rises from
6 to 12 after 20 seconds. A training window that spans the change can
produce a coefficient between 2 and 4. Once a fit uses the new readings
alone, the app predicts 12 for new samples at pressure 3. Commands to
inspect the fits and predictions are in the
[example README](https://github.com/DataDrivenCPS/acquirium/tree/main/examples/actors).

### Check model error before retraining

Use an hourly check to decide whether retraining is worth the cost. An app
can compare predictions with measured flux and publish the difference to
a stream. Declare that stream as the actor's `flux_error` input.

After the first fit, the example check requires at least ten errors from
the last hour and a mean absolute error above 0.5 in flux units.

```python
# Add this check at the start of the training handler.
end = pd.Timestamp(tick.scheduled_at)
errors = tick.snapshot.read("flux_error", start=end - tick.interval, end=end)
values = errors["value"].drop_nulls()
if self.state["fits"] > 0 and (len(values) < 10 or values.abs().mean() <= 0.5):
    return aq.Transition(self.state)
```

An early return leaves the current coefficient in use. If the app sets a
maximum age, the actor also needs to refresh the coefficient before it
expires.

## Follow the probes assigned to a tank

Use a query when the streams an actor reads can change. For a tank whose
probes can be replaced during maintenance, the following input selects
the measurements currently assigned to the tank and includes their units.

```python
inputs = {
    "tank_probes": (
        aq.Query()
        .entity(uri="urn:plant:tank-a", alias="tank")
        .measurement(alias="probe", include_connection_points=False)
        .include("unit", of="probe")
    ),
}
```

In `on_timer`, `tick.snapshot.read("tank_probes")` returns the selected
readings with `time`, numeric `value`, and `text` columns. When several
probes match, `stream_uri` tells you which one produced each row. For a
time range, `start` is included and `end` is excluded. Call
`tick.snapshot.metadata("tank_probes")` to look up the units for those
same probes.

If a probe is swapped during a check, the handler keeps using the
assignments and readings it started with. The next check can use the
replacement. A deferred read with `dataframe()` uses the same snapshot.
This example collects the five minutes leading up to the scheduled check.

```python
import pandas as pd

end = pd.Timestamp(tick.scheduled_at)
query = tick.snapshot.query("tank_probes")
readings = query.dataframe(start=end - pd.Timedelta("5m"), end=end)
```

Use `snapshot.query()` with no names to read all declared inputs together.
The resulting `input` column identifies the declaration each row came from.

## React when a probe is replaced

Use `on_graph_change` to respond to a change between timer checks. For
example, reset the sample count for a baseline when a probe is added or
removed.

```python
def on_graph_change(self, change: aq.GraphChange) -> aq.Transition:
    probes = change.inputs.get("tank_probes")
    if probes and (probes.added_streams or probes.removed_streams):
        return aq.Transition({**self.state, "samples_since_swap": 0})
    return aq.Transition(self.state)
```

Acquirium calls this handler when a declared query's matched streams or
metadata change. To handle an edit to a probe's unit, for example, compare
`probes.old_metadata` and `probes.new_metadata`. Read `change.snapshot` if
you need the readings for the new assignment right away.

:::{dropdown} Scheduling and snapshots in more detail

- Each actor handles one message at a time. Ticks fall on multiples of the
  interval counted from the Unix epoch, so an hourly timer fires on the hour
  and a daily timer fires at midnight UTC, not local midnight. An actor that
  falls behind takes the latest due tick for each timer and skips the rest.
- When a query change and a timer are both due, the change handler runs
  before the timer uses the updated assignment. If several revisions arrive
  while the actor is busy, it compares the last observed assignment with
  the latest available one.
- Startup and restart establish the initial query assignments. After a
  restart, the actor schedules its next tick and resumes from saved state.
  A failed handler leaves the last saved state and output intact. The server
  logs the error and continues processing messages.
- For diagnostics, `snapshot.graph_revision` identifies the metadata view
  used for query inputs and `snapshot.epoch` identifies the published data.

Every class, method and message is listed in the [actor reference](../reference/actors.md).

:::
