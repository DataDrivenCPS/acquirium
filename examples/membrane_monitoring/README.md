# Stateful membrane monitoring

Run the complete example from the repository root:

```sh
uv run --locked python -m examples.membrane_monitoring.run
```

It starts a local Acquirium server, registers flux, transmembrane pressure and
permeate conductivity for two membrane skids, and deploys three apps whose
results build on the results before them:

```text
conductivity ──> smoothed conductivity     exponentially weighted mean
conductivity ──> conductivity drift        CUSUM statistic and alarm
flux, TMP ─────> membrane permeability     recursive least squares, hourly
```

Each function is declared with `sequential=True`. Escapement evaluates its
windows in event-time order and passes each call the latest earlier window
that wrote outputs as `context["previous"]`. Those outputs are the function's
state, so each function writes what it carries forward: the smoothed value,
the CUSUM statistic, and the permeability estimate with its variance. Nothing
else is kept between calls, so the results are the same however late the
readings arrive, and they survive restarts like any derived stream.

Read these files in order:

- [metadata.ttl](metadata.ttl): the skids and the class of each point.
- [declarations.py](declarations.py): registration, one selection per
  measurement class, and per-skid parameters: the smoothing time constant, the
  drift detector's target, allowance and limit, and the fit's forgetting factor.
- [functions.py](functions.py): the three sequential functions.
- [run.py](run.py): simulated readings, a late spike, its deletion, and a late
  flux reading.

The simulated skid 1 fouls: its permeability falls from 3.0 by
0.05 L/m²/h/bar per hour, and its permeate conductivity rises by
8 µS/cm from 08:00. The fit follows the decline about four hours behind, as a
forgetting factor of 0.8 per hourly window implies. The drift alarm raises
about half an hour after the rise. Skid 2 holds steady.

Each late reading repairs the results after it, in order, until they stop
changing:

- **The spike** raises skid 2's drift statistic, which falls back to zero
  within about two and a half hours. The repair stops at the first unchanged
  statistic. The smoothed conductivity changes at every later reading, since
  an exponentially weighted mean only fades the spike.
- **Deleting the spike** restores every earlier result.
- **The late flux reading** changes its hour's permeability and every later
  estimate.

A sequential Python function runs once per window and cannot be batched, so a
repair costs about a millisecond per window it reaches. A filter that forgets
slowly repairs to the end of its history: give it a coarser window, as the
permeability fit's hourly buckets do, or restart it each period with
`escapement.same_period`. See [the apps reference](../../docs/reference/apps.md)
for the declaration and the context it receives.
