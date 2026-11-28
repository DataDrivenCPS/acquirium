# Temperature summaries by tank

```sh
uv run --locked python -m examples.soft_sensors.run
```

Run from the repository root. The script starts a local server and computes a
ten-second mean for each tank, finds the hottest tank in the plant, and raises
an alarm above 27 °C. It then adds a late reading, corrects a value, moves a probe
between tanks, and deletes a reading.

Read [metadata.ttl](metadata.ttl) for the tank membership and
[declarations.py](declarations.py) for the selections. Each output is attached to
its tank or plant, so the next app finds it through the graph. Moving a probe
recalculates both tanks' history using its current membership.

The means weight observations equally. The alarm stores the hottest temperature
while it is above the limit; no row means clear. Each stage reads one published
snapshot. Pass `--data-dir /path/to/demo-data` to retain the history.

For a guided introduction, start with [the sine-wave driver and threshold](../transformation/README.md),
then [the turbidity alarm](../turbidity/README.md). The
[calculation guide](../../docs/how-to/write-soft-sensors.md) explains how
to write your own function.
