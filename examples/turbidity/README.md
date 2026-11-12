# Turbidity alarm pipeline

```sh
uv run --locked python -m examples.turbidity.run
```

Run from the repository root. A temporary server runs twenty-five minutes of
readings from four locations through the
[bulk-ingest turbidity apps](../bulk_ingest/chain.py). Filter 3 is high with a
one-minute gap; the gap is filled and a sustained alarm fires. Late readings
for the gap then clear the alarm, and the snapshot taken before them still
shows it. The [tutorial](../../docs/tutorials/turbidity.md) explains each step.
