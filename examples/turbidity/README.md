# Follow a turbidity alarm

```sh
uv run --locked python -m examples.turbidity.run
```

Run from the repository root. A temporary server processes twenty-five minutes
of readings from four locations. Filter 3 has a short gap during a high-turbidity
interval. Interpolation fills the gap, and a sustained alarm fires. Late readings
then break the high interval, clearing the alarm and the plant count. An earlier
snapshot preserves the original result.

The script checks each of those results and shuts down. It reuses the
[bulk-ingest calculations](../bulk_ingest/chain.py), without the CSV driver or
benchmark output. Follow the [tutorial](../../docs/tutorials/turbidity.md) for
the story one step at a time.
