# Semantic soft sensors

Run the complete example from the repository root:

```sh
uv run --locked python -m examples.soft_sensors.run
```

It starts a local Acquirium server, registers three probes, and deploys:

```text
tank A probes ──> 10-second mean ──┐
                                 ├──> hottest tank ──> temperature alarm
tank B probe ───> 10-second mean ──┘
```

Acquirium resolves each tank's probes from RDF. `Output(on_group=True)` attaches
each derived point to its tank or plant with `s223:hasProperty`; downstream
selections discover those points using their producing app and group membership.
No derived stream IDs are hardcoded in the declarations.

Read these files in order:

- [metadata.ttl](metadata.ttl): tank/plant topology and probe membership.
- [declarations.py](declarations.py): registration, semantic selections, windows,
  output metadata, and frozen threshold/unit context.
- [transforms.py](transforms.py): self-contained pure Python/Polars calculations.
- [run.py](run.py): ordinary client ingestion, metadata edits, and snapshot reads.

The means weight observations equally, rather than weighting probes equally.
Input units resolve to frozen conversion factors; calculations convert to Celsius.
The hottest-tank and alarm functions are pointwise at each upstream bucket start.
The alarm emits the hottest value above 27 °C; no row means clear for that bucket.
An empty result retracts a previously raised alarm. This is a numeric example,
not a notification delivery service or a missing-data detector.

The script shows a late reading, a correction clearing an alarm, a probe moving
between tanks, and an explicit deletion. Each stage reads one committed epoch.
Metadata edits repair retained history. Buckets are provisional and immediately
updated; the example does not wait for wall-clock bucket completion.

`--data-dir /path/to/demo-data` retains data for restart. Each run has new request
IDs, resets the demo layout, and upserts the same demonstration timestamps.

Escapement owns subprocess execution and scheduling. Each affected operator leases
a warm Python child process for its timestamp/bucket groups. Processes retain a
digest-identified module across entrypoints and epochs; independent tank operators
can use separate replicas within the pool bound. Code is pinned
by digest, and the default interpreter is the server's interpreter. The example
uses the repository's locked environment and no adjacent helper modules or model
files. See the [authoring guide](../../docs/how-to/write-soft-sensors.md) for
deployment and failure recovery contracts.

This adapts Escapement's `examples/live` to a real Acquirium server, canonical
registered references, and Acquirium's semantic plan controller. It uses the sole
Escapement store; there is no second app scheduler or timeseries store.
