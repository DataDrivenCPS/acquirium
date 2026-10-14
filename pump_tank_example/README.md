# Pump/tank experiment example

The incomplete `load_shifting_experiment.py` sketch has been retired. Use the
[runnable FlexPSE example](../examples/flexpse/README.md) to record optimization
inputs, results, time series, and solver events with Acquirium.

That example includes pinned model and solver dependencies, JSON inputs, and
an Acquirium environment. From the repository root:

```bash
cd examples/flexpse
uv sync
uv run python record_experiment.py
```

For a guided notebook that records and compares several scenarios, run from
the same directory:

```bash
uv run marimo edit ../../docs/tutorials/first-experiment.py
```

Both use a local Acquirium runtime. The script starts and stops its server;
the example's README describes the stored results and notebook workflow.
