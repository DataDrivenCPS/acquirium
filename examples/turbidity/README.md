# Turbidity alarm pipeline

Run from the repository root.

```sh
uv run --locked python -m examples.turbidity.run
```

The [tutorial](../../docs/tutorials/turbidity.md) explains the two apps and
shows the output before and after late readings arrive.

- [calculations.py](calculations.py) implements the cleaning and alarm functions.
- [run.py](run.py) starts a temporary server, declares and deploys both apps, then loads the sample data and compares the alarm snapshots.
