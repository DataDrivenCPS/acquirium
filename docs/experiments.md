# Bounded experiments

Experiments are immutable, bounded runs. Their definition, graph revision,
binding snapshot, input versions, requested interval, parameters schema,
parameters, metadata, and optional state revision are recorded before code runs.

```python
import acquirium as aq


class LoadShift(aq.Experiment):
    parameters_schema = {
        "type": "object",
        "required": ["max_shift_kw"],
        "properties": {"max_shift_kw": {"type": "number", "exclusiveMinimum": 0}},
    }

    def run(self, context):
        schedule_ref = context.output_ref("schedule")
        schedule, total_cost = solve_load_shift(context.params["max_shift_kw"])
        write_schedule(schedule_ref, schedule)
        context.metric("total_cost", total_cost)
        return {"schedule": schedule_ref}
```

Start the run with a unique ID and a frozen snapshot. `execute_experiment()` uses
the shared bounded executor, marks the row succeeded or failed, and retains
metrics independently of run output collection. Use `keep_experiment()` for a
selected scenario; unkept runs can be collected while retaining their small
provenance tombstone for comparisons and reruns.

Each output identity is scoped by the run ID, so two scenarios cannot overwrite
one another.
