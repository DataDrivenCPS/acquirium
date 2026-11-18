"""Running experiments

The code of the tutorial page "Running experiments".
Run 5_a_full_example first, in the same folder. Every run of this script adds new runs to the study.
"""

from datetime import datetime, timezone

import acquirium as aq

acq = aq.init()

# 1. Create a study and declare variables

study = acq.study.define("pump-speed")

speed = study.input("speed").scalar(unit="Hz")
power = study.output("power").scalar(unit="kW")
predicted = study.output("predicted power").timeseries(
    observed="urn:swro/P1-mechanical-power",
    unit="W",
)

# 2. Record runs

for hz in [40, 45, 50]:
    run = study.start({"operator": "ana", "pump": "P1"})

    speed.record(hz)
    result = round(1100 * (hz / 50) ** 3, 1)     # your calculation or test goes here
    power.record(result)

    run.finish()

print(study.experiments.frame().select("experiment_id", "status", "started_at"))

# 3. Read the results back

print(study.experiments.scalars("speed", "power", metadata=["operator"]))

# Filter the runs:

print(study.experiments.where(status="succeeded", operator="ana").scalars())

# One variable across the runs:

print(power.observations().frame().select("experiment_id", "value"))

# 4. Connect an experiment to the plant
#
# Plant data as an input of a run:

p1_power = acq.stream("csv-data", "P1-mechanical-power")

run = study.start({"operator": "ana", "pump": "P1"})

measured = run.annotate_stream(
    p1_power.stream_uri,
    start=datetime(2025, 7, 1, tzinfo=timezone.utc),
    end=datetime(2025, 7, 1, 3, tzinfo=timezone.utc),
)
df = measured.dataframe()

print(df)

# A scalar and a timeseries result:

power.record(df["value"].mean() / 1000)
predicted.record([(t, v * 0.98) for t, v in zip(df["time"], df["value"])])

run.finish()

print(run.observations(predicted).latest().dataframe())

# Stop Acquirium.

aq.shutdown()
