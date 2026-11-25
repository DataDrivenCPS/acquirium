"""Declaring and deploying Apps

The code of the tutorial page "Declaring and deploying Apps".
Run 5_a_full_example first, in the same folder. The app itself is in plant_apps.py.
"""

import time
import acquirium as aq
from plant_apps import PumpPowerKW

acq = aq.init()

# 2. Check it
#
# Runs the app on the stored data. Nothing is saved.

result = acq.check_app(PumpPowerKW, limit=2)

for binding in result["bindings"]:
    print(binding["row"]["pump"], binding["error"])
    print(binding["outputs"]["power"]["values"])

# 3. Deploy it

print(acq.deploy_app(PumpPowerKW))

# Wait until the app has caught up with the data.

while acq.list_apps()["apps"][0]["status"] != "idle":
    time.sleep(1)

print(acq.list_apps()["apps"][0])

# 4. Query the result

q = acq.query().measurement().where(app="pump-power-kw")

print(q.dataframe(limit=3))

# Insert a new reading into one of the inputs, and give the app a few seconds.

acq.insert_timeseries("csv-data", "P1-mechanical-power", [
    ("2026-01-01", 1_200_000.0),
])
time.sleep(8)

print(q.dataframe(start="2026-01-01"))

# Changing and removing an app
#
# Deploy again after a change of the code, and apply it to the old data.

acq.deploy_app(PumpPowerKW)

print(acq.reprocess_app(
    "pump-power-kw",
    "2025-01-01",
    "2026-01-02",
))

# Remove the app.

print(acq.remove_app("pump-power-kw"))

# Stop Acquirium.

aq.shutdown()
