"""Build back the model the latest run of record_model.py saved.

Run from this directory, after record_model.py, with:

    uv run python load_model.py
"""
from pathlib import Path

import acquirium as aq

HERE = Path(__file__).resolve().parent

ac = aq.init(data_dir=HERE / ".data")
study = ac.study.get("flexpse-model-state")
model_state = study.variables["model"]

# load() imports the declared codec, flexpse_codec:FlexModelCodec, by name.
# It runs here, in this process; the server only stores the bytes.
observation = model_state.observations().latest()
model = observation.load()

print(f"experiment: {observation.experiment_id}")
print(f"model {model.name!r} with {len(model.time_block.time_index)} time steps")
print(f"pump status fixed at the first step: {model.site.pump.status[0].fixed}")

aq.shutdown()
