"""Generic Streamlit GUI for editing a WaterTAP model's inputs.

Model-agnostic: point it at any model folder (one with the standard
``generate-values.py`` exposing ``generate_new_values(ts, rng)``) and it builds
one numeric field per input variable, seeded with a realistic default from the
model itself. Whenever the user changes a value it writes the full input dict to
a shared JSON file, which the companion :class:`GuiDriver` polls, solves, and
ingests.

Configured entirely through environment variables (set by ``GuiDriver``):

  - ``ACQ_GUI_MODEL_DIR``   model folder to introspect (required)
  - ``ACQ_GUI_INPUTS_PATH`` JSON file to write the inputs to (default
    ``.gui_inputs.json`` in the working directory)

Run standalone with::

    ACQ_GUI_MODEL_DIR=deployments/WATERTAP/models/seawater-ro \\
    ACQ_GUI_INPUTS_PATH=/tmp/inputs.json \\
    streamlit run deployments/WATERTAP/scripts/input_gui.py
"""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import streamlit as st

MODEL_DIR = Path(os.environ.get("ACQ_GUI_MODEL_DIR", "")).expanduser()
INPUTS_PATH = Path(os.environ.get("ACQ_GUI_INPUTS_PATH", ".gui_inputs.json")).expanduser()
# Fixed reference moment + seed so the seeded defaults are stable across reruns.
_DEFAULT_TS = datetime(2025, 1, 1, 12, 0, 0)


def _load_generate_fn(model_dir: Path):
    """Load ``generate_new_values`` from ``<model_dir>/generate-values.py``."""
    path = model_dir / "generate-values.py"
    if not path.exists():
        raise FileNotFoundError(f"no generate-values.py in {model_dir}")
    spec = importlib.util.spec_from_file_location(f"gui_genvalues_{model_dir.name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate_new_values


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


st.set_page_config(page_title="WaterTAP Inputs", page_icon="💧", layout="centered")
st.title("💧 WaterTAP — Model Inputs")

if not MODEL_DIR or not MODEL_DIR.is_dir():
    st.error(f"Set ACQ_GUI_MODEL_DIR to a model folder (got {os.environ.get('ACQ_GUI_MODEL_DIR')!r}).")
    st.stop()

try:
    generate_new_values = _load_generate_fn(MODEL_DIR)
    defaults = generate_new_values(_DEFAULT_TS, np.random.RandomState(0))
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not load inputs from {MODEL_DIR}: {exc}")
    st.stop()

st.caption(f"Model **{MODEL_DIR.name}** · {len(defaults)} input(s) · writing to `{INPUTS_PATH}`")
st.write("Adjust a value to trigger a solve on the driver's next tick.")

# One numeric field per model input variable, seeded with the model's default.
inputs: dict[str, float] = {}
for key, default in defaults.items():
    default = float(default)
    # A modest step derived from the magnitude keeps the control usable for any
    # variable (pressures ~1e5, flows ~0.3, concentrations ~35, temps ~298).
    step = max(abs(default) * 0.01, 1e-6)
    inputs[key] = st.number_input(key, value=default, step=step, format="%.6g")

_atomic_write(INPUTS_PATH, json.dumps(inputs, indent=2))
st.success("Inputs saved — the driver will solve these on its next tick.")
st.json(inputs)
