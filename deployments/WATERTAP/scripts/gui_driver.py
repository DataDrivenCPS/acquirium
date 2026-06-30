"""GUI-driven WaterTAP driver.

Inherits the built-in :class:`WaterTAPDriver`. On setup it launches the generic
Streamlit GUI (``input_gui.py``, alongside this file) pointed at a model folder.
The GUI introspects that model's input variables and lets a user edit them,
writing the current values to a shared JSON file. Each tick this driver reads
that file and, **only when the inputs have changed**, applies them via
``change_inputs``, solves the flowsheet, and ingests the mapped outputs.

Driver and GUI are decoupled through the shared file, so the GUI never imports
the flowsheet or talks to Acquirium directly — the driver owns the solve +
ingest. The GUI is model-agnostic: it works for any model folder.

Extra config keys (in addition to everything ``WaterTAPDriver`` needs):

  - ``watertap_model_dir``: model folder the GUI introspects for its inputs
    (default: the folder containing ``watertap_build_spec``'s file)
  - ``watertap_inputs_path``: shared JSON file the GUI writes / the driver reads
    (relative to this config's directory; default ``.gui_inputs.json``)
  - ``gui_script_path``: Streamlit app to launch (default ``input_gui.py``
    next to this driver)
  - ``gui_port``: Streamlit server port (default ``8501``)
  - ``gui_autostart``: launch the GUI subprocess on setup (default ``true``)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import polars as pl

from acquirium.BuiltinDrivers.watertap import WaterTAPDriver

logger = logging.getLogger("acquirium.watertap.gui")

_EMPTY_SCHEMA = {
    "ts": pl.Datetime("us", "UTC"),
    "ref_name": pl.Utf8,
    "value": pl.Object,
}

_HERE = Path(__file__).resolve().parent


class GuiDriver(WaterTAPDriver):
    """WaterTAP driver fed by a generic Streamlit GUI through a shared inputs file."""

    def setup(self) -> None:
        super().setup()
        cfg = self.config.get("driver", {})
        cfg_dir = self.config_dir()

        inputs_path = Path(cfg.get("watertap_inputs_path", ".gui_inputs.json"))
        self._inputs_path = inputs_path if inputs_path.is_absolute() else (cfg_dir / inputs_path)

        gui_script = Path(cfg.get("gui_script_path", _HERE / "input_gui.py"))
        self._gui_script = gui_script if gui_script.is_absolute() else (cfg_dir / gui_script)

        self._model_dir = self._resolve_model_dir(cfg)
        self._gui_port = int(cfg.get("gui_port", 8501))
        self._gui_autostart = bool(cfg.get("gui_autostart", True))
        self._gui_proc: subprocess.Popen | None = None

        if self._change_inputs_fn is None:
            raise ValueError(
                "GuiDriver requires watertap_change_inputs_spec so GUI values can "
                "be applied to the model"
            )

        # Only solve when the file content actually changes between ticks.
        self._last_inputs_raw: str | None = None

        if self._gui_autostart:
            self._launch_gui()

    def _resolve_model_dir(self, cfg: dict) -> Path:
        """Model folder the GUI introspects — explicit, or derived from build_spec."""
        explicit = cfg.get("watertap_model_dir")
        if explicit:
            p = Path(explicit)
            return p if p.is_absolute() else (Path.cwd() / p).resolve()
        # Fall back to the directory of the build spec's file (e.g.
        # ".../models/seawater-ro/build-and-solve.py:build" -> ".../seawater-ro").
        build_path = self._build_spec.split(":", 1)[0]
        return Path(build_path).expanduser().resolve().parent

    def _launch_gui(self) -> None:
        if not self._gui_script.exists():
            logger.warning("gui_driver: GUI script not found at %s; not launching", self._gui_script)
            return
        env = {
            **os.environ,
            "ACQ_GUI_INPUTS_PATH": str(self._inputs_path),
            "ACQ_GUI_MODEL_DIR": str(self._model_dir),
        }
        try:
            self._gui_proc = subprocess.Popen(
                [
                    sys.executable, "-m", "streamlit", "run", str(self._gui_script),
                    "--server.port", str(self._gui_port),
                    "--server.headless", "true",
                ],
                env=env,
            )
            logger.info(
                "gui_driver: launched Streamlit GUI for %s on port %d (writes %s)",
                self._model_dir.name, self._gui_port, self._inputs_path,
            )
        except Exception:
            logger.exception("gui_driver: failed to launch Streamlit GUI")

    def collect(self) -> pl.DataFrame:
        if not self._inputs_path.exists():
            return pl.DataFrame(schema=_EMPTY_SCHEMA)

        raw = self._inputs_path.read_text()
        if raw == self._last_inputs_raw:
            return pl.DataFrame(schema=_EMPTY_SCHEMA)  # unchanged → nothing to solve

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("gui_driver: inputs file not valid JSON yet; skipping")
            return pl.DataFrame(schema=_EMPTY_SCHEMA)

        self._last_inputs_raw = raw
        # Drop any GUI-only bookkeeping keys (prefixed with "_").
        self._inputs = {k: v for k, v in data.items() if not str(k).startswith("_")}
        logger.info("gui_driver: inputs changed -> solving %s", self._inputs)
        return super().collect()

    def stop(self) -> None:
        if self._gui_proc is not None and self._gui_proc.poll() is None:
            logger.info("gui_driver: terminating Streamlit GUI")
            self._gui_proc.terminate()
            try:
                self._gui_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._gui_proc.kill()
