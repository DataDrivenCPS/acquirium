"""Self-driving WaterTAP simulation driver.

Inherits the built-in :class:`WaterTAPDriver` and turns it into an autonomous
simulator: on every tick it calls the model's ``generate_new_values`` to
synthesize a fresh set of feed inputs, applies them via ``change_inputs``,
solves the flowsheet, and ingests the mapped outputs into Acquirium. No
external input source (file, GUI, broker) is required — it just runs.

This is the live-streaming counterpart of ``scripts/data-generator.py``: same
``build / change_inputs / solve`` model contract and the same
``generate_new_values(ts, rng)`` driver, but pushing to Acquirium each tick
instead of writing parquet.

Extra config keys (in addition to everything ``WaterTAPDriver`` needs):

  - ``watertap_generate_spec``: ``path/to/generate-values.py:generate_new_values``
  - ``watertap_seed``: RNG seed for reproducible noise (default ``42``)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np

from acquirium.Drivers.BuiltInDrivers.watertap import (
    WaterTAPDriver,
    _load_callable,
    _require_config,
)

logger = logging.getLogger("acquirium.watertap.simulation")


class SimulationDriver(WaterTAPDriver):
    """WaterTAP driver that generates its own inputs each tick and solves them."""

    def setup(self) -> None:
        super().setup()
        cfg = self.config.get("driver", {})
        gen_spec = str(
            _require_config(cfg.get("watertap_generate_spec"), "watertap_generate_spec")
        )
        self._generate_fn = _load_callable(gen_spec)
        self._rng = np.random.RandomState(int(cfg.get("watertap_seed", 42)))
        if self._change_inputs_fn is None:
            raise ValueError(
                "SimulationDriver requires watertap_change_inputs_spec so generated "
                "values can be applied to the model"
            )
        logger.info("simulation driver ready: generating inputs via %s", gen_spec)

    def collect(self):
        # Synthesize this tick's inputs from the model's own generator, then let
        # WaterTAPDriver.collect() run build -> change_inputs -> solve -> read.
        ts = datetime.now(timezone.utc)
        self._inputs = self._generate_fn(ts, self._rng)
        logger.debug("simulation driver: generated inputs %s", self._inputs)
        return super().collect()
