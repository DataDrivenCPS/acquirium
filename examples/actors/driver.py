"""Publish synthetic filter and membrane readings once the server starts."""

import pandas as pd

from acquirium import PollingIngestDriver


class WaterTreatmentDriver(PollingIngestDriver):
    def setup(self):
        self.declare("turbidity", source_id="filter", value_kind="numeric")
        self.declare("pressure", source_id="membrane", value_kind="numeric")
        self.declare("flux", source_id="membrane", value_kind="numeric")

        now = pd.Timestamp.now(tz="UTC")
        self.permeability_change_at = now + pd.Timedelta("20s")

    def read(self):
        # We're making up all the readings in this example.
        now = pd.Timestamp.now(tz="UTC")
        self.add("turbidity", 0.8, now, source_id="filter")

        # Hold pressure at 3 and make up flux as pressure times permeability.
        # After 20 seconds, change permeability from 2 to 4, so flux goes from 6 to 12.
        # This lets us see the actor estimate the new permeability from the readings
        # and the prediction app use that estimate for later pressure samples.
        pressure = 3.0
        permeability = 2.0 if now < self.permeability_change_at else 4.0
        self.add("pressure", pressure, now, source_id="membrane")
        self.add("flux", pressure * permeability, now, source_id="membrane")
