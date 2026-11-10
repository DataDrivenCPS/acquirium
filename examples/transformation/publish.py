"""Generate a sine wave through the standard polling-driver lifecycle."""

import math

from acquirium import PollingIngestDriver


class SineWaveDriver(PollingIngestDriver):
    def setup(self):
        self.phase = 0.0
        self.declare(
            "signal", point_uri="urn:example:sine-wave", value_kind="numeric"
        )

    def read(self):
        self.add("signal", math.sin(self.phase))
        self.phase += math.pi / 4
