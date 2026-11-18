import random

import acquirium as aq


class TankDriver(aq.PollingIngestDriver):
    def setup(self):
        self.source_id = "tank"
        self.declare("level", unit="m", label="Tank level")
        self.declare("state", value_kind="text")

    def read(self):
        level = 2.0 + random.random()        # ask your sensor here
        self.add("level", level)
        self.add("state", "high" if level > 2.8 else "normal")
