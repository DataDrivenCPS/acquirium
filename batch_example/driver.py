"""A deterministic synthetic building driver: no hardware or files required."""

from datetime import datetime, timezone
from math import sin

from acquirium import PollingIngestDriver


class SyntheticBuildingDriver(PollingIngestDriver):
    def setup(self) -> None:
        self.source_id = "batch-example-building"
        self.sample = 0
        self.declare(
            "zone_a_temperature_c",
            value_kind="numeric",
            point_uri="urn:batch-example:sensor:zone-a-temperature",
            label="Zone A temperature",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        )
        self.declare(
            "zone_b_temperature_c",
            value_kind="numeric",
            point_uri="urn:batch-example:sensor:zone-b-temperature",
            label="Zone B temperature",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        )

    def read(self) -> None:
        ts = datetime.now(timezone.utc)
        self.add("zone_a_temperature_c", 21.0 + 2.0 * sin(self.sample / 5), ts)
        self.add("zone_b_temperature_c", 23.0 + 1.5 * sin(self.sample / 7), ts)
        self.sample += 1
