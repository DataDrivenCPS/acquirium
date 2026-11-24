"""CSV driver for the synthetic plant: binds turbidity columns to model points."""

from pathlib import Path

from acquirium.Drivers.BuiltInDrivers.csv_ingest import CSVIngestDriver

MODEL = Path(__file__).resolve().parent / "model.ttl"
LOCATIONS = 4


class BulkIngestDriver(CSVIngestDriver):
    def setup(self) -> None:
        super().setup()
        self.insert_graph_file(MODEL)
        # Declare the modelled streams before any file arrives, so apps can
        # be deployed against them first.
        for location in range(LOCATIONS):
            self.declare_stream(f"turbidity-{location}")

    def declare_stream(self, stream_name: str) -> None:
        # "turbidity-2" is the turbidity sensor at urn:bulk/loc-2; filler
        # columns have no model point.
        if stream_name.startswith("turbidity-"):
            location = stream_name.removeprefix("turbidity-")
            self.declare(stream_name, property_uri=f"urn:bulk/loc-{location}-turbidity")
        else:
            self.declare(stream_name)
