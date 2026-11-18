import json

import acquirium as aq


class PlantCSV(aq.CSVIngestDriver):
    def setup(self):
        super().setup()
        self.insert_graph_file("example/model.ttl")
        with open("mapping.json") as f:
            self.mapping = json.load(f)

    def declare_stream(self, stream_name):
        self.declare(stream_name, property_uri=self.mapping[stream_name])
