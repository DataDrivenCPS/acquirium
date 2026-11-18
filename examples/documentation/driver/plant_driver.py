import acquirium as aq


class PlantCSV(aq.CSVIngestDriver):
    def declare_stream(self, stream_name):
        self.declare(stream_name, property_uri="urn:swro/" + stream_name)
