"""5 - A Full Example

The code of the tutorial page "5 - A Full Example": the seawater desalination plant that ships with Acquirium, with one year of data.

Before each run, erase the stored data and create the example files:

    acquirium clear
    acquirium example init --force
"""

from datetime import datetime, timezone

import polars as pl

import acquirium as aq

acq = aq.init()

# 1. Insert the plant model

acq.insert_graph_file("example/model.ttl", source_id="plant")

# 2. Register the streams of the file
#
# The file is the source, and every column other than time is one stream.

data = pl.read_csv("example/data.csv", try_parse_dates=True)
stream_names = [column for column in data.columns if column != "time"]

acq.register_streams([
    {"source_id": "csv-data", "stream_name": name} for name in stream_names
])

# Insert the data, one list of (timestamp, value) pairs per stream.

times = data["time"].to_list()

print(acq.insert_timeseries_batch("csv-data", {
    name: list(zip(times, data[name].to_list())) for name in stream_names
}))

# 3. Find what is not connected
#
# The properties of the model that have no stream:

print(acq.query().property(has_stream=False))

# The streams that are not connected to a property of the model:

print(acq.query().measurement().where(is_disconnected=True))

# 4. Connect the streams to the model
#
# The column headers of the file are the names of the properties, so the two lists are matched by name.

properties = acq.query().property(has_stream=False).metadata()["property"].to_list()
streams = acq.query().measurement().where(is_disconnected=True).metadata()["data.label"].to_list()

matches = [
    {"source_id": "csv-data", "stream_name": name, "property_uri": "wbs:" + name}
    for name in streams
    if "wbs:" + name in properties
]

acq.register_streams(matches)

# Both lists are now empty.

print(acq.query().property(has_stream=False))

print(acq.query().measurement().where(is_disconnected=True))

# 5. Query the plant
#
# The pumps of the plant:

print(acq.query().entity("pump").include("label").metadata())

# The reverse osmosis membrane downstream of each pump:

print(acq.query().entity("pump").related("Reverse Osmosis Membrane", direction="downstream").metadata())

# The power streams of the pumps:

q = acq.query().entity("pump").measurement(quantity_kind="power")

print(q)

# The nearest pressure stream upstream of the membrane:

ro = acq.query().entity(uri="wbs:RO", alias="ro")

print(ro.measurement(direction="upstream", quantity_kind="pressure").metadata())

# The pump power of the first three hours of July:

print(q.dataframe(
    start=datetime(2025, 7, 1, tzinfo=timezone.utc),
    end=datetime(2025, 7, 1, 3, tzinfo=timezone.utc),
))

# The same streams in kilowatts:

print(q.data().convert_to("kW").dataframe(limit=3))

# Stop Acquirium.

aq.shutdown()
