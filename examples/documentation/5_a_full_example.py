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
# The mapping below says which property of the model each stream belongs to.

mapping = {
    "intake-in-flow-rate-m3s":                         "wbs:intake-in-flow-rate-m3s",
    "intake-in-tds-concentration":                     "wbs:intake-in-tds-concentration",
    "intake-in-tss-concentration-kgm3":                "wbs:intake-in-tss-concentration-kgm3",
    "conn-cartridge-filtration-to-S1-temperature-k":   "wbs:conn-cartridge-filtration-to-S1-temperature-k",
    "conn-cartridge-filtration-to-S1-flow-mass-water": "wbs:conn-cartridge-filtration-to-S1-flow-mass-water",
    "conn-cartridge-filtration-to-S1-flow-mass-tds":   "wbs:conn-cartridge-filtration-to-S1-flow-mass-tds",
    "P1-mechanical-power":                             "wbs:P1-mechanical-power",
    "P2-mechanical-power":                             "wbs:P2-mechanical-power",
    "RO-in-temperature-k":                             "wbs:RO-in-temperature-k",
    "RO-in-flow-mass-water":                           "wbs:RO-in-flow-mass-water",
    "RO-in-flow-mass-tds":                             "wbs:RO-in-flow-mass-tds",
    "RO-out-flow-mass-water":                          "wbs:RO-out-flow-mass-water",
    "RO-out-flow-mass-tds":                            "wbs:RO-out-flow-mass-tds",
    "RO-out-retentate-pressure-pa":                    "wbs:RO-out-retentate-pressure-pa",
    "RO-out-retentate-flow-mass-water":                "wbs:RO-out-retentate-flow-mass-water",
    "RO-out-retentate-flow-mass-tds":                  "wbs:RO-out-retentate-flow-mass-tds",
    "PXR-brine-out-flow-mass-water":                   "wbs:PXR-brine-out-flow-mass-water",
    "PXR-brine-out-flow-mass-tds":                     "wbs:PXR-brine-out-flow-mass-tds",
    "storage-tank-3-out-flow-rate-m3s":                "wbs:storage-tank-3-out-flow-rate-m3s",
    "storage-tank-3-out-tds-concentration":            "wbs:storage-tank-3-out-tds-concentration",
    "conn-cartridge-filtration-to-S1-pressure-pa":     "wbs:conn-cartridge-filtration-to-S1-pressure-pa",
    "P1-efficiency":                                   "wbs:P1-efficiency",
    "P1-out-pressure-pa":                              "wbs:P1-out-pressure-pa",
    "P2-efficiency":                                   "wbs:P2-efficiency",
    "PXR-efficiency":                                  "wbs:PXR-efficiency",
    "RO-membrane-area":                                "wbs:RO-membrane-area",
    "RO-in-pressure-pa":                               "wbs:RO-in-pressure-pa",
    "RO-out-pressure-pa":                              "wbs:RO-out-pressure-pa",
    "PXR-brine-out-pressure-pa":                       "wbs:PXR-brine-out-pressure-pa",
    "ferric-chloride-addition-ferric-chloride-dose":   "wbs:ferric-chloride-addition-ferric-chloride-dose",
    "anti-scalant-addition-anti-scalant-dose":         "wbs:anti-scalant-addition-anti-scalant-dose",
    "lime-addition-lime-dose":                         "wbs:lime-addition-lime-dose",
}

matches = [
    {"source_id": "csv-data", "stream_name": name, "property_uri": prop}
    for name, prop in mapping.items()
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
