"""1 - Inserting Data

The code of the tutorial page "1 - Inserting Data".
Run acquirium clear first, so the results match the page.
"""

import acquirium as aq

acq = aq.init()

# Stream registration

acq.register_streams([
    {
        "source_id": "lab-import",
        "stream_name": "effluent-tds",
        "value_kind": "numeric",    # optional
    },
    {
        "source_id": "lab-import",
        "stream_name": "effluent-ph",
        "value_kind": "numeric",    # optional
    },
])

# Insert timeseries data

rows = [("2026-08-08 10:00", 412.0),
        ("2026-08-08 10:05", 415.3)]

print(acq.insert_timeseries("lab-import", "effluent-tds", rows))

# Several streams at once:

batch = {
    "effluent-tds": [("2026-08-08 10:10", 418.1)],
    "effluent-ph":  [("2026-08-08 10:10", 7.2)],
}

print(acq.insert_timeseries_batch("lab-import", batch))

# Inserting the same data twice
#
# With replace=True the stream contains exactly the rows you supply.

print(acq.insert_timeseries("lab-import", "effluent-tds", rows, replace=True))

# An empty list with replace=True clears the stream.

print(acq.insert_timeseries("lab-import", "effluent-tds", [], replace=True))

# Insert the rows again, so the next pages have their data.

acq.insert_timeseries("lab-import", "effluent-tds", rows)

print(acq.insert_timeseries_batch("lab-import", batch))

# Stop Acquirium.

aq.shutdown()
