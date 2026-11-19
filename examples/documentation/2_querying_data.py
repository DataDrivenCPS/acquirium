"""2 - Querying Data

The code of the tutorial page "2 - Querying Data".
Run 1_inserting_data first, in the same folder.
"""

import acquirium as aq

acq = aq.init()

# Building a query

q = acq.query().measurement()     # every registered stream

print(q)

# Reading the data
#
# Wide format, one column per stream:

print(q.dataframe())

# Narrow format, one row per reading:

print(q.dataframe(shape="narrow"))

# Selecting streams
#
# By label:

print(acq.query().measurement().where(label="effluent-tds").dataframe())

# By source_id and stream_name:

tds = acq.stream("lab-import", "effluent-tds")

print(acq.query().measurement(stream=tds).dataframe())

# Selecting a time range

print(q.dataframe(
    start="2026-08-08 10:05",
    end="2026-08-08 10:10",
))

# The latest reading:

print(q.dataframe(limit=1, order="desc"))

# Checking what a query selects

print(q.metadata())

# Reading one stream

print(acq.stream("lab-import", "effluent-tds").dataframe())

# Stop Acquirium.

aq.shutdown()
