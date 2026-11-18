"""3 - Inserting Metadata

The code of the tutorial page "3 - Inserting Metadata".
Run 1_inserting_data first, in the same folder.
"""

import acquirium as aq

acq = aq.init()

# Built-in keys

acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-tds",
    "unit": "mg/L",
    "quantity_kind": "mass concentration",
}])

# A text that does not match anything is skipped with a warning:

acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-ph",
    "unit": "pH units",
}])

# The symbol of the unit is matched:

acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-ph",
    "unit": "pH",
}])

# Your own keys

ph = acq.stream("lab-import", "effluent-ph")

print(acq.insert_metadata(ph, {"sampled_by": "day shift"}))

# Your own keys can also be given at registration, with the metadata key:

acq.register_streams([{
    "source_id": "lab-import",
    "stream_name": "effluent-tds",
    "metadata": {"sampled_by": "night shift"},
}])

# Seeing the metadata

q = acq.query().measurement()

print(q.include("unit", "quantity_kind", "sampled_by").metadata())

# Selecting streams by metadata
#
# By a built-in key:

print(q.where(quantity_kind="mass concentration").dataframe())

# By your own key:

print(q.where(sampled_by="day shift").dataframe())

# Several filters:

print(q.where(unit="mg/L", sampled_by="night shift").metadata())

# The values a key has:

print(q.options("sampled_by"))

# Converting units

print(q.where(unit="mg/L").data().convert_to("g/L").dataframe())

# Stop Acquirium.

aq.shutdown()
