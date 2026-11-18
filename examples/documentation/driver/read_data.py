"""Writing a driver

Reads what the drivers of the tutorial page "Writing a driver" inserted.
Run it after the steps of the README in this folder.
"""

import acquirium as aq

acq = aq.init(address="http://127.0.0.1:8000")

# The streams of the CSV driver, the tank driver included:

q = acq.query().measurement()

print(q)

# The data of the tank driver:

tank = acq.query().measurement().where(label="Tank level")

print(tank.dataframe(limit=4))

# The streams of the CSV file, found through the plant model:

print(acq.query().entity("pump").measurement(quantity_kind="power"))

# Disconnect. The server keeps running.

aq.shutdown()
