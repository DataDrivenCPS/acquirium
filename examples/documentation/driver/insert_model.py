"""Insert the plant model of the example into the running server."""

import acquirium as aq

acq = aq.init(address="http://127.0.0.1:8000")

acq.insert_graph_file("example/model.ttl", source_id="plant")

aq.shutdown()
