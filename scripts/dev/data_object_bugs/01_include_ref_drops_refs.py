"""Bug 1: compact narrow output drops all but one ref per point.

Cause: the compact path in ``DataObject._compact_dataframe`` dedups the tall
frame on ``(data_alias, point_uri, time)``, so when two refs (two drivers) feed
the same point at the same timestamp only the first row survives, whether or
not ``include_ref`` is set. The raw path (``compact=False``) has no such dedup
and shows both. Fix: dedup on ``(data_alias, point_uri, ref_uri, time)``. Data
for a point includes every ref's observations; ``compact`` and ``include_ref``
only change whether the ref URI is visible.
"""
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

import acquirium

point = "urn:site#flow"
t = datetime(2026, 1, 1, tzinfo=timezone.utc)

with TemporaryDirectory() as data_dir:
    ac = acquirium.init(data_dir=data_dir)
    try:
        for source, value in [("driver_a", 1.0), ("driver_b", 2.0)]:
            ac.register_streams([{"source_id": source, "ref_name": "flow",
                                  "point_uri": point, "value_kind": "numeric"}])
            ac.insert_timeseries(source, "flow", [(t, value)], point_uri=point)

        data = ac.query().measurement().data()   # the only registered point
        with_ref = data.dataframe(shape="narrow", include_ref=True)
        raw = data.dataframe(shape="narrow", compact=False)

        print(with_ref)
        print(raw)
        print(f"include_ref=True rows: {with_ref.height}   raw rows: {raw.height}")
        assert with_ref.height == raw.height == 2, "include_ref=True should keep one row per ref"
    finally:
        acquirium.shutdown()
