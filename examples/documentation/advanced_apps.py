"""Run 5_a_full_example first, in the same data folder."""

import acquirium as aq
from plant_apps import pump_power_kw

acq = aq.init()
try:
    app = pump_power_kw(acq)
    print(acq.deploy_app(app, dry_run=True))
    print(acq.deploy_app(app))

    readings = acq.query().entity("pump", alias="pump").measurement(app="pump-power-kw")
    print(readings.dataframe(shape="narrow", limit=3))

    acq.insert_timeseries(
        source_id="csv-data", stream_name="P1-mechanical-power",
        rows=[("2026-01-01", 1_200_000.0)],
    )
    print(readings.dataframe(shape="narrow", start="2026-01-01"))

    print(acq.remove_app(app.name))
finally:
    aq.shutdown()
