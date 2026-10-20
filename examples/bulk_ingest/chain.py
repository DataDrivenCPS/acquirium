"""Six chained turbidity soft sensors over the synthetic plant.

    raw turbidity at each location
      -> bulk-turbidity-snap         1-minute averages
      -> bulk-turbidity-fill         fill gaps shorter than 2 hours
           -> bulk-plant-turbidity   plant-wide mean / max / locations reporting
           -> bulk-turbidity-high    alarm when above the location's threshold
                -> bulk-turbidity-sustained     alarm on for N minutes straight
                     -> bulk-plant-turbidity-alarms  locations in sustained alarm
"""

from pathlib import Path

import acquirium as aq

CALCULATIONS = Path(__file__).resolve().parent / "transforms.py"
SOURCE = "bulk-ingest"

MINUTE = 60 * 1_000_000_000
HOUR = 60 * MINUTE
# Gaps shorter than this are filled by linear interpolation; fill_gaps reads
# it back as its window's lookback.
MAX_GAP = 2 * HOUR

NTU = "http://qudt.org/vocab/unit/NTU"
TURBIDITY = "http://qudt.org/vocab/quantitykind/Turbidity"

RAW_TURBIDITY = """
PREFIX s223: <http://data.ashrae.org/standard223#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
PREFIX acq: <urn:acquirium#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX qk: <http://qudt.org/vocab/quantitykind/>

SELECT ?stream ?group WHERE {
    ?group s223:hasProperty ?point .
    ?point qudt:hasQuantityKind qk:Turbidity ;
           ref:hasExternalReference ?stream .
    ?stream acq:sourceId "bulk-ingest" .
}
"""

location_turbidity = aq.Output(unit=NTU, quantity_kind=TURBIDITY, on_group=True)
location_alarm = aq.Output(on_group=True)
plant_turbidity_output = aq.Output(unit=NTU, quantity_kind=TURBIDITY)
plant_count = aq.Output()

snap = aq.App(
    name="bulk-turbidity-snap",
    inputs=[aq.Selection(RAW_TURBIDITY, role="turbidity")],
    outputs={"turbidity": location_turbidity},
    function=aq.PythonFunction.from_file(CALCULATIONS, "snap_to_minute"),
    window=aq.FixedBucket(MINUTE),
)

fill = aq.App(
    name="bulk-turbidity-fill",
    inputs=[aq.Selection.outputs_of(snap, "turbidity", role="snapped")],
    outputs={"turbidity": location_turbidity},
    function=aq.PythonFunction.from_file(CALCULATIONS, "fill_gaps"),
    window=aq.Rolling(lookback_ns=MAX_GAP, step_ns=MINUTE, lookahead_ns=MAX_GAP),
)

plant_turbidity = aq.App(
    name="bulk-plant-turbidity",
    inputs=[aq.Selection.outputs_of(fill, "turbidity", role="filled", combine=True)],
    outputs={
        "mean": plant_turbidity_output,
        "max": plant_turbidity_output,
        "reporting": plant_count,
    },
    function=aq.PythonFunction.from_file(CALCULATIONS, "plant_turbidity"),
)

high = aq.App(
    name="bulk-turbidity-high",
    inputs=[aq.Selection.outputs_of(fill, "turbidity", role="filled")],
    outputs={"alarm": location_alarm},
    function=aq.PythonFunction.from_file(CALCULATIONS, "high_turbidity"),
)

# Minutes a location must stay high before its sustained alarm fires. The
# sustained app's window covers the longest of these.
SUSTAINED_MINUTES = {
    "urn:bulk/loc-0": 15,
    "urn:bulk/loc-1": 15,
    "urn:bulk/loc-2": 15,
    "urn:bulk/loc-3": 5,
}

sustained = aq.App(
    name="bulk-turbidity-sustained",
    inputs=[
        aq.Selection.outputs_of(
            high,
            "alarm",
            role="high",
            parameters={
                location: {"sustained_minutes": minutes}
                for location, minutes in SUSTAINED_MINUTES.items()
            },
        )
    ],
    outputs={"alarm": location_alarm},
    function=aq.PythonFunction.from_file(CALCULATIONS, "sustained_high_turbidity"),
    window=aq.Rolling(lookback_ns=15 * MINUTE, step_ns=MINUTE),
)

plant_alarms = aq.App(
    name="bulk-plant-turbidity-alarms",
    inputs=[aq.Selection.outputs_of(sustained, "alarm", role="sustained", combine=True)],
    outputs={"locations": plant_count},
    function=aq.PythonFunction.from_file(CALCULATIONS, "plant_alarm_count"),
)

SOFT_SENSORS = [snap, fill, plant_turbidity, high, sustained, plant_alarms]
