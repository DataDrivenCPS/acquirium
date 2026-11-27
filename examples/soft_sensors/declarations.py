"""Resolve semantic membership in Acquirium; execute calculations in Escapement."""

import math
from pathlib import Path

import pandas as pd

import acquirium as aq

DIRECTORY = Path(__file__).resolve().parent
SOURCE = "soft-sensor-probes"
MEAN = "tank-temperature-mean"
HOTTEST = "hottest-tank"
ALARM = "high-tank-temperature"
PLANT = "urn:soft-sensors:plant"
TANK_A = "urn:soft-sensors:tank-a"
TANK_B = "urn:soft-sensors:tank-b"
UNIT = "http://qudt.org/vocab/unit/DEG_C"
QUANTITY = "http://qudt.org/vocab/quantitykind/Temperature"
BUCKET = pd.Timedelta("10s")
BUCKET_NS = BUCKET.value  # The streaming producer aligns integer timestamps to it.
PREFIXES = """
PREFIX ex: <urn:soft-sensors:>
PREFIX s223: <http://data.ashrae.org/standard223#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
PREFIX acq: <urn:acquirium#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
"""


def register(client):
    client.register_streams(
        [
            {
                "source_id": SOURCE,
                "stream_name": probe,
                "property_uri": f"urn:soft-sensors:probe-{probe}",
                "unit": UNIT,
                "quantity_kind": QUANTITY,
                "value_kind": "numeric",
            }
            for probe in ("a1", "a2", "b1")
        ]
    )
    client.insert_graph_file(DIRECTORY / "metadata.ttl", source_id="soft-sensor-layout")


def declarations(*, threshold=27.0, module=None):
    if isinstance(threshold, bool) or not math.isfinite(threshold):
        raise ValueError("threshold must be a finite number in Celsius")
    module = Path(module) if module is not None else DIRECTORY / "transforms.py"
    output = aq.Output(unit=UNIT, quantity_kind=QUANTITY, on_group=True)
    return [
        aq.App(
            name=MEAN,
            inputs={
                "temperature": aq.Selection(
                    PREFIXES
                    + f"""
                    SELECT ?stream ?group ?unit WHERE {{
                        ?group a ex:Tank ; s223:hasProperty ?point .
                        ?point ref:hasExternalReference ?stream ; qudt:hasUnit ?unit .
                        ?stream acq:sourceId "{SOURCE}" .
                    }}
                    """,
                    target_unit=UNIT,
                )
            },
            outputs={"mean": output},
            function=aq.PythonFunction.from_file(module, "tank_mean"),
            window=aq.FixedBucket(BUCKET),
        ),
        aq.App(
            name=HOTTEST,
            inputs={
                "mean": aq.Selection(
                    PREFIXES
                    + f"""
                    SELECT ?stream ?group WHERE {{
                        ?tank a ex:Tank ; ex:partOf ?group ; s223:hasProperty ?point .
                        ?point acq:producedBy "{MEAN}" ; ref:hasExternalReference ?stream .
                    }}
                    """,
                )
            },
            outputs={"hottest": output},
            function=aq.PythonFunction.from_file(module, "hottest_tank"),
        ),
        aq.App(
            name=ALARM,
            inputs={
                "hottest": aq.Selection(
                    PREFIXES
                    + f"""
                    SELECT ?stream ?group WHERE {{
                        ?group a ex:Plant ; s223:hasProperty ?point .
                        ?point acq:producedBy "{HOTTEST}" ; ref:hasExternalReference ?stream .
                    }}
                    """,
                    parameters={PLANT: {"threshold": float(threshold)}},
                )
            },
            outputs={"alarm": output},
            # Passes the hottest temperature through while it is above the threshold.
            function=aq.Native.threshold("alarm", above=aq.Parameter("threshold")),
        ),
    ]


def deploy(client, *, threshold=27.0, module=None):
    for declaration in declarations(threshold=threshold, module=module):
        state = client.deploy_app(declaration)["app"]
        if state["status"] != "active" or state["error"]:
            raise RuntimeError(f"{declaration.name}: {state}")
