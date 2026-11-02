"""Membrane-skid apps whose results build on the results before them."""

from pathlib import Path

import acquirium as aq

DIRECTORY = Path(__file__).resolve().parent
SOURCE = "membrane-skids"
SMOOTHED = "permeate-conductivity-smoothed"
DRIFT = "permeate-conductivity-drift"
PERMEABILITY = "membrane-permeability"
SKIDS = ("urn:membrane:skid-1", "urn:membrane:skid-2")
MINUTE = 60_000_000_000
HOUR = 60 * MINUTE
CONDUCTIVITY_UNIT = "http://qudt.org/vocab/unit/MicroS-PER-CentiM"
PRESSURE_UNIT = "http://qudt.org/vocab/unit/BAR"
PREFIXES = """
PREFIX ex: <urn:membrane:>
PREFIX s223: <http://data.ashrae.org/standard223#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
PREFIX acq: <urn:acquirium#>
"""
# Per skid: the smoothing time constant, the drift detector's normal level,
# allowance and alarm limit (uS/cm), and the permeability fit's forgetting
# factor per hourly window and starting estimate (L/m2/h/bar). The estimate
# trails a steady decline by about forgetting / (1 - forgetting) windows.
SMOOTHING = {skid: {"tau_ns": 30 * MINUTE} for skid in SKIDS}
DRIFT_LIMITS = {
    "urn:membrane:skid-1": {"target": 50.0, "slack": 2.0, "limit": 40.0},
    "urn:membrane:skid-2": {"target": 60.0, "slack": 2.0, "limit": 40.0},
}
FIT = {
    skid: {"forgetting": 0.8, "initial": 3.0, "initial_variance": 100.0} for skid in SKIDS
}


def register(client):
    streams = []
    for skid in ("skid-1", "skid-2"):
        for measurement, unit in (
            ("flux", None),
            ("tmp", PRESSURE_UNIT),
            ("conductivity", CONDUCTIVITY_UNIT),
        ):
            streams.append(
                {
                    "source_id": SOURCE,
                    "ref_name": f"{skid}-{measurement}",
                    "point_uri": f"urn:membrane:{skid}-{measurement}",
                    "unit": unit,
                    "value_kind": "numeric",
                }
            )
    client.register_streams(streams)
    client.insert_graph_file(DIRECTORY / "metadata.ttl", source_id="membrane-layout")


def measured(kind, role, parameters=None):
    """Each skid's point of class ``kind``, grouped by skid."""
    return aq.Selection(
        PREFIXES
        + f"""
        SELECT ?stream ?group WHERE {{
            ?group a ex:MembraneSkid ; s223:hasProperty ?point .
            ?point a ex:{kind} ; ref:hasExternalReference ?stream .
            ?stream acq:sourceId "{SOURCE}" .
        }}
        """,
        role=role,
        parameters=parameters,
    )


def declarations(*, module=None):
    module = Path(module) if module is not None else DIRECTORY / "functions.py"

    def function(entrypoint):
        return aq.PythonFunction.from_file(module, entrypoint, sequential=True)

    def output(unit=None):
        return aq.Output(unit=unit, on_group=True)

    return [
        aq.App(
            name=SMOOTHED,
            inputs=[measured("PermeateConductivity", "conductivity", SMOOTHING)],
            outputs={"smoothed": output(CONDUCTIVITY_UNIT)},
            function=function("smoothed"),
        ),
        aq.App(
            name=DRIFT,
            inputs=[measured("PermeateConductivity", "conductivity", DRIFT_LIMITS)],
            outputs={"cusum": output(CONDUCTIVITY_UNIT), "alarm": output(CONDUCTIVITY_UNIT)},
            function=function("drift"),
        ),
        aq.App(
            name=PERMEABILITY,
            inputs=[
                measured("PermeateFlux", "flux", FIT),
                measured("TransmembranePressure", "tmp"),
            ],
            outputs={"permeability": output(), "variance": output()},
            function=function("permeability"),
            # One estimate per hour, from that hour's mean flux and TMP.
            window=aq.FixedBucket(HOUR),
        ),
    ]


def deploy(client, *, module=None):
    for declaration in declarations(module=module):
        state = client.deploy_app(declaration)["app"]
        if state["status"] != "active" or state["error"]:
            raise RuntimeError(f"{declaration.name}: {state}")
