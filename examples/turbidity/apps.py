"""The turbidity tutorial's two apps: a cleaner and a sustained alarm."""

from pathlib import Path

import acquirium as aq

CALCULATIONS = Path(__file__).resolve().parent / "calculations.py"
SOURCE = "turbidity-analysers"
NTU = "http://qudt.org/vocab/unit/NTU"
TURBIDITY = "http://qudt.org/vocab/quantitykind/Turbidity"
MINUTE = 60 * 1_000_000_000

# Each location's limit, and how long turbidity must stay above it.
LIMITS = {
    "urn:turbidity:filter-1": {"limit_ntu": 0.5, "sustained_minutes": 15},
    "urn:turbidity:filter-2": {"limit_ntu": 0.5, "sustained_minutes": 15},
    "urn:turbidity:filter-3": {"limit_ntu": 0.5, "sustained_minutes": 15},
    "urn:turbidity:combined": {"limit_ntu": 0.3, "sustained_minutes": 5},
}


def declarations(client):
    raw = (
        client.query()
        .measurement(alias="raw", quantity_kind=TURBIDITY)
        .context(alias="location")
    )
    clean = aq.App(
        name="turbidity-clean",
        inputs=[aq.Selection.from_query(
            raw, alias="raw", group="location", exclude_app="turbidity-clean"
        )],
        outputs={"turbidity": aq.Output(unit=NTU, quantity_kind=TURBIDITY, on_group=True)},
        function=aq.PythonFunction.from_file(CALCULATIONS, "clean"),
        window=aq.Rolling(lookback_ns=5 * MINUTE, step_ns=MINUTE, lookahead_ns=5 * MINUTE),
    )
    alarm = aq.App(
        name="turbidity-alarm",
        inputs=[aq.Selection.outputs_of(clean, "turbidity", role="clean", parameters=LIMITS)],
        outputs={"alarm": aq.Output(on_group=True)},
        function=aq.PythonFunction.from_file(CALCULATIONS, "alarm"),
        window=aq.Rolling(lookback_ns=15 * MINUTE, step_ns=MINUTE),
    )
    return [clean, alarm]
