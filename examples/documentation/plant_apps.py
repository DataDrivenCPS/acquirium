"""Declaration for the pump power example."""

from pathlib import Path

import acquirium as aq


def pump_power_kw(acq):
    query = acq.query().entity("pump", alias="pump").measurement(alias="power", quantity_kind="power")
    return aq.App(
        name="pump-power-kw",
        inputs={"power": aq.Selection.from_query(
            query, measurement="power", group="pump", exclude_app="pump-power-kw",
        )},
        outputs={"power": aq.Output(
            unit="http://qudt.org/vocab/unit/KiloW",
            quantity_kind="http://qudt.org/vocab/quantitykind/Power",
            on_group=True,
        )},
        function=aq.PythonFunction.from_file(Path(__file__), "watts_to_kilowatts"),
        window=aq.Pointwise(),
    )


def watts_to_kilowatts(frame, context):
    from acquirium.functions import emit, empty

    if frame.is_empty():
        return {"power": empty()}
    return {"power": emit(context, frame["value"][0] / 1000)}
