"""Task twin of monitor.py: the same seawater-TDS alert as a class-less task.

Compare with SeawaterTDSmonitor in monitor.py — identical query and body,
but no class, no build phase, no actor of its own: every task shares one
server-side host. Registers from a script or a notebook cell alike.
"""
from acquirium import Acquirium, Output


def check_seawater_tds(ctx) -> list[Output]:
    data = ctx.query.data(cast_value="float")
    latest = data.latest("ro-tds")["value"][0]
    unit = data.units()["ro-tds"].rsplit("/", 1)[-1]
    return [Output.trigger(
        url="localhost:10000/alerts",
        message={"text": f"Latest seawater salt level is {latest} {unit}"},
    )]


if __name__ == "__main__":
    aq = Acquirium(server_url="localhost", server_port=8000)
    query = (aq.query().entity("reverse osmosis membrane", alias="ro")
               .related("ConnectionPoint", alias="RO_cp", max_depth=1)
               .related("Water-Seawater", alias="RO_CP_medium", max_depth=1)
               .measurement(frm="RO_cp", alias="ro-tds",
                            quantity_kind="flow mass", substance="constituent salt"))
    aq.register_task(
        check_seawater_tds,
        name="seawater_tds_task",
        query=query,
        outputs=[{"kind": "trigger", "point_uri": "urn:derived:chlorine_level_task"}],
        interval=10,
        run_mode="interval",      # starts on registration and resumes after a restart
        replace=True,
    )
    print(aq.list_app_runs(app_id="seawater_tds_task"))
    # aq.stop_app(app_id="seawater_tds_task")
