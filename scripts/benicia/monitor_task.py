"""Task twin of monitor.py: the same effluent-chlorine alert as a class-less task.

Compare with EffluentChlorineMonitor in monitor.py — identical query and
body, but no class, no build phase, no actor of its own: every task shares
one server-side host. Registers from a script or a notebook cell alike.
"""
from acquirium import Acquirium, Output


def check_effluent_chlorine(ctx) -> list[Output]:
    data = ctx.query.data(cast_value="float")
    latest = data.latest("eff-cl2")["value"][0]
    unit = data.units()["eff-cl2"]
    return [Output.trigger(
        url="localhost:10002/alerts",
        message={"text": f"Latest effluent chlorine residual is {latest} {unit}"},
    )]


if __name__ == "__main__":
    aq = Acquirium(server_url="localhost", server_port=8000)
    query = (aq.query().entity("pump", alias="effluent")
               .related("outlet Connection Point", alias="eff_cp", max_depth=1)
               .related("fluid water", alias="eff_cp_medium", max_depth=1)
               .measurement(frm="eff_cp", alias="eff-cl2",
                            quantity_kind="concentration", substance="chlorine"))
    aq.register_task(
        check_effluent_chlorine,
        name="effluent_chlorine_task",
        query=query,
        outputs=[{"kind": "trigger", "point_uri": "urn:derived:chlorine_residual_task"}],
        interval=10,
        run_mode="on_change",     # re-runs when the chlorine stream gets new data
        replace=True,
    )
    print(aq.list_app_runs(app_id="effluent_chlorine_task"))
    # aq.stop_app(app_id="effluent_chlorine_task")
