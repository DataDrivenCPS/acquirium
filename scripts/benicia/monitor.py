from acquirium import App, Output
from acquirium import Acquirium, AppContext

class EffluentChlorineMonitor(App):
    name = "effluent_chlorine_monitoring"
    version = "0.1"
    app_type = "soft_sensor"
    outputs = [
        {
            "kind": "trigger",
            "point_uri": "urn:derived:chlorine_residual",
        }
    ]

    def build_query(self, aq: Acquirium):
        return (aq.query().entity("pump", alias="effluent")
                  .related("outlet Connection Point", alias="eff_cp", max_depth=1)
                  .related("fluid water", alias="eff_cp_medium", max_depth=1)
                  .measurement(frm="eff_cp", alias="eff-cl2",
                               quantity_kind="concentration", substance="chlorine")
                  )

    def run(self, ctx: AppContext) -> list[Output]:
        data = ctx.query.data(cast_value="float")
        latest = data.latest("eff-cl2")["value"][0]
        unit = data.units()["eff-cl2"]
        message = {"text":f"Latest effluent chlorine residual is {latest} {unit}"}

        outputs = [Output.trigger(
                url="localhost:10002/alerts",
                message=message
            )]

        return outputs

if __name__ == "__main__":
    acq = Acquirium(server_url="localhost", server_port=8000)
    # acq.register_app(EffluentChlorineMonitor(),replace=True)
    # acq.run_app("effluent_chlorine_monitoring", keep_alive=True, interval=10)
    acq.stop_app(app_id="effluent_chlorine_monitoring")
    print(acq.list_app_runs())
