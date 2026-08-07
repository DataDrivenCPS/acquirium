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
        return (aq.find_entity(_class="pump", alias="effluent")
                  .find_related(_class="outlet Connection Point", alias=f"eff_cp", hops=1)
                  .find_related(_class="fluid water", alias=f"eff_cp_medium", hops=1)
                  .find_data(_from="eff_cp",alias="eff-cl2")
                  .filter_by_quantity_kind("concentration")
                  .filter_by_substance("chlorine")
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
