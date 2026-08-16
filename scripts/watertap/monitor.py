from acquirium import App, Output
from acquirium import Acquirium, AppContext

class SeawaterTDSmonitor(App):
    name = "seawater_tds_monitoring"
    version = "0.1"
    app_type = "soft_sensor"
    outputs = [
        {
            "kind": "trigger",
            "point_uri": "urn:derived:chlorine_level",
        }
    ]

    def build_query(self, aq: Acquirium):
        return (aq.query().entity("reverse osmosis membrane", alias="ro")
                  .related("ConnectionPoint", alias="RO_cp", max_depth=1)
                  .related("Water-Seawater", alias="RO_CP_medium", max_depth=1)
                  .measurement(frm="RO_cp", alias="ro-tds",
                               quantity_kind="flow mass", substance="constituent salt")
                  )

    def run(self, ctx: AppContext) -> list[Output]:
        data = ctx.query.data(cast_value="float")
        latest = data.latest("ro-tds")["value"][0]
        unit = data.units()["ro-tds"].rsplit('/',1)[-1]
        message = {"text":f"Latest seawater salt level is {latest} {unit}"}

        outputs = [Output.trigger(
                url="localhost:10000/alerts",
                message=message
            )]

        return outputs

if __name__ == "__main__":
    acq = Acquirium(server_url="localhost", server_port=8000)
    # acq.register_app(SeawaterTDSmonitor(),replace=True)
    # acq.run_app("seawater_tds_monitoring", keep_alive=True, interval=10)
    acq.stop_app(app_id="seawater_tds_monitoring")
    print(acq.list_app_runs())