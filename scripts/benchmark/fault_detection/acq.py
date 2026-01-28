#14

from datetime import datetime
from acquirium import App, Output
from acquirium import Acquirium, AppContext

class ChlorineLevelWarning(App):
    name = "chlorine_level_warning"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"
    outputs = [
        {
            "kind": "trigger",
            "point_uri": "urn:derived:chlorine_level",
        }
    ]

    def build_query(self, aq: Acquirium):
        return aq.find_entity(_class="Chlorination Basin").find_related_data(unit=["MilliGM-PER-L"])

    def run(self, ctx: AppContext) -> list[Output]:
        ic_df = ctx.query.dataframe(cast_value='float' )
        test_df = ctx.query.dataframe(cast_value='float',start=datetime.utcnow()-timedelta(hours=1))  )
        fault_isolation_pipeline(ic_df, test_df)

acq = Acquirium(server_url="localhost", server_port=8000, lexicon_path="ontologies/lexicon.json")
acq.register_app(ChlorineLevelWarning())
acq.run_app("chlorine_level_warning", keep_alive=True, interval=10)
