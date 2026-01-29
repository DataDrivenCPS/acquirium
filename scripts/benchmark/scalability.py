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
        df = ctx.query.latest_data(cast_value='float')
        if df.is_empty() or df.shape[0] == 0:
            message = {"text" : "No data available for chlorine level.",
                       "severity": "LOW",
                       "data": {}}
        elif df[0,1] >75:
            message = {"text" : "Chlorine level exceeds safe threshold.",
                       "severity": "HIGH",
                       "data": {"chlorine_level": df[0,1], "timestamp": df[0,0].isoformat()}}
        else:
            message = {"text" : "Chlorine level is within safe limits.",
                       "severity": "NORMAL",
                       "data": {"chlorine_level": df[0,1], "timestamp": df[0,0].isoformat()}}
        return [Output.trigger(
            url= "host.docker.internal:10000/alerts",
            message=message
        )]

if __name__ == "__main__":
    all_apps=[]
    for i in range(1):
        all_apps.append(ChlorineLevelWarning())
        all_apps[i].name=f"chlorine_level_warning_{i}"


    acq = Acquirium(server_url="localhost", server_port=8000, lexicon_path="ontologies/lexicon.json")
    acq.insert_graph("deployments/BENICIA/benicia-model-with-refs-thresholds.ttl")
    i = 0
    for app in all_apps:
        acq.register_app(app)
        acq.run_app(app.name, keep_alive=True, interval=1)
        # acq.stop_app(app_id=app.name)
        i += 1