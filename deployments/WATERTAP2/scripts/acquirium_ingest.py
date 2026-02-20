from acquirium import Acquirium

acq = Acquirium(
    server_url="localhost",
    server_port=8000,
    lexicon_path="ontologies/lexicon.json",
)

acq.insert_graph("deployments/WATERTAP2/models/test-model.ttl")

q = acq.find_all_data()
q.data_head()
q = q.filter_by_unit(unit = "kilogram per second")
q.data_head()
# q.add_grafana_panel(type="Gauge")
# q.add_grafana_panel(type="TimeSeries", panel_title="Pressure Over Time")
