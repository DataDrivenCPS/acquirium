#TOTAL LOC: 17

import pymortar
from pymortar import Client

client = Client(api_url="https://mortardata.org")  # illustrative

#11
tank_pump_points = """
SELECT DISTINCT ?sed ?bio ?pt WHERE {
  ?sed rdf:type/rdfs:subClassOf* ontoX:SedimentationTank .
  FILTER (?sed IN (ex:AS_Secondary_Sedimentation, ex:RBC_Secondary_Sedimentation)) .
  { ?sed (s223:connectedTo){1,2} ?bio .
    ?bio rdf:type/rdfs:subClassOf* ?bioClass .
    FILTER (?bioClass IN (ontoX:AerationBasin, ontoX:RotatingBiologicalContactor)) . }
  UNION
  { BIND(?sed AS ?bio) }
  ?bio s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:GAL_US, unit:GAL_US_PER_MIN, unit:MilliGM_PER_L))
}
"""

#4
request = pymortar.FetchRequest(
    sites=["SITE_ID_OR_NAME"],
    views=[
        pymortar.View(
            name="tank_pump_view",
            query=tank_pump_points,
        ),
    ],
    dataFrames=[
        # Pull raw timeseries for the point variable ?pt
        pymortar.DataFrame(
            name="data_df",
            timeseries=["tank_pump_view.pt"],
            # optionally:
            # aggregation=pymortar.Aggregation(mean=True, window="10m")
        )
    ],
    # optional time bounds
    # time=pymortar.TimeParams(start="2024-01-01T00:00:00Z", end="2024-02-01T00:00:00Z")
)

#2
result = client.fetch(request)
data_df = result["dataFrames"]["data_df"]
