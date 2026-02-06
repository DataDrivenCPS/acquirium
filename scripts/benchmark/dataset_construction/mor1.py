#TOTAL LOC: 15

import pymortar
from pymortar import Client

client = Client(api_url="https://mortardata.org")  # illustrative

#9
tank_pump_points = """
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX bf:    <https://brickschema.org/schema/BrickFrame#>
PREFIX s223:  <http://data.ashrae.org/standard223#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qudt:  <http://qudt.org/schema/qudt/>
PREFIX unit:  <http://qudt.org/vocab/unit/>

SELECT DISTINCT ?tank ?pump ?pt WHERE {
  ?tank rdf:type/rdfs:subClassOf* brick:Tank .
  ?pump rdf:type/rdfs:subClassOf* brick:Pump .
  ?tank s223:connectedTo ?pump .
  { ?tank brick:hasPoint ?pt . }
  UNION
  { ?pump brick:hasPoint ?pt . }
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:GAL_US, unit:GAL_US_PER_MIN))
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
