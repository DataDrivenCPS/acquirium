#TOTAL LOC: 13

import pymortar
from pymortar import Client

client = Client(api_url="https://mortardata.org")  # illustrative

#7
influent_train_q = """
SELECT DISTINCT ?pump ?eq ?pt WHERE {
  ?pump rdf:type/rdfs:subClassOf* ontoX:Pump .
  ?pump rdfs:label "Influent_Pump" .
  ?pump (s223:connectedTo){0,4} ?eq .
  ?eq s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:PH, unit:NTU, unit:MilliGM_PER_L, unit:MicroGM_PER_L))
}
"""

#6
request = FetchRequest(
  sites=["SITE"],
  views=[View(name="influent_train", query=influent_train_q)],
  dataFrames=[DataFrame(name="data_df", timeseries=["influent_train.pt"])]
)
result = client.fetch(request)
data_df = result["dataFrames"]["data_df"]

