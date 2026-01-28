#29 lines

import time
import requests
from chrontext import ChrontextClient

ALERT_URL = "http://host.docker.internal:10000/alerts"
THRESHOLD = 75.0

CHRON_SPARQL = """
PREFIX ontoX: <urn:water-ontology#>
PREFIX s223:  <http://data.ashrae.org/standard223#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qudt:  <http://qudt.org/schema/qudt/>
PREFIX unit:  <http://qudt.org/vocab/unit/>
PREFIX ct:    <https://github.com/magbak/chrontext#>
SELECT ?t ?v WHERE {
  ?basin rdf:type/rdfs:subClassOf* ontoX:ChlorinationBasin .
  ?basin s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:MilliGM_PER_L)) .
  ?pt ct:hasTimeseries ?ts .
  ?ts ct:hasDatapoint ?dp .
  ?dp ct:hasTimestamp ?t ; ct:hasValue ?v .
}
ORDER BY DESC(?t)
LIMIT 1
"""

def post_alert(message: dict) -> None:
    requests.post(ALERT_URL, json=message, timeout=5)

def main() -> None:
    client = ChrontextClient(endpoint="http://localhost:8080/sparql")  # illustrative
    while True:
        df = client.query(CHRON_SPARQL)  # returns a DataFrame-like table

        if df is None or len(df) == 0:
            post_alert({"text": "No data available for chlorine level.", "severity": "LOW", "data": {}})
        else:
            ts = df.iloc[0, 0]
            val = float(df.iloc[0, 1])

            if val > THRESHOLD:
                post_alert({"text": "Chlorine level exceeds safe threshold.", "severity": "HIGH",
                            "data": {"chlorine_level": val, "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts)}})
            else:
                post_alert({"text": "Chlorine level is within safe limits.", "severity": "NORMAL",
                            "data": {"chlorine_level": val, "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts)}})

        time.sleep(10)

if __name__ == "__main__":
    main()
