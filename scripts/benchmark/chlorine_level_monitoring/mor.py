# 28 lines


import time
import requests
import pandas as pd
from datetime import datetime, timezone
from pymortar import Client, FetchRequest, View, DataFrame, TimeParams

ALERT_URL = "http://host.docker.internal:10000/alerts"
THRESHOLD = 75.0

CHLORINE_VIEW_QUERY = """
PREFIX brick: <https://brickschema.org/schema/Brick#>
PREFIX ontoX: <urn:water-ontology#>
PREFIX s223:  <http://data.ashrae.org/standard223#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qudt:  <http://qudt.org/schema/qudt/>
PREFIX unit:  <http://qudt.org/vocab/unit/>
SELECT DISTINCT ?pt WHERE {
  ?basin rdf:type/rdfs:subClassOf* ontoX:ChlorinationBasin .
  ?basin s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:MilliGM_PER_L))
}
"""

def post_alert(message: dict) -> None:
    requests.post(ALERT_URL, json=message, timeout=5)

def main() -> None:
    client = Client(api_url="https://mortardata.org")  # illustrative
    while True:
        end = datetime.now(timezone.utc)
        start = end.replace(minute=end.minute - 60)

        req = FetchRequest(
            sites=["SITE"],
            views=[View(name="chlorine_pts", query=CHLORINE_VIEW_QUERY)],
            dataFrames=[DataFrame(name="chlorine_df", timeseries=["chlorine_pts.pt"])],
            time=TimeParams(start=start.isoformat(), end=end.isoformat()),
        )
        res = client.fetch(req)
        df = res["dataFrames"]["chlorine_df"]  # expected tabular timeseries

        if df is None or len(df) == 0:
            post_alert({"text": "No data available for chlorine level.", "severity": "LOW", "data": {}})
        else:
            df = df.sort_values(by=df.columns[0], ascending=False)
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
