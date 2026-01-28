#27
IC_START = "2024-01-01T00:00:00Z"
IC_END   = "2025-01-01T00:00:00Z"
TEST_START = "2025-01-01T00:00:00Z"
TEST_END   = "2025-02-01T00:00:00Z"

MORTAR_VIEW_QUERY = """
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

def run_fault_isolation(ic_df, test_df):
    return fault_isolation_pipeline(ic_df, test_df)

def fetch_window(client, start, end):
    req = FetchRequest(
        sites=["SITE"],
        views=[View(name="chlorine_pts", query=MORTAR_VIEW_QUERY)],
        dataFrames=[DataFrame(name="df", timeseries=["chlorine_pts.pt"])],
        time=TimeParams(start=start, end=end),
    )
    res = client.fetch(req)
    return res["dataFrames"]["df"]

def main():
    client = Client(api_url="https://mortardata.org")

    ic_df = fetch_window(client, IC_START, IC_END)
    test_df = fetch_window(client, TEST_START, TEST_END)

    results = run_fault_isolation(ic_df, test_df)

    report = format_report(results)
    persist_report(report)

if __name__ == "__main__":
    main()
