#27


IC_START = "2024-01-01T00:00:00Z"
IC_END   = "2025-01-01T00:00:00Z"
TEST_START = "2025-01-01T00:00:00Z"
TEST_END   = "2025-02-01T00:00:00Z"

CHRON_QUERY_TMPL = """
PREFIX ontoX: <urn:water-ontology#>
PREFIX s223:  <http://data.ashrae.org/standard223#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qudt:  <http://qudt.org/schema/qudt/>
PREFIX unit:  <http://qudt.org/vocab/unit/>
PREFIX ct:    <https://github.com/magbak/chrontext#>
PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>

SELECT ?pt ?t ?v WHERE {
  ?basin rdf:type/rdfs:subClassOf* ontoX:ChlorinationBasin .
  ?basin s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:MilliGM_PER_L)) .
  ?pt ct:hasTimeseries ?ts .
  ?ts ct:hasDatapoint ?dp .
  ?dp ct:hasTimestamp ?t ; ct:hasValue ?v .
  FILTER (?t >= "{start}"^^xsd:dateTime && ?t < "{end}"^^xsd:dateTime)
}
"""

def run_fault_isolation(ic_df, test_df):
    return fault_isolation_pipeline(ic_df, test_df)

def main():
    client = ChrontextClient(endpoint="http://localhost:8080/sparql")

    ic_q = CHRON_QUERY_TMPL.format(start=IC_START, end=IC_END)
    test_q = CHRON_QUERY_TMPL.format(start=TEST_START, end=TEST_END)

    ic_long = client.query(ic_q)
    test_long = client.query(test_q)

    ic_df = pivot_timeseries(ic_long, index="t", columns="pt", values="v", cast_value="float")
    test_df = pivot_timeseries(test_long, index="t", columns="pt", values="v", cast_value="float")

    results = run_fault_isolation(ic_df, test_df)

    report = format_report(results)
    persist_report(report)

if __name__ == "__main__":
    main()
