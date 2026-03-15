from acquirium import Acquirium

client = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )
client.insert_graph(
    "deployments/BENICIA/benicia-model-with-refs-thresholds.ttl"
)
client.insert_graph(
    "ontologies/water.ttl", replace=False
)

## Wait for the server to build embedding indexes after inserting the graphs. 
import time
time.sleep(20)

matches = client.client.resolve_text("condenser", kind="class", top_k=3, min_score=0.6)

print(matches)

matches = client.client.resolve_text("has accuracy", kind="predicate", top_k=3, min_score=0.6)

print(matches)

matches = client.client.resolve_text("nonexistent term", kind="class", top_k=3, min_score=0.6)

print(matches)

matches = client.client.resolve_text("kilogram", kind="unit", top_k=3, min_score=0.6)

print(matches)

pred_query = """
        SELECT DISTINCT ?uri ?label WHERE {
          {
            ?uri a <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
          } UNION {
            ?uri a <http://www.w3.org/2002/07/owl#ObjectProperty> .
          } UNION {
            ?uri a <http://www.w3.org/2002/07/owl#DatatypeProperty> .
          } UNION {
            ?s ?uri ?o .
          }
          OPTIONAL {
            {
              ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?label .
            } UNION {
              ?uri <http://www.w3.org/2004/02/skos/core#altLabel> ?label .
            }
            FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
          }
          FILTER(isIRI(?uri))
        }
        """


res = client.client.sparql_query(pred_query)
print(res)