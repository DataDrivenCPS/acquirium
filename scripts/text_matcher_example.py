from acquirium import Acquirium

client = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )
# client.insert_graph(
#     "deployments/BENICIA/benicia-model-with-refs-thresholds.ttl"
# )
# client.insert_graph(
#     "ontologies/water.ttl", replace=False
# )
# client.insert_graph(
#     "ontologies/qudt_unit.ttl", replace=False
# )


matches = client.client.resolve_text("pump", kind="class", top_k=1, min_score=0.6)

print(matches)

matches = client.client.resolve_text("has propty", kind="predicate", top_k=1, min_score=0.6)

print(matches)

matches = client.client.resolve_text("nonexistent term", kind="class", top_k=1, min_score=0.6)

print(matches)

matches = client.client.resolve_text("kg per sec", kind="class", top_k=1, min_score=0.6)

print(matches)