from acquirium import Acquirium

plX = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )
# plX.insert_graph("ontologies/water.ttl")
# plX.insert_graph("deployments/BENICIA/benicia-model-with-refs-thresholds.ttl",replace=False)
q1 = plX.find_entity(_class="tank",alias="Tanks")
q2 = q1.find_related(_class="pump",alias="Pumps",hops=1)
q3 = q2.find_related_data(_from="*",unit=["GAL_US","GAL_US_PER_MIN"])
q3.metadata_head()
data_df = q3.dataframe()
print(data_df.head())