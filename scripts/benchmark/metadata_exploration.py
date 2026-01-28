from acquirium import Acquirium

plX = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )
q1 = plX.find_entity(_class="tank",alias="Tanks")
q2 = q1.find_related(_class="pump",alias="Pumps",hops=1)
q2.metadata_head()
