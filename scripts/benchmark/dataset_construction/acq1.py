# TOTAL LOC: 4

from acquirium import Acquirium
plX = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )


#4
q1 = plX.find_entity(_class="tank",alias="Tanks")
q2 = q1.find_related(_class="pump",alias="Pumps",hops=1)
q3 = q2.find_related_data(_from="*",unit=["GAL_US","GAL_US_PER_MIN"])
data_df = q3.dataframe()
