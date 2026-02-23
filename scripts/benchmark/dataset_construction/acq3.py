# TOTAL LOC: 4

from acquirium import Acquirium
plX = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )


#4
q1 = plX.find_entity(_class="sedimentation_tank", alias="SecondarySed", name=["AS_Secondary_Sedimentation","RBC_Secondary_Sedimentation"])
q2 = q1.find_related(_class=["aeration_basin","rotating_biological_contactor"], alias="UpstreamBio", hops=2)
q3 = (q1 + q2).find_related_data(_from="*", unit=["GAL_US", "GAL_US_PER_MIN", "MilliGM-PER-L"])
data_df = q3.dataframe()
