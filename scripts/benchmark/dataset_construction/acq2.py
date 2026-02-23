# TOTAL LOC: 4

from acquirium import Acquirium

plX = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )

#4
q1 = plX.find_entity(_class="pump", alias="InfluentPump", name="Influent_Pump")
q2 = q1.find_related(_class="*", alias="InfluentTrain", hops=4)
q3 = q2.find_related_data(_from="*", unit=["PH", "NTU", "MilliGM-PER-L", "MicroGM-PER-L"])
data_df = q3.dataframe()
