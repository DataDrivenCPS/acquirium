# TOTAL LOC: 5
query = '''
SELECT Pump(B) JOIN (Screen(B) + GritChamber(B) + Grinder(B) + SedimentationTank(B)) * (Acidity(B) + Turbidity(B) + Concentration(B))
FROM Building B
WHERE B.Source = 'Benicia' AND B.Instance = 'Influent_Pump'
FILTER Data.Unit IN ('PH','NTU','MilliGM-PER-L','MicroGM-PER-L')
'''
data_df = fetch(query)