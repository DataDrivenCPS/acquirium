# TOTAL LOC: 5
query = '''
SELECT (SedimentationTank(B) JOIN (AerationBasin(B) + RotatingBiologicalContactor(B)))
     * (Volume(B) + VolumeFlowRate(B) + Concentration(B))
FROM Building B
WHERE B.Source = 'Benicia' AND B.Instance IN ('AS_Secondary_Sedimentation','RBC_Secondary_Sedimentation')
FILTER Data.Unit IN ('GAL_US','GAL_US-PER-MIN','MilliGM-PER-L')
'''
data_df = fetch(query)