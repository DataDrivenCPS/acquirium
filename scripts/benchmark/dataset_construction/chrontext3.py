#TOTAL LOC: 15
q = '''
SELECT DISTINCT ?sed ?bio ?ts ?t ?v WHERE {
  ?sed rdf:type/rdfs:subClassOf* ontoX:SedimentationTank .
  FILTER (?sed IN (ex:AS_Secondary_Sedimentation, ex:RBC_Secondary_Sedimentation)) .
  { ?sed (s223:connectedTo){1,2} ?bio .
    FILTER EXISTS { ?bio rdf:type/rdfs:subClassOf* ontoX:AerationBasin }
    || FILTER EXISTS { ?bio rdf:type/rdfs:subClassOf* ontoX:RotatingBiologicalContactor } }
  UNION
  { BIND(?sed AS ?bio) }
  ?bio s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:GAL_US, unit:GAL_US_PER_MIN, unit:MilliGM_PER_L))
  ?pt ct:hasTimeseries ?ts .
  ?ts ct:hasDatapoint ?dp .
  ?dp ct:hasTimestamp ?t ; ct:hasValue ?v .
}
'''

df = engine.query(q)