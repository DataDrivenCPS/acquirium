#TOTAL LOC: 11
q = '''
SELECT DISTINCT ?pump ?eq ?ts ?t ?v WHERE {
  ?pump rdf:type/rdfs:subClassOf* ontoX:Pump .
  ?pump rdfs:label "Influent_Pump" .
  ?pump (s223:connectedTo){0,4} ?eq .
  ?eq s223:hasProperty ?pt .
  ?pt qudt:hasUnit ?u .
  FILTER (?u IN (unit:PH, unit:NTU, unit:MilliGM_PER_L, unit:MicroGM_PER_L))
  ?pt ct:hasTimeseries ?ts .
  ?ts ct:hasDatapoint ?dp .
  ?dp ct:hasTimestamp ?t ; ct:hasValue ?v .
}
'''

df = engine.query(q)