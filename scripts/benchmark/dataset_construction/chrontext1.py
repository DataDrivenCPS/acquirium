#TOTAL LOC: 16
q = '''
SELECT DISTINCT ?tank ?pump ?ts ?t ?v WHERE {
  ?tank rdf:type/rdfs:subClassOf* ontoX:Tank .
  ?pump rdf:type/rdfs:subClassOf* ontoX:Pump .
  ?tank s223:connectedTo ?pump .
  { ?tank s223:hasProperty ?prop . }
  UNION
  { ?pump s223:hasProperty ?prop . }
  ?prop qudt:hasUnit ?u .
  FILTER (?u IN (unit:GAL_US, unit:GAL_US_PER_MIN))
  ?prop ct:hasTimeseries ?ts .
  ?ts ct:hasDatapoint ?dp .
  ?dp ct:hasTimestamp ?t ;
      ct:hasValue ?v .
  FILTER (?t >= "2024-01-01T00:00:00Z"^^xsd:dateTime && ?t < "2024-02-01T00:00:00Z"^^xsd:dateTime)
}
'''

df = engine.query(q)