from rdflib.namespace import Namespace
from rdflib import URIRef, RDF, RDFS

ACQUIRIUM_NS = Namespace("urn:acquirium#")
ACQUIRIUM_POINT_NS = Namespace("urn:acquirium:point#")

QUDT = Namespace("http://qudt.org/schema/qudt/")
QUDT_UNIT = Namespace("http://qudt.org/vocab/unit/")
QUDT_QUANTITY_KIND = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
S223 = Namespace("http://data.ashrae.org/standard223#")
WATR = Namespace("urn:nawi-water-ontology#")
BRICK = Namespace("https://brickschema.org/schema/Brick#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")


# Internal predicates for scaffold-only metadata
DEFAULT_MAIN_GRAPH = ACQUIRIUM_NS.MainGraph
DEFAULT_UNION_GRAPH = ACQUIRIUM_NS.UnionGraph
VIRTUAL_POINT = ACQUIRIUM_NS.VirtualPoint

LAST_REPORTED = ACQUIRIUM_NS.lastReported
IS_CALCULATED_FROM = ACQUIRIUM_NS.isCalculatedFrom

# Soft sensor SHACL-ish vocabulary
# SS = Namespace("urn:duckttape:ss#")
SOFT_SENSOR = ACQUIRIUM_NS.SoftSensor
STREAM = ACQUIRIUM_NS.Stream
DATA_SOURCE = ACQUIRIUM_NS.DataSource

DEPENDS_ON = ACQUIRIUM_NS.dependsOn
PRODUCES = ACQUIRIUM_NS.produces
HAS_MODULE = ACQUIRIUM_NS.hasModule
HAS_VERSION = ACQUIRIUM_NS.hasVersion
LAST_RUN = ACQUIRIUM_NS.lastRun
LAST_INPUT_CHANGE = ACQUIRIUM_NS.lastInputChange


HAS_EXTERNAL_REFERENCE = ACQUIRIUM_NS.hasExternalReference
HAS_MEDIUM = S223.hasMedium
OF_SUBSTANCE = S223.ofSubstance
HAS_QUANTITY_KIND = QUDT.hasQuantityKind
HAS_ENUMERATION_KIND = QUDT.hasEnumerationKind
HAS_UNIT = QUDT.hasUnit
DATA_SOURCE = ACQUIRIUM_NS.DataSource

# File-based Reference predicates
PARQUET_REF = ACQUIRIUM_NS.ParquetReference
CSV_REF = ACQUIRIUM_NS.CSVReference
REF_PATH = ACQUIRIUM_NS.hasFilePath
REF_TIME_COL = ACQUIRIUM_NS.hasTimeColumn
REF_VALUE_COL = ACQUIRIUM_NS.hasValueColumn

# MQTT Reference predicates
BROKER = ACQUIRIUM_NS.Broker
PORT = ACQUIRIUM_NS.Port
TOPIC = ACQUIRIUM_NS.Topic
VALUE_KEY = ACQUIRIUM_NS.value_key
TIME_KEY = ACQUIRIUM_NS.time_key
MQTT_REFERENCE = ACQUIRIUM_NS.MQTTReference

OWL_CLASS = URIRef("http://www.w3.org/2002/07/owl#Class")
OWL_OBJ_PROP = URIRef("http://www.w3.org/2002/07/owl#ObjectProperty")
OWL_DATA_PROP = URIRef("http://www.w3.org/2002/07/owl#DatatypeProperty")
OWL_ANN_PROP = URIRef("http://www.w3.org/2002/07/owl#AnnotationProperty")
RDF_PROP = RDF.Property

CLASS_TYPES = {RDFS.Class, OWL_CLASS}
PROP_TYPES = {RDF_PROP, OWL_OBJ_PROP, OWL_DATA_PROP, OWL_ANN_PROP}