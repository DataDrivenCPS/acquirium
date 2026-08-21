from rdflib.namespace import Namespace
from rdflib import URIRef, RDF, RDFS

### ACQUIRIUM INTERNAL NAMESPACES
ACQUIRIUM_NS = Namespace("urn:acquirium#")
ACQUIRIUM_POINT_NS = Namespace("urn:acquirium:point#")

### External Namespaces
QUDT = Namespace("http://qudt.org/schema/qudt/")
QUDT_UNIT = Namespace("http://qudt.org/vocab/unit/")
QUDT_QUANTITY_KIND = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
S223 = Namespace("http://data.ashrae.org/standard223#")
WATR = Namespace("urn:nawi-water-ontology#")
BRICK = Namespace("https://brickschema.org/schema/Brick#")
BRICK_REF = Namespace("https://brickschema.org/schema/Brick/ref#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")


# Internal predicates for scaffold-only metadata
DEFAULT_MAIN_GRAPH = ACQUIRIUM_NS.MainGraph
VIRTUAL_POINT = ACQUIRIUM_NS.VirtualPoint

# Well-known URI representing the Acquirium TimescaleDB instance
ACQUIRIUM_DB_URI = ACQUIRIUM_NS.TimescaleDB
DATABASE = ACQUIRIUM_NS.Database


LAST_REPORTED = ACQUIRIUM_NS.lastReported
IS_CALCULATED_FROM = ACQUIRIUM_NS.isCalculatedFrom

# App metadata
APP = ACQUIRIUM_NS.App
APP_QUERY = ACQUIRIUM_NS.querySpec
APP_PARAMS = ACQUIRIUM_NS.paramSpec
HAS_IMAGE = ACQUIRIUM_NS.hasImage
HAS_ENTRYPOINT = ACQUIRIUM_NS.hasEntrypoint
HAS_COMMAND = ACQUIRIUM_NS.hasCommand
HAS_APP_CLASS = ACQUIRIUM_NS.hasAppClass
STORAGE_BACKEND = ACQUIRIUM_NS.storageBackend
TIMESERIES_STREAM = ACQUIRIUM_NS.TimeseriesStream
EVENT_STREAM = ACQUIRIUM_NS.EventStream
THRESHOLD = ACQUIRIUM_NS.Threshold
ALARM = ACQUIRIUM_NS.Alarm
REPORT = ACQUIRIUM_NS.Report

# Soft sensor SHACL-ish vocabulary
SOFT_SENSOR = ACQUIRIUM_NS.SoftSensor
STREAM = ACQUIRIUM_NS.Stream
DATA_SOURCE = ACQUIRIUM_NS.DataSource

DEPENDS_ON = ACQUIRIUM_NS.dependsOn
PRODUCES = ACQUIRIUM_NS.produces
HAS_MODULE = ACQUIRIUM_NS.hasModule
HAS_VERSION = ACQUIRIUM_NS.hasVersion
LAST_RUN = ACQUIRIUM_NS.lastRun
LAST_INPUT_CHANGE = ACQUIRIUM_NS.lastInputChange


# External-reference vocabulary follows ontologies/ref-schema.ttl
# (https://brickschema.org/schema/Brick/ref#). Predicates that the schema
# does not define live under ACQUIRIUM_NS and are Acquirium-specific.
HAS_EXTERNAL_REFERENCE = BRICK_REF.hasExternalReference
EXTERNAL_REFERENCE = BRICK_REF.ExternalReference
TIMESERIES_REFERENCE = BRICK_REF.TimeseriesReference
HAS_TIMESERIES_ID = BRICK_REF.hasTimeseriesId
STORED_AT = BRICK_REF.storedAt


# Predicates stored on TimeseriesReference nodes to distinguish
# Acquirium-managed streams (these two are present) from external PG
# references (storedAt is a literal DSN).
ACQUIRIUM_SOURCE_ID  = ACQUIRIUM_NS.sourceId   # the registered datasource name
ACQUIRIUM_REF_NAME   = ACQUIRIUM_NS.refName    # the source-local stream identifier
ACQUIRIUM_VALUE_KIND = ACQUIRIUM_NS.valueKind  # "numeric" or "text"

# Set true on a reference whose unit was knowingly registered as
# irreconcilable with its linked point's. Registration refuses such a pair
# without it; with it, reads return the point's unit unconverted and warn.
UNIT_MISMATCH_ALLOWED = ACQUIRIUM_NS.unitMismatchAllowed

# Class for registered datasource nodes
ACQUIRIUM_DATASOURCE = ACQUIRIUM_NS.DataSourceRegistry

HAS_MEDIUM = S223.hasMedium
OF_MEDIUM = S223.ofMedium
OF_SUBSTANCE = S223.ofSubstance
HAS_QUANTITY_KIND = QUDT.hasQuantityKind
HAS_ENUMERATION_KIND = QUDT.hasEnumerationKind
HAS_UNIT = QUDT.hasUnit

HAS_LOG = ACQUIRIUM_NS.hasLog
LOGBOOK = ACQUIRIUM_NS.Logbook
PLANT_URI = str(ACQUIRIUM_NS.Plant)  # Generic URI representing the entire plant


# Origin tag literal on a reference node (e.g. "Lab", "SCADA").
DATA_SOURCE = ACQUIRIUM_NS.dataSource


# MQTT Reference predicates. timeKey/valueKey are Acquirium-specific (they
# describe how to decode a JSON payload) and live under ACQUIRIUM_NS.
MQTT_REFERENCE = BRICK_REF.MQTTReference
HAS_MQTT_REFERENCE = BRICK_REF.hasMQTTReference
MQTT_BROKER = BRICK_REF.MQTTBroker  # accepts "host", "host:port", or "mqtt(s)://..."
MQTT_TOPIC = BRICK_REF.MQTTTopic
MQTT_PORT = ACQUIRIUM_NS.mqttPort
TIME_KEY = ACQUIRIUM_NS.timeKey
VALUE_KEY = ACQUIRIUM_NS.valueKey
HAS_PYOMO_VAR = ACQUIRIUM_NS.hasPyomoVar

# Direct timeseries references are created by drivers. External databases
# should be ingested through a driver rather than queried from graph metadata.
HAS_TIMESERIES_REFERENCE = BRICK_REF.hasTimeseriesReference

OWL_CLASS = URIRef("http://www.w3.org/2002/07/owl#Class")
OWL_OBJ_PROP = URIRef("http://www.w3.org/2002/07/owl#ObjectProperty")
OWL_DATA_PROP = URIRef("http://www.w3.org/2002/07/owl#DatatypeProperty")
OWL_ANN_PROP = URIRef("http://www.w3.org/2002/07/owl#AnnotationProperty")
RDF_PROP = RDF.Property

CLASS_TYPES = {RDFS.Class, OWL_CLASS}
PROP_TYPES = {RDF_PROP, OWL_OBJ_PROP, OWL_DATA_PROP, OWL_ANN_PROP}

CONNECTION_POINT = S223.hasConnectionPoint

# Direction-aware topology predicates
CONNECTED_THROUGH = S223.connectedThrough
CONNECTS_TO = S223.connectsTo
CONNECTS_FROM = S223.connectsFrom
