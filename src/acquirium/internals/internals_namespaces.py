from rdflib.namespace import Namespace, NamespaceManager
from rdflib import Graph, URIRef, RDF

### ACQUIRIUM INTERNAL NAMESPACES
ACQUIRIUM_NS = Namespace("urn:acquirium#")
ACQUIRIUM_POINT_NS = Namespace("urn:acquirium:point#")
# User metadata attributes (``insert_metadata``): one predicate per leaf path,
# ``attr:product_info.year``. Whole namespace is hidden from generic traversal.
ACQUIRIUM_ATTR_NS = Namespace("urn:acquirium:attr#")

### External Namespaces
QUDT = Namespace("http://qudt.org/schema/qudt/")
QUDT_UNIT = Namespace("http://qudt.org/vocab/unit/")
QUDT_QUANTITY_KIND = Namespace("http://qudt.org/vocab/quantitykind/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
S223 = Namespace("http://data.ashrae.org/standard223#")
G36 = Namespace("http://data.ashrae.org/standard223/1.0/extensions/g36#")
WATR = Namespace("https://watermetadata.org/ontology/watr#")
BRICK = Namespace("https://brickschema.org/schema/Brick#")
BRICK_REF = Namespace("https://brickschema.org/schema/Brick/ref#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")


# Internal predicates for scaffold-only metadata
DEFAULT_MAIN_GRAPH = ACQUIRIUM_NS.MainGraph
VIRTUAL_POINT = ACQUIRIUM_NS.VirtualPoint

# Well-known URI representing the embedded Escapement engine
ACQUIRIUM_DB_URI = ACQUIRIUM_NS.Escapement
DATABASE = ACQUIRIUM_NS.Database


# External-reference vocabulary follows ontologies/ref-schema.ttl
# (https://brickschema.org/schema/Brick/ref#). Predicates that the schema
# does not define live under ACQUIRIUM_NS and are Acquirium-specific.
HAS_EXTERNAL_REFERENCE = BRICK_REF.hasExternalReference
STORED_AT = BRICK_REF.storedAt


# Predicates stored on TimeseriesReference nodes to distinguish
# Acquirium-managed streams (these two are present) from external PG
# references (storedAt is a literal DSN).
ACQUIRIUM_SOURCE_ID  = ACQUIRIUM_NS.sourceId   # the registered datasource name
ACQUIRIUM_REF_NAME   = ACQUIRIUM_NS.refName    # the source-local stream identifier
ACQUIRIUM_VALUE_KIND = ACQUIRIUM_NS.valueKind  # "numeric" or "text"

# Class for registered datasource nodes
ACQUIRIUM_DATASOURCE = ACQUIRIUM_NS.DataSourceRegistry

HAS_MEDIUM = S223.hasMedium
OF_MEDIUM = S223.ofMedium
OF_SUBSTANCE = S223.ofSubstance
HAS_QUANTITY_KIND = QUDT.hasQuantityKind
HAS_ENUMERATION_KIND = QUDT.hasEnumerationKind
HAS_UNIT = QUDT.hasUnit


# Origin tag literal on a point (e.g. "Lab", "SCADA").
DATA_SOURCE = ACQUIRIUM_NS.dataSource

# Name of the app that produced a derived point (e.g. "normalize-temperatures").
# Recorded when an app is deployed, so a query can ask for one app's output.
PRODUCED_BY = ACQUIRIUM_NS.producedBy
# Port and operator group of a derived point, so a downstream app can select
# one output of an upstream app without relying on where it is attached.
OUTPUT_PORT = ACQUIRIUM_NS.outputPort
OUTPUT_GROUP = ACQUIRIUM_NS.outputGroup


# MQTT Reference predicates. timeKey/valueKey are Acquirium-specific (they
# describe how to decode a JSON payload) and live under ACQUIRIUM_NS.
MQTT_REFERENCE = BRICK_REF.MQTTReference
MQTT_BROKER = BRICK_REF.MQTTBroker  # accepts "host", "host:port", or "mqtt(s)://..."
MQTT_TOPIC = BRICK_REF.MQTTTopic
TIME_KEY = ACQUIRIUM_NS.timeKey
VALUE_KEY = ACQUIRIUM_NS.valueKey
HAS_PYOMO_VAR = ACQUIRIUM_NS.hasPyomoVar

OWL_CLASS = URIRef("http://www.w3.org/2002/07/owl#Class")
OWL_OBJ_PROP = URIRef("http://www.w3.org/2002/07/owl#ObjectProperty")
OWL_DATA_PROP = URIRef("http://www.w3.org/2002/07/owl#DatatypeProperty")
RDF_PROP = RDF.Property

CONNECTION_POINT = S223.hasConnectionPoint

# Direction-aware topology predicates
CONNECTED_THROUGH = S223.connectedThrough
CONNECTS_TO = S223.connectsTo
CONNECTS_FROM = S223.connectsFrom


# The prefix acquirium uses for each namespace it owns or ships an ontology
# for. The store seeds its own namespace manager from this one on every open
# and these bindings win: the bundled ontologies are loaded as N-Triples,
# which carries no prefix declarations, so nothing else ever names them.
def _canonical_namespace_manager() -> NamespaceManager:
    graph = Graph(bind_namespaces="none")
    for prefix, namespace in (
        ("acq", ACQUIRIUM_NS),
        ("point", ACQUIRIUM_POINT_NS),
        ("attr", ACQUIRIUM_ATTR_NS),
        ("qudt", QUDT),
        ("unit", QUDT_UNIT),
        ("quantitykind", QUDT_QUANTITY_KIND),
        ("s223", S223),
        ("g36", G36),
        ("watr", WATR),
        ("brick", BRICK),
        ("ref", BRICK_REF),
    ):
        graph.namespace_manager.bind(prefix, namespace, override=True, replace=True)
    return graph.namespace_manager


CANONICAL_NAMESPACES: NamespaceManager = _canonical_namespace_manager()
