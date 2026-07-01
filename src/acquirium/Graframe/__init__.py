"""Graframe: a fluent, facet-based query interface over RDF graphs.

Typical use (via the Acquirium client)::

    g = aq.graph()                              # Graframe root
    sensors = g.instances("s223:Sensor")        # seed a Selection
    sensors.facets().show()                      # explore the neighbourhood
    temp = sensors.having("s223:hasProperty", is_a="qudt:Temperature")
    props = sensors.follow("s223:hasProperty")   # move the cursor
    temp.nodes()                                 # pull results

See :mod:`acquirium.Graframe.selection` for the operator reference and
:mod:`acquirium.Graframe.algebra` for the SPARQL compilation core.
"""

from .algebra import Alt, Inv, Iri, Lit, Mod, Path, Pred, RawPath, Seq, parse_path, to_path
from .facets import FacetRow, Facets
from .profile import Profile
from .resolve import Fuzzy, like
from .selection import Graframe, Reasoning, Selection


def P(predicate: str) -> Pred:
    """Build an atomic property-path step from a predicate URI/CURIE.

    Compose with :meth:`~acquirium.Graframe.algebra.Path` combinators to make
    virtual edges, e.g. ``P("s223:connectedTo").plus()`` or
    ``P("s223:hasProperty").then(P("qudt:hasUnit"))``. Note: pass the *full URI*
    or resolve CURIEs yourself; combinators do not touch the server.
    """
    return Pred(predicate)


__all__ = [
    "Graframe",
    "Selection",
    "Reasoning",
    "Profile",
    "Fuzzy",
    "like",
    "Facets",
    "FacetRow",
    "Path",
    "Pred",
    "Iri",
    "Lit",
    "Inv",
    "Seq",
    "Alt",
    "Mod",
    "RawPath",
    "parse_path",
    "to_path",
    "P",
]
