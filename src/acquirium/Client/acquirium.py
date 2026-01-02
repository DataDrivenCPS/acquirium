from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence, Callable, Optional

from rdflib import Graph as RDFGraph

# from acquirium.soft_sensors import SoftSensorRunner
# from acquirium.internals.models import PointCreateRequest, Point, SoftSensorSpec, TimeseriesInfo
from acquirium.Client.query import Query
from acquirium.Client.client import AcquiriumClient

@dataclass
class Acquirium:
    """
    High level entry point for Acquirium.

    This class is intended to be the **user-facing client API**. It connects to the server and exposes
    a small set of convenience methods that should feel natural for end users.

    """


    # ---------- construction ----------
    def __init__(
            self,
            server_url: str = "localhost",
            server_port: int = 8000,
            use_ssl: bool = False,
            lexicon_path: Optional[Path] = None,
        ) -> Acquirium:
        self.client = AcquiriumClient(
            server_url=server_url,
            server_port=server_port,
            use_ssl=use_ssl,
            lexicon_path=lexicon_path,
        )

    # ------------------------------------------------------------------
    # GRAPH API
    # ------------------------------------------------------------------

    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace = True) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: `pathlib.Path` like object, or string. 
            In the case of a string the string it can be either:
                - graph content as text
                - location of the source file
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
        """
        self.client.insert_graph(rdf_graph, format=format, replace=replace)

    def query(self) -> Query:
        """Create a new empty Query bound to this Acquirium instance."""
        return Query(client=self.client)

    def find_entity(self, *, _class: str, alias: Optional[str] = None) -> "Query":
        q = Query(client=self.client).find_entity(_class=_class, alias=alias)
        return q
    
    def find_all_data(self) -> "Query":
        q = Query(client=self.client).find_all_data()
        return q

    # ------------------------------------------------------------------
    # TIMESERIES API
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # SOFT SENSOR API
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # SPARQL / GRAPH UTILITIES
    # ------------------------------------------------------------------

