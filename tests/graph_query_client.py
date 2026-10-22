"""Local query adapter used by graph contract tests."""
from rdflib import Literal, URIRef
from acquirium.Metadata import read_documents

class _GraphQueryClient:
    """Run the shared query builder against the local graph, without HTTP."""

    def __init__(self, graph_store, resolve=None):
        self.graph_store = graph_store
        self._resolve = resolve
        self.base_url = f"experiment-graph:{id(graph_store)}"

    def resolve(self, text, kind=None, min_score=.4):
        return self._resolve(text, kind) if self._resolve is not None else None

    def graph_version(self):
        return self.graph_store.graph_status()["source_version"]

    def metadata_documents(self, subjects):
        return read_documents(self.graph_store, subjects)

    @staticmethod
    def compact_uri(uri):
        # Keep full URIs in saved metadata; native HTTP queries use CURIEs.
        return str(uri)

    def sparql_query(self, query, include_dependencies=False, **kwargs):
        kwargs.setdefault("wait_for_fresh", True)
        result = self.graph_store.sparql_query(query, include_dependencies=include_dependencies, **kwargs)
        if "datatypes" not in result:
            result["datatypes"] = [
                ["iri" if isinstance(value, URIRef) else
                 str(value.datatype) if isinstance(value, Literal) and value.datatype else None
                 for value in row]
                for row in result["rows"]
            ]
        result["rows"] = [[str(value) if value is not None else None for value in row] for row in result["rows"]]
        return result




from acquirium.Storage.metadata_outbox import LocalMetadataGraph


class QueryableLocalMetadataGraph(LocalMetadataGraph):
    """Test-only effective query view over a standalone document backend."""
    version = 0

    def sparql_update(self, update, *, graph_uri):
        super().sparql_update(update, graph_uri=graph_uri)
        self.version += 1

    def graph_status(self):
        return {"source_version": self.version}

    def sparql_query(self, query, **kwargs):
        from rdflib import Graph
        from oxrdflib.store import OxigraphStore
        import pyoxigraph
        from acquirium.Metadata import overlay_documents
        from acquirium.Storage.graph_registry import METADATA_SOURCE_ID, source_graph_uri
        graph = Graph(store=OxigraphStore(store=pyoxigraph.Store()))
        for context in self.dataset.graphs():
            graph += context
        overlay_documents(graph, self.dataset.graph(source_graph_uri(METADATA_SOURCE_ID)))
        result = graph.query(query)
        return {"columns": [str(v) for v in result.vars], "rows": [list(row) for row in result]}
