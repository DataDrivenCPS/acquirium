"""Unit tests for model compilation (SHACL-AF inference).

``compile_graph`` is the pure pyshifty bridge; ``OxigraphGraphStore._compile_graph``
is the storage hook that must fall back to the un-inferred graph if inference
raises (so a bad rule can't break every query).
"""

from rdflib import Graph, Namespace, RDF

from acquirium.internals.inference import compile_graph

EX = Namespace("http://example.org/")

# A model carrying its own SHACL-AF rule: every ex:Thing gets an ex:derived edge.
RULE_MODEL = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:ThingShape a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:rule [ a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:derived ;
        sh:object true ] .

ex:a a ex:Thing .
"""


def _has_derived(g: Graph) -> bool:
    return any(str(p) == str(EX.derived) for _, p, _ in g)


def test_compile_graph_materializes_shacl_af_rule():
    g = compile_graph(RULE_MODEL)
    assert _has_derived(g)                       # rule fired
    assert (EX.a, RDF.type, EX.Thing) in g       # base triples preserved


def test_compile_graph_accepts_rdflib_graph():
    base = Graph().parse(data=RULE_MODEL, format="turtle")
    out = compile_graph(base)
    assert len(out) > len(base)
    assert _has_derived(out)


def test_compile_graph_without_rules_is_noop():
    plain = "@prefix ex: <http://example.org/> .\nex:a a ex:Thing .\n"
    g = compile_graph(plain)
    assert not _has_derived(g)
    assert (EX.a, RDF.type, EX.Thing) in g


def test_store_compile_falls_back_on_inference_error(monkeypatch):
    from acquirium.Storage import graph_store as gs
    import acquirium.internals.inference as inf

    def boom(*a, **k):
        raise RuntimeError("inference exploded")

    monkeypatch.setattr(inf, "compile_graph", boom)
    merged = Graph().parse(data=RULE_MODEL, format="turtle")
    # _compile_graph only touches the module logger, so it's callable directly.
    out = gs.OxigraphGraphStore._compile_graph(object(), merged)
    assert out is merged  # fell back to the un-inferred graph, no exception
