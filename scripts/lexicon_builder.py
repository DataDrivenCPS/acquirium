from rdflib import Graph, RDF, RDFS, URIRef, OWL, BNode
# from acquirium.TextMatch.kg_index import build_kg_index, KGIndex
from pathlib import Path
import os
def tokenize(text: str) -> str:
    """
    Simple tokenizer: lowercase, split on whitespace, camelcase, _, and -
    """
    import re

    # 1. Replace separators with space
    text = re.sub(r"[_\-]+", " ", text)

    # 2. Split CamelCase boundaries
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)

    # 3. Extract tokens
    tokens = re.findall(r"[A-Za-z0-9]+", text.lower())

    return " ".join(tokens)

     

def build_lexicon_from_ontology(g: Graph) -> dict:
    """
    Build a lexicon dictionary from the given ontology graph.

    Args:
        g: RDFLib Graph containing the ontology. 
    Returns:
        A dictionary representing the lexicon.
    """
    lexicon: dict[str, dict] = {"concepts": {}}

    all_classes: set[str] = set()
    all_predicates: set[str] = set()
    for s, p, o in g:
        all_predicates.add(str(p))
        if (o == RDFS.Class or o == OWL.Class) and p == RDF.type and not isinstance(s, BNode):
                all_classes.add(str(s))
        if p == RDFS.subClassOf and not isinstance(s, BNode) and not isinstance(o, BNode):
            all_classes.add(str(s))
            all_classes.add(str(o))
        if p == RDF.type and not isinstance(s, BNode) and o in {RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty, OWL.AnnotationProperty}:
            all_classes.add(str(o))
        if p == RDF.type and o == URIRef("http://qudt.org/schema/qudt/Unit") and not isinstance(s, BNode):
            all_classes.add(str(s))
    abbrevs = []
    for uri in all_classes:
        label = None
        surfaces = set()
        for o in g.objects(URIRef(uri), RDFS.label):
            if isinstance(o, str):
                label = o
                surfaces.add(o)
        try:
            without_ns = g.compute_qname(URIRef(uri))[2]
            surfaces.add(without_ns)
            words = tokenize(without_ns)
            try:
                ind_tokens = words.split()
            except:
                ind_tokens = [words]
            if len(ind_tokens) >= 2:
                abbrev = "".join(token[0] for token in ind_tokens)
                abbrevs.append((abbrev, words))
                surfaces.add(abbrev)
        except:
            pass

        lexicon["concepts"][uri] = {
            "kind": "class",
            "label": label,
            "surfaces": list(surfaces)
        }
    for uri in all_predicates:
        label = None
        surfaces = set()
        for o in g.objects(URIRef(uri), RDFS.label):
            if isinstance(o, str):
                label = o
                surfaces.add(o)
        
        without_ns = g.compute_qname(URIRef(uri))[2]
        surfaces.add(without_ns)
        words = tokenize(without_ns)
        surfaces.add(words)

        lexicon["concepts"][uri] = {
            "kind": "predicate",
            "label": label,
            "surfaces": list(surfaces)
        }
    abbrev_dict: dict[str, list[str]] = {}
    for abbr, full in abbrevs:
        if abbr not in abbrev_dict:
            abbrev_dict[abbr] = []
        abbrev_dict[abbr].append(full)
    lexicon["abbrev"] = abbrev_dict 
    return lexicon

def save_lexicon_json(path: Path, lexicon: dict) -> None:
    """
    Save the lexicon dictionary as a JSON file.
    """
    import json

    with path.open("w", encoding="utf-8") as f:
        json.dump(lexicon, f, indent=2, ensure_ascii=False)
    

ontologies = [i for i in os.listdir("ontologies") if i.endswith(".ttl")]
g = Graph(store="Oxigraph")
# for onto in ontologies:
#     g.parse(f"ontologies/{onto}", format="turtle")

g.parse("ontologies/water2.ttl", format="turtle")
g.parse("ontologies/qudt_unit.ttl", format="turtle")
g.parse("ontologies/qudt_qk.ttl", format="turtle")

lex = build_lexicon_from_ontology(g)
save_lexicon_json(Path("ontologies/lexicon.json"), lex)