import json

with open("ontologies/lexicon.json", "r") as f:
    lexicon = json.load(f)

from acquirium.TextMatch.matcher import ConceptMatcher

matcher = ConceptMatcher(lexicon)

results = matcher.match("kilogram per sec", restrict_kinds={"class"}, top_k=5)
print("Top matches for 'kilogram per sec':")
for res in results:
    print(f"- {res.uri} ({res.label}) [score: {res.score:.3f}] matched on '{res.matched_surface}'")