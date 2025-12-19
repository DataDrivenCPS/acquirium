from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, TYPE_CHECKING

import json

from .matcher import ConceptMatcher
if TYPE_CHECKING:
    from acquirium.Client.client import AcquiriumClient



@dataclass
class TextMatchService:
    lexicon_path: Path
    lexicon: Dict[str, Any]
    matcher: ConceptMatcher

    @classmethod
    def from_client(
        cls,
        client: "AcquiriumClient",
        lexicon_path: Path,
    ) -> "TextMatchService":
        lexicon_path = Path(lexicon_path)
        if not lexicon_path or not lexicon_path.is_file():
            print("No lexicon file provided, TextMatchService will not be able to resolve terms.")
            return cls(
                lexicon_path=lexicon_path,
                lexicon=None,
                matcher=None,
            )

        with open(lexicon_path, "r", encoding="utf-8") as f:
            lex = json.load(f)

        matcher = ConceptMatcher(lex)
        return cls(lexicon_path=lexicon_path, lexicon=lex, matcher=matcher)

def make_resolver_from_service(service: TextMatchService):
    """
    Returns a resolver(text, kind) -> uri, for decorator usage.
    """
    def no_lexicon_resolver(text: str, kind: str) -> str:
        return text

    def resolver(text: str, kind: str) -> str:
        restrict = None
        if kind == "class":
            restrict = {"class"}
        elif kind == "predicate":
            restrict = {"predicate"}

        matches = service.matcher.match(text, restrict_kinds=restrict, top_k=1, min_score=0.55)
        if not matches:
            raise ValueError(f"Could not resolve '{text}' as {kind}")
        return matches[0].uri

    if service.matcher is None:
        return no_lexicon_resolver
    return resolver
