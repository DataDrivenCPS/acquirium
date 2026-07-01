"""Fuzzy term resolution — let people query by name, not by URI.

Acquirium already runs a server-side embedding matcher (``/resolve_text``,
surfaced as :meth:`AcquiriumClient.resolve_concept`). This module plugs it into
Graframe so a natural-language string in a *concept slot* (class or predicate)
is resolved to a URI automatically:

    g.instances("sensor")          # -> s223:Sensor
    sel.pivot("connected to")      # -> s223:connectedTo
    sel.is_a("temperature sensor")

Resolution rule for a slot value:

* a :class:`Fuzzy` marker (from :func:`like`) -> always embedding-resolved;
* a full URI / URIRef -> used as-is;
* a CURIE (contains ``:``) -> expanded against bound prefixes (a bad prefix is
  an error, never fuzzy-guessed);
* anything else (no ``:``) -> treated as natural language and embedding-resolved
  when fuzzy matching is on.

Literal *value* slots (``value=``/``in_=``) stay literal unless wrapped in
:func:`like`, so filtering by a plain string isn't silently turned into a URI.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from rdflib import URIRef

from .algebra import _is_uri

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Fuzzy:
    """A term to resolve via the embedding matcher, optionally pinned to a kind."""

    text: str
    kind: str | None = None


def like(text: str, kind: str | None = None) -> Fuzzy:
    """Wrap a string for fuzzy (embedding) resolution.

    Use in slots that are literal by default, or to pin a ``kind``::

        sel.refine("qudt:hasQuantityKind", value=like("concentration", "quantity_kind"))
    """
    return Fuzzy(text, kind)


def resolve_iri(
    client: Any,
    x: Any,
    *,
    kind: str | None,
    fuzzy: bool,
    min_score: float,
) -> str:
    """Resolve ``x`` (Fuzzy / URIRef / URI / CURIE / natural language) to a URI."""
    if isinstance(x, Fuzzy):
        return _match(client, x.text, x.kind or kind, min_score)
    if isinstance(x, URIRef):
        return str(x)
    s = str(x)
    if _is_uri(s):
        return s
    if ":" in s:
        return client.expand_uri(s)  # CURIE; a bad prefix raises (never guessed)
    if fuzzy:
        return _match(client, s, kind, min_score)
    raise ValueError(
        f"cannot resolve {s!r}: not a URI or bound CURIE (enable fuzzy matching "
        f"or wrap with like())"
    )


def suggest(client: Any, text: str, kind: str | None = None, *, top_k: int = 5) -> list[dict]:
    """Preview embedding matches: ``[{curie, score, kind}, ...]`` best-first."""
    out: list[dict] = []
    for m in client.resolve_text(text, kind=kind, top_k=top_k, min_score=0.0):
        uri = m.get("uri", "")
        try:
            curie = client.compact_uri(uri)
        except Exception:
            curie = uri
        out.append({"curie": curie, "score": float(m.get("score", 0)), "kind": m.get("kind")})
    return out


def _match(client: Any, text: str, kind: str | None, min_score: float) -> str:
    if _is_uri(text):
        return text
    uri = client.resolve_concept(text, kind=kind, min_score=min_score)
    if uri is None:
        raise ValueError(
            f"could not resolve {text!r}"
            + (f" as {kind}" if kind else "")
            + _suggestion_hint(client, text, kind)
        )
    logger.info("graframe: resolved %r%s -> %s", text, f" ({kind})" if kind else "", uri)
    return uri


def _suggestion_hint(client: Any, text: str, kind: str | None) -> str:
    try:
        matches = client.resolve_text(text, kind=kind, top_k=3, min_score=0.0)
    except Exception:
        return ""
    if not matches:
        return ""
    parts = []
    for m in matches[:3]:
        uri = m.get("uri", "")
        try:
            uri = client.compact_uri(uri)
        except Exception:
            pass
        parts.append(f"{uri} ({float(m.get('score', 0)):.2f})")
    return "; closest: " + ", ".join(parts)
