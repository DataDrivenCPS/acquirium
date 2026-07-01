"""SPARQL term / property-path / pattern algebra for the facet query interface.

This module is the *denotational core* of Graframe. Everything the fluent API
does is ultimately a transformation of a small, immutable pattern algebra that
renders to a well-defined fragment of SPARQL (conjunctive queries + property
paths + filters). Keeping this layer tiny and pure is what lets us reason about
correctness: a Selection compiles to exactly the query these objects render.

Three layers:

* **Terms** (:class:`Var`, :class:`Iri`, :class:`Lit`) — the things that appear
  in subject / object position.
* **Paths** (:class:`Pred`, :class:`Inv`, :class:`Seq`, :class:`Alt`,
  :class:`Mod`, :class:`RawPath`) — SPARQL property paths, i.e. the "virtual
  edges" of the design. An atomic predicate is just :class:`Pred`.
* **Patterns** (:class:`Triple`, :class:`Values`, :class:`Cmp`,
  :class:`Exists`, :class:`OrExists`) — graph patterns joined conjunctively in a
  ``WHERE`` block.

Every pattern supports :meth:`render` (to SPARQL) and :meth:`rename` (variable
substitution), the latter powering correlation / membership joins where one
Selection is inlined into another.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Mapping

# Well-known IRIs used by the reasoning profile.
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"


def _is_uri(text: Any) -> bool:
    return isinstance(text, str) and (
        text.startswith("urn:")
        or text.startswith("http://")
        or text.startswith("https://")
    )


# ---------------------------------------------------------------------------
# Terms
# ---------------------------------------------------------------------------


class Term:
    """A SPARQL term (variable, IRI, or literal)."""

    def render(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def rename(self, mapping: Mapping[str, str]) -> "Term":
        return self


@dataclass(frozen=True)
class Var(Term):
    name: str

    def render(self) -> str:
        return f"?{self.name}"

    def rename(self, mapping: Mapping[str, str]) -> "Var":
        return Var(mapping.get(self.name, self.name))


@dataclass(frozen=True)
class Iri(Term):
    uri: str

    def render(self) -> str:
        return f"<{self.uri}>"


@dataclass(frozen=True)
class Lit(Term):
    value: Any
    datatype: str | None = None

    def render(self) -> str:
        v = self.value
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, (datetime, date)):
            return f'"{v.isoformat()}"^^<{XSD_DATETIME}>'
        s = str(v)
        escaped = (
            s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        )
        if self.datatype:
            return f'"{escaped}"^^<{self.datatype}>'
        return f'"{escaped}"'


def term(x: Any) -> Term:
    """Coerce a Python value to a :class:`Term`.

    URIs / :class:`Iri` become IRIs; everything else becomes a literal. CURIE
    expansion happens *before* this call (Graframe resolves prefixes against the
    server), so a bare ``prefix:local`` here is treated as a literal.
    """
    if isinstance(x, Term):
        return x
    if _is_uri(x):
        return Iri(str(x))
    return Lit(x)


# ---------------------------------------------------------------------------
# Property paths
# ---------------------------------------------------------------------------


class Path:
    """A SPARQL property path — the general form of an edge ("virtual edge")."""

    def render(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    # fluent combinators -------------------------------------------------
    def inverse(self) -> "Path":
        return Inv(self)

    def then(self, other: "Path") -> "Path":
        return Seq(self, other)

    def or_(self, other: "Path") -> "Path":
        return Alt(self, other)

    def plus(self) -> "Path":
        return Mod(self, "+")

    def star(self) -> "Path":
        return Mod(self, "*")

    def opt(self) -> "Path":
        return Mod(self, "?")


@dataclass(frozen=True)
class Pred(Path):
    uri: str

    def render(self) -> str:
        return f"<{self.uri}>"


@dataclass(frozen=True)
class Inv(Path):
    p: Path

    def render(self) -> str:
        return f"^{_wrap(self.p)}"


@dataclass(frozen=True)
class Seq(Path):
    a: Path
    b: Path

    def render(self) -> str:
        return f"{_wrap(self.a)}/{_wrap(self.b)}"


@dataclass(frozen=True)
class Alt(Path):
    a: Path
    b: Path

    def render(self) -> str:
        return f"{_wrap(self.a)}|{_wrap(self.b)}"


@dataclass(frozen=True)
class Mod(Path):
    p: Path
    mod: str  # one of '*', '+', '?'

    def render(self) -> str:
        return f"{_wrap(self.p)}{self.mod}"


@dataclass(frozen=True)
class RawPath(Path):
    """An already-rendered path fragment (used for the rdf:type reasoning path)."""

    text: str

    def render(self) -> str:
        return self.text


def _wrap(p: Path) -> str:
    """Parenthesize composite paths so precedence is unambiguous."""
    if isinstance(p, (Pred, RawPath)):
        return p.render()
    return f"({p.render()})"


def alt_of(paths: list[Path]) -> Path:
    """Fold a list of paths into a single alternation (``a|b|c``)."""
    if not paths:
        raise ValueError("alt_of: empty path list")
    acc = paths[0]
    for p in paths[1:]:
        acc = Alt(acc, p)
    return acc


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------


class Pattern:
    """A graph pattern that renders to one line of a SPARQL ``WHERE`` block."""

    def render(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def rename(self, mapping: Mapping[str, str]) -> "Pattern":  # pragma: no cover
        raise NotImplementedError

    def vars(self) -> set[str]:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True)
class Triple(Pattern):
    subj: Term
    path: Path
    obj: Term

    def render(self) -> str:
        return f"{self.subj.render()} {self.path.render()} {self.obj.render()} ."

    def rename(self, mapping: Mapping[str, str]) -> "Triple":
        return Triple(self.subj.rename(mapping), self.path, self.obj.rename(mapping))

    def vars(self) -> set[str]:
        return _term_vars(self.subj) | _term_vars(self.obj)


@dataclass(frozen=True)
class Values(Pattern):
    var: Var
    terms: tuple[Term, ...]

    def render(self) -> str:
        vals = " ".join(t.render() for t in self.terms)
        return f"VALUES {self.var.render()} {{ {vals} }}"

    def rename(self, mapping: Mapping[str, str]) -> "Values":
        return Values(self.var.rename(mapping), self.terms)

    def vars(self) -> set[str]:
        return {self.var.name}


@dataclass(frozen=True)
class Cmp(Pattern):
    """A scalar ``FILTER`` comparison, e.g. ``FILTER(?o >= 5)``."""

    var: Var
    op: str  # one of =, !=, <, <=, >, >=
    value: Term

    def render(self) -> str:
        return f"FILTER({self.var.render()} {self.op} {self.value.render()})"

    def rename(self, mapping: Mapping[str, str]) -> "Cmp":
        return Cmp(self.var.rename(mapping), self.op, self.value.rename(mapping))

    def vars(self) -> set[str]:
        return {self.var.name}


@dataclass(frozen=True)
class Exists(Pattern):
    patterns: tuple[Pattern, ...]
    negated: bool = False

    def render(self) -> str:
        body = " ".join(p.render() for p in self.patterns)
        kw = "FILTER NOT EXISTS" if self.negated else "FILTER EXISTS"
        return f"{kw} {{ {body} }}"

    def rename(self, mapping: Mapping[str, str]) -> "Exists":
        return Exists(tuple(p.rename(mapping) for p in self.patterns), self.negated)

    def vars(self) -> set[str]:
        out: set[str] = set()
        for p in self.patterns:
            out |= p.vars()
        return out


@dataclass(frozen=True)
class OrExists(Pattern):
    """Disjunction of existential branches: ``FILTER(EXISTS{..} || EXISTS{..})``.

    This is how ``any_of`` compiles — non-multiplying disjunctive refinement.
    """

    branches: tuple[tuple[Pattern, ...], ...]

    def render(self) -> str:
        parts = []
        for branch in self.branches:
            body = " ".join(p.render() for p in branch)
            parts.append(f"EXISTS {{ {body} }}")
        return f"FILTER({' || '.join(parts)})"

    def rename(self, mapping: Mapping[str, str]) -> "OrExists":
        return OrExists(
            tuple(tuple(p.rename(mapping) for p in b) for b in self.branches)
        )

    def vars(self) -> set[str]:
        out: set[str] = set()
        for branch in self.branches:
            for p in branch:
                out |= p.vars()
        return out


def _term_vars(t: Term) -> set[str]:
    return {t.name} if isinstance(t, Var) else set()


def patterns_vars(patterns: tuple[Pattern, ...]) -> set[str]:
    out: set[str] = set()
    for p in patterns:
        out |= p.vars()
    return out
