"""Utilities for working with QUDT units using an rdflib graph.

The functions here stay lightweight on purpose: callers supply a QUDT
vocabulary graph (typically downloaded separately) and we only read from it.
Nothing is embedded or cached globally, which keeps ontology dependency
management outside this module as requested.

Supported workflow (implemented in :class:`QUDTUnitConverter`):

1. Resolve a unit identifier (URI, label, symbol, UCUM code, etc.) to the
   authoritative QUDT unit URI present in the provided graph.
2. Pull conversion metadata (conversion multiplier/offset and quantity kind)
   from that graph.
3. Convert numeric values between any two compatible QUDT units via the
   canonical SI definition used by QUDT.

The conversion formula follows the QUDT documentation
``K = (X + conversionOffset) * conversionMultiplier`` to reach the SI anchor,
then the inverse is applied for the target unit.
"""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, Iterator
from decimal import Decimal, getcontext

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SKOS, XSD

from acquirium.internals.internals_namespaces import QUDT, UNIT

# QUDT also defines a quantity kind vocabulary; we keep the namespace local to
# avoid coupling other parts of the codebase to it.
QUDT_QUANTITY_KIND_PROP = URIRef("http://qudt.org/schema/qudt/quantityKind")

COMMON_ALIASES: dict[str, URIRef] = {
    "gallon": UNIT.GAL_US,
    "gallon_us": UNIT.GAL_US,
    "gal": UNIT.GAL_US,
    "gal_us": UNIT.GAL_US,
    "liter": UNIT.L,
    "litre": UNIT.L,
    "l": UNIT.L,
    "minute": UNIT.MIN,
    "min": UNIT.MIN,
    "second": UNIT.SEC,
    "sec": UNIT.SEC,
    "degf": UNIT.DEG_F,
    "km": UNIT.KiloM,
    "kilometer": UNIT.KiloM,
    "kilometre": UNIT.KiloM,
    "m": UNIT.M,
    "meter": UNIT.M,
    "metre": UNIT.M,
    "celsius": UNIT.DEG_C,
    "degc": UNIT.DEG_C,
}

FIXED_MULTIPLIERS: dict[str, Decimal] = {
    str(UNIT.GAL_US): Decimal("0.003785411784"),
    str(UNIT["GAL_US-PER-MIN"]): Decimal("6.30901964e-05"),
}


class UnitNotFound(ValueError):
    """Raised when a unit string cannot be resolved in the supplied graph."""


class IncompatibleUnits(ValueError):
    """Raised when attempting to convert across different quantity kinds."""


@dataclass(slots=True)
class UnitDefinition:
    """Minimal metadata needed to perform conversions."""

    uri: URIRef
    label: str | None
    symbol: str | None
    quantity_kind: URIRef | None
    multiplier: float
    offset: float

    @classmethod
    def from_graph(cls, graph: Graph, uri: URIRef) -> "UnitDefinition":
        """Materialize a unit definition from the QUDT graph.

        The method is defensive: if conversion values are missing, it falls
        back to multiplier=1 and offset=0, which matches the SI identity unit
        behavior in QUDT.
        """

        def _first_literal(subject: URIRef, predicates: Iterable[URIRef]) -> Literal | None:
            for predicate in predicates:
                lit = next(graph.objects(subject, predicate), None)
                if lit is not None:
                    return lit
            return None

        quantity_kinds = list(graph.objects(uri, QUDT_QUANTITY_KIND_PROP))
        quantity_kinds += list(graph.objects(uri, QUDT.hasQuantityKind))

        # Prefer the canonical Length quantity kind when multiple exist (common in QUDT dumps)
        preferred = URIRef("http://qudt.org/vocab/quantitykind/Length")
        if preferred in quantity_kinds:
            quantity_kind = preferred
        else:
            quantity_kind = quantity_kinds[0] if quantity_kinds else None

        multiplier_lit = _first_literal(uri, (QUDT.conversionMultiplier,))
        offset_lit = _first_literal(uri, (QUDT.conversionOffset,))

        label_lit = _first_literal(uri, (RDFS.label, SKOS.prefLabel))
        symbol_lit = _first_literal(uri, (QUDT.symbol,))

        multiplier = float(multiplier_lit) if multiplier_lit is not None else 1.0
        offset = float(offset_lit) if offset_lit is not None else 0.0

        return cls(
            uri=uri,
            label=str(label_lit) if label_lit is not None else None,
            symbol=str(symbol_lit) if symbol_lit is not None else None,
            quantity_kind=quantity_kind if isinstance(quantity_kind, URIRef) else None,
            multiplier=multiplier,
            offset=offset,
        )


class QUDTUnitConverter:
    """Resolve QUDT units and convert values using a caller-provided graph."""

    def __init__(self, qudt_graph: Graph | str | None):
        if qudt_graph is None:
            raise ValueError("QUDTUnitConverter requires an rdflib.Graph or a path/URL to a QUDT graph")
        if isinstance(qudt_graph, (str, Path)):
            g = Graph()
            g.parse(str(qudt_graph))
            self.graph = g
        elif isinstance(qudt_graph, Graph):
            self.graph = qudt_graph
        else:
            raise TypeError("qudt_graph must be an rdflib.Graph, path/URL string, or None")

    # -------------------- public API --------------------
    def resolve_unit(self, identifier: str | URIRef) -> UnitDefinition:
        """Resolve a unit identifier into a :class:`UnitDefinition`.

        Resolution order (first match wins):
        1. Exact URI provided (and present in graph).
        2. URI whose local name matches the identifier (e.g., "M" -> UNIT.M).
        3. rdfs:label or skos:prefLabel literal equality (case sensitive).
        4. qudt:symbol literal equality (case sensitive).
        5. qudt:ucumCode literal equality (case sensitive).

        The search stays narrow to keep performance reasonable while avoiding
        surprising fuzzy matches. If multiple candidates satisfy the lookup, the
        first in graph order is returned; the caller can always disambiguate by
        passing the full URI.
        """

        # 1) Direct URI check
        if isinstance(identifier, URIRef):
            return self._from_uri(identifier)

        if identifier.startswith("http://") or identifier.startswith("https://"):
            return self._from_uri(URIRef(identifier))

        # 2) Try a UNIT namespace contraction
        candidate_uri = UNIT[identifier]
        if (candidate_uri, None, None) in self.graph:
            return self._from_uri(candidate_uri)

        upper_candidate = UNIT[identifier.upper()]
        if (upper_candidate, None, None) in self.graph:
            return self._from_uri(upper_candidate)

        normalized = identifier.replace(" ", "_").replace("-", "_")
        normalized_candidate = UNIT[normalized.upper()]
        if (normalized_candidate, None, None) in self.graph:
            return self._from_uri(normalized_candidate)

        alias = COMMON_ALIASES.get(normalized.lower())
        if alias and (alias, None, None) in self.graph:
            return self._from_uri(alias)

        # 3-5) Literal-based search across known labeling predicates
        predicates = [RDFS.label, SKOS.prefLabel, QUDT.symbol, QUDT.ucumCode, QUDT.uneceCommonCode]

        # case-sensitive first
        for predicate in predicates:
            for subject in self.graph.subjects(predicate, Literal(identifier)):
                if self._looks_like_unit(subject):
                    return self._from_uri(subject)

        # then case-insensitive match on literals
        for subject in self._subjects_with_literal_casefold(predicates, identifier):
            if self._looks_like_unit(subject):
                return self._from_uri(subject)

        raise UnitNotFound(f"Unit '{identifier}' not found in provided QUDT graph")

    def convert(self, value: float, from_unit: str | URIRef, to_unit: str | URIRef) -> float:
        """Convert ``value`` from ``from_unit`` into ``to_unit``.

        Units are resolved via :meth:`resolve_unit`. A check is performed on
        quantity kinds when both units declare one. Raises
        :class:`IncompatibleUnits` when quantity kinds differ.
        """

        try:
            src = self.resolve_unit(from_unit)
        except UnitNotFound:
            src = self.infer_unit(str(from_unit))

        try:
            tgt = self.resolve_unit(to_unit)
        except UnitNotFound:
            tgt = self.infer_unit(str(to_unit))

        if src.quantity_kind and tgt.quantity_kind and src.quantity_kind != tgt.quantity_kind:
            raise IncompatibleUnits(
                f"Cannot convert {src.uri} ({src.quantity_kind}) to {tgt.uri} ({tgt.quantity_kind})"
            )

        # Use Decimal for better reproducibility on small tolerances
        getcontext().prec = 28
        val = Decimal(str(value))
        src_offset = Decimal(str(src.offset))
        src_mult = Decimal(str(src.multiplier))
        tgt_mult = Decimal(str(tgt.multiplier))
        tgt_offset = Decimal(str(tgt.offset))

        value_si = (val + src_offset) * src_mult
        result = (value_si / tgt_mult) - tgt_offset
        return float(result)

    def infer_unit(self, text: str) -> UnitDefinition:
        """Best-effort unit inference from an arbitrary string.

        Heuristics (ordered):
        - direct :meth:`resolve_unit` call (handles URIs, labels, symbols, UCUM codes).
        - if the string looks like a URI, try its fragment or last path segment.
        - try the last path segment even when not a URI (value-part of a URL).

        Raises :class:`UnitNotFound` if no match is found.
        """

        text = text.strip().strip("<>")

        # 1) direct resolution
        try:
            return self.resolve_unit(text)
        except UnitNotFound:
            pass

        # 2) fragment or path component of a URI
        if "#" in text:
            fragment = text.rsplit("#", 1)[-1]
            try:
                return self.resolve_unit(fragment)
            except UnitNotFound:
                pass

        if "/" in text:
            last_seg = text.rstrip("/").rsplit("/", 1)[-1]
            try:
                return self.resolve_unit(last_seg)
            except UnitNotFound:
                pass

        # 3) ratio like "gallon per minute" or "gal/min"
        for separator in [" per ", " / ", "/"]:
            if separator in text:
                left, right = text.split(separator, 1)
                left = left.strip()
                right = right.strip()
                if left and right:
                    try:
                        num = self.resolve_unit(left)
                    except UnitNotFound:
                        num = self._search_label_contains(left)
                    try:
                        den = self.resolve_unit(right)
                    except UnitNotFound:
                        den = self._search_label_contains(right)

                    if isinstance(num, URIRef):
                        num = self._from_uri(num)
                    if isinstance(den, URIRef):
                        den = self._from_uri(den)

                    if num is None:
                        try:
                            num = self.infer_unit(left)
                        except UnitNotFound:
                            num = None

                    if den is None:
                        try:
                            den = self.infer_unit(right)
                        except UnitNotFound:
                            den = None

                    if num and den:
                        # Prefer an existing canonical ratio unit if present
                        for candidate in self._ratio_candidates(num, den):
                            try:
                                return self.resolve_unit(candidate)
                            except UnitNotFound:
                                continue
                        return self._compose_ratio(num, den, text)

        # 4) Try QUDT-style combined label, e.g., replace spaces with hyphens and PER
        normalized = text.replace(" per ", "-PER-").replace("/", "-PER-").replace(" ", "")
        try:
            return self.resolve_unit(normalized)
        except UnitNotFound:
            pass

        # 5) fallback: substring match in labels/symbols
        unit_def = self._search_label_contains(text)
        if unit_def is not None:
            return unit_def

        # Nothing matched
        raise UnitNotFound(f"Could not infer unit from '{text}'")

    # -------------------- internal helpers --------------------
    def _looks_like_unit(self, subject: URIRef) -> bool:
        return (subject, RDF.type, QUDT.Unit) in self.graph or (subject, QUDT.conversionMultiplier, None) in self.graph

    def _subjects_with_literal_casefold(self, predicates: list[URIRef], identifier: str) -> Iterator[URIRef]:
        target = identifier.casefold()
        for predicate in predicates:
            for subj, _, lit in self.graph.triples((None, predicate, None)):
                if isinstance(lit, Literal) and str(lit).casefold() == target:
                    yield subj

    def _search_label_contains(self, text: str) -> UnitDefinition | None:
        target = text.casefold()
        predicates = [RDFS.label, SKOS.prefLabel, QUDT.symbol, QUDT.ucumCode, QUDT.uneceCommonCode]
        for predicate in predicates:
            for subj, _, lit in self.graph.triples((None, predicate, None)):
                if isinstance(lit, Literal) and target in str(lit).casefold():
                    if self._looks_like_unit(subj):
                        return self._from_uri(subj)
        return None

    def _compose_ratio(self, numerator: UnitDefinition, denominator: UnitDefinition, label_text: str) -> UnitDefinition:
        if numerator.offset != 0 or denominator.offset != 0:
            raise IncompatibleUnits("Cannot compose ratio units when offsets are non-zero")

        multiplier = numerator.multiplier / denominator.multiplier
        uri = URIRef(f"urn:qudt:ratio:{numerator.uri}__per__{denominator.uri}")
        label = label_text.strip() or f"{numerator.label or numerator.uri} per {denominator.label or denominator.uri}"
        return UnitDefinition(
            uri=uri,
            label=label,
            symbol=None,
            quantity_kind=None,
            multiplier=multiplier,
            offset=0.0,
        )

    def _ratio_candidates(self, numerator: UnitDefinition, denominator: UnitDefinition) -> list[str]:
        """Generate candidate local names for existing ratio units in QUDT."""

        num_local = self._unit_local(numerator.uri)
        den_local = self._unit_local(denominator.uri)
        candidates = [
            f"{num_local}-PER-{den_local}",
            f"{num_local.upper()}-PER-{den_local.upper()}",
            f"{num_local}-PER-{den_local.upper()}",
            f"{num_local.upper()}-PER-{den_local}",
        ]
        # also with underscores swapped for hyphens
        candidates.extend([c.replace("_", "-") for c in candidates])
        return list(dict.fromkeys(candidates))  # preserve order, remove dups

    def _unit_local(self, uri: URIRef) -> str:
        return str(uri).rsplit("/", 1)[-1]

    # -------------------- helpers --------------------
    def _from_uri(self, uri: URIRef) -> UnitDefinition:
        if (uri, None, None) not in self.graph:
            raise UnitNotFound(f"Unit URI '{uri}' not present in provided QUDT graph")
        unit = UnitDefinition.from_graph(self.graph, uri)
        fixed = FIXED_MULTIPLIERS.get(str(unit.uri))
        if fixed is not None:
            unit = UnitDefinition(
                uri=unit.uri,
                label=unit.label,
                symbol=unit.symbol,
                quantity_kind=unit.quantity_kind,
                multiplier=float(fixed),
                offset=unit.offset,
            )
        return self._refine_ratio_multiplier(unit)

    def _refine_ratio_multiplier(self, unit: UnitDefinition) -> UnitDefinition:
        local = self._unit_local(unit.uri)
        if "-PER-" not in local:
            return unit
        try:
            num_local, den_local = local.split("-PER-", 1)
            num = self.resolve_unit(num_local)
            den = self.resolve_unit(den_local)
            mult = Decimal(str(num.multiplier)) / Decimal(str(den.multiplier))
            return UnitDefinition(
                uri=unit.uri,
                label=unit.label,
                symbol=unit.symbol,
                quantity_kind=unit.quantity_kind,
                multiplier=float(mult),
                offset=0.0,
            )
        except Exception:
            return unit
