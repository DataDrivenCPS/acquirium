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
from typing import Iterable
from decimal import Decimal, getcontext

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SKOS, XSD

from acquirium.internals.internals_namespaces import QUDT, UNIT, QUDT_QUANTITY_KIND
from acquirium.TextMatch.unit_key import UnitKeyIndex


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
    dimension_vector: URIRef | None = None
    quantity_kinds: tuple[URIRef, ...] = ()

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

        quantity_kinds = list(graph.objects(uri, QUDT.QuantityKind))
        quantity_kinds += list(graph.objects(uri, QUDT.hasQuantityKind))
        # Deduplicate while preserving order
        seen_qk: set[URIRef] = set()
        unique_qks: list[URIRef] = []
        for qk in quantity_kinds:
            if isinstance(qk, URIRef) and qk not in seen_qk:
                seen_qk.add(qk)
                unique_qks.append(qk)

        # Prefer the canonical Length quantity kind when multiple exist (common in QUDT dumps)
        preferred = QUDT_QUANTITY_KIND.Length
        if preferred in unique_qks:
            quantity_kind = preferred
        else:
            quantity_kind = unique_qks[0] if unique_qks else None

        # Dimension vector for robust compatibility checking
        dim_vec = next(graph.objects(uri, QUDT.hasDimensionVector), None)

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
            dimension_vector=dim_vec if isinstance(dim_vec, URIRef) else None,
            quantity_kinds=tuple(unique_qks),
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
        # Lookup tables over the graph, built on first use.
        self._key_index: UnitKeyIndex | None = None
        self._literal_index: tuple[dict[str, list[URIRef]], dict[str, list[URIRef]]] | None = None

    # -------------------- public API --------------------
    def resolve_unit(self, identifier: str | URIRef) -> UnitDefinition:
        """Resolve a unit identifier into a :class:`UnitDefinition`.

        Resolution order (first match wins):
        1. Exact URI provided (and present in graph).
        2. URI whose local name is the identifier as typed ("M" -> UNIT.M).
        3. The unit expression, through :class:`UnitKeyIndex`: any spelling of
           the symbol, UCUM code or local name ("m3/h", "m³·h⁻¹", "hrs"), with
           its tie rules ("h" is the hour, "gal" the US gallon).
        4. URI whose local name is the identifier upper-cased ("l-per-min").
        5. A common alias ("gallon", "celsius").
        6. Label, symbol, UCUM or UN/ECE code equality, case-sensitive and
           then case-folded.

        Nothing here is fuzzy: a text is a unit's name or code, or it is not
        found. The caller can always disambiguate by passing the full URI.

        Example::

            # turbidimeter unit symbol off a plant tag
            resolve_unit("NTU")   # or "NTU"/the full UNIT.NTU URI
            # -> UnitDefinition(uri=UNIT.NTU, label="Nephelometric Turbidity"
            #                    " Unit", symbol="NTU", multiplier=1.0,
            #                    offset=0.0, ...)
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

        # 3) The typed expression. Ahead of the upper-cased local name, which
        # reads "h" as the henry and "s" as the siemens.
        hits = self._unit_keys().lookup(identifier)
        if hits:
            return self._from_uri(URIRef(hits[0][0]["uri"]))

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

        # 6) Literal equality across the labeling predicates: case-sensitive
        # first, then case-folded.
        exact, folded = self._literals()
        for subject in (*exact.get(identifier, ()), *folded.get(identifier.casefold(), ())):
            return self._from_uri(subject)

        raise UnitNotFound(f"Unit '{identifier}' not found in provided QUDT graph")

    def are_compatible(self, unit_a: str | URIRef, unit_b: str | URIRef) -> bool:
        """Check if two units are compatible for conversion.

        Uses dimension vectors (most reliable), then falls back to
        quantity kind overlap.
        """
        return self._check_compatible(self.infer_unit(str(unit_a)), self.infer_unit(str(unit_b)))

    @staticmethod
    def _check_compatible(src: UnitDefinition, tgt: UnitDefinition) -> bool:
        """Check compatibility using dimension vectors first, then quantity kind overlap."""
        # Dimension vector is the most reliable check
        if src.dimension_vector and tgt.dimension_vector:
            return src.dimension_vector == tgt.dimension_vector
        # Fall back to quantity kind overlap
        if src.quantity_kinds and tgt.quantity_kinds:
            return bool(set(src.quantity_kinds) & set(tgt.quantity_kinds))
        # If only one has a single quantity_kind, check that
        if src.quantity_kind and tgt.quantity_kind:
            return src.quantity_kind == tgt.quantity_kind
        # If we can't determine, allow the conversion (may fail at math level)
        return True

    def convert(self, value: float, from_unit: str | URIRef, to_unit: str | URIRef) -> float:
        """Convert ``value`` from ``from_unit`` into ``to_unit``.

        Uses dimension vectors for compatibility checking (handles cases where
        units share the same physical dimension but different quantity kind
        labels, e.g., L has LiquidVolume, MilliL has Volume).
        Raises :class:`IncompatibleUnits` when units are incompatible.
        """

        # Never a substring guess: a wrong unit here silently rescales data.
        src = self.infer_unit(str(from_unit))
        tgt = self.infer_unit(str(to_unit))

        if not self._check_compatible(src, tgt):
            raise IncompatibleUnits(
                f"Cannot convert {src.uri} ({src.dimension_vector or src.quantity_kind}) "
                f"to {tgt.uri} ({tgt.dimension_vector or tgt.quantity_kind})"
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

    def infer_unit(self, text: str, *, fuzzy: bool = False) -> UnitDefinition:
        """Best-effort unit inference from an arbitrary string.

        Heuristics (ordered):
        - direct :meth:`resolve_unit` call (handles symbols, UCUM codes, and ratio notation like "mg/L").
        - if the string looks like a URI, try its fragment or last path segment.
        - a ratio such as "gal/min" or "gallon per minute", resolved part by part.
        - with ``fuzzy``, a substring search over labels and symbols.

        The substring search returns the first unit whose label merely
        contains the text ("watts" is inside "Terawatt Hour per Year"). It is
        off by default, and nothing in this module turns it on.

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

        # Only for a URI: in a unit, "/" is a division ("m3/h" is not "h").
        if "/" in text and self._looks_like_uri(text):
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
                        num = self._search_label_contains(left) if fuzzy else None
                    try:
                        den = self.resolve_unit(right)
                    except UnitNotFound:
                        den = self._search_label_contains(right) if fuzzy else None

                    if isinstance(num, URIRef):
                        num = self._from_uri(num)
                    if isinstance(den, URIRef):
                        den = self._from_uri(den)

                    if num is None:
                        try:
                            num = self.infer_unit(left, fuzzy=fuzzy)
                        except UnitNotFound:
                            num = None

                    if den is None:
                        try:
                            den = self.infer_unit(right, fuzzy=fuzzy)
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
        if fuzzy:
            unit_def = self._search_label_contains(text)
            if unit_def is not None:
                return unit_def

        # Nothing matched
        raise UnitNotFound(f"Could not infer unit from '{text}'")

    # -------------------- internal helpers --------------------
    @staticmethod
    def _looks_like_uri(text: str) -> bool:
        return "://" in text or text.startswith("urn:")

    def _looks_like_unit(self, subject: URIRef) -> bool:
        return (subject, RDF.type, QUDT.Unit) in self.graph or (subject, QUDT.conversionMultiplier, None) in self.graph

    def _unit_keys(self) -> UnitKeyIndex:
        """Units by typed expression, built once from the graph."""
        if self._key_index is None:
            units = [
                {
                    "uri": str(unit),
                    "symbol": next((str(o) for o in self.graph.objects(unit, QUDT.symbol)), None),
                    "ucum": next((str(o) for o in self.graph.objects(unit, QUDT.ucumCode)), None),
                    "related": [str(o) for o in self.graph.objects(unit, QUDT.hasQuantityKind)],
                }
                for unit in sorted(set(self.graph.subjects(RDF.type, QUDT.Unit)))
            ]
            self._key_index = UnitKeyIndex(units)
        return self._key_index

    def _literals(self) -> tuple[dict[str, list[URIRef]], dict[str, list[URIRef]]]:
        """``(by text, by case-folded text)`` over the labeling predicates, built once.

        Predicate order is the precedence: a label before a symbol before a
        UCUM code. Only plain (untagged) and English literals of units.
        """
        if self._literal_index is None:
            exact: dict[str, list[URIRef]] = {}
            folded: dict[str, list[URIRef]] = {}
            for predicate in (RDFS.label, SKOS.prefLabel, QUDT.symbol, QUDT.ucumCode, QUDT.uneceCommonCode):
                triples = self.graph.triples((None, predicate, None))
                for subj, _, lit in sorted(triples, key=lambda t: (str(t[0]), str(t[2]))):
                    if not isinstance(lit, Literal) or not isinstance(subj, URIRef):
                        continue
                    if lit.language and not lit.language.startswith("en"):
                        continue
                    if not self._looks_like_unit(subj):
                        continue
                    for table, key in ((exact, str(lit)), (folded, str(lit).casefold())):
                        bucket = table.setdefault(key, [])
                        if subj not in bucket:
                            bucket.append(subj)
            self._literal_index = (exact, folded)
        return self._literal_index

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
                dimension_vector=unit.dimension_vector,
                quantity_kinds=unit.quantity_kinds,
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
                dimension_vector=unit.dimension_vector,
                quantity_kinds=unit.quantity_kinds,
            )
        except Exception:
            return unit
