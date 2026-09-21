"""Look units up by the expression the user typed.

A QUDT symbol ("m³/h", "Btu{IT}/(ft²·s)") and a UCUM code ("m3.h-1",
"[Btu_IT].[ft_i]-2.s-1") are two spellings of one thing: a product of unit
atoms with integer exponents. :func:`unit_key` reduces any such spelling to a
single string, so typed input is verified by lookup and unit symbols never
need an embedding::

    "m3/h", "m^3/h", "m³/h", "m3 / h", "m3.h-1", "m³·h⁻¹", "m3 per h"
        -> "h^-1|m^3"

Atoms are not translated ("gal" stays "gal", "[gal_us]" stays "[gal_us]").
:class:`UnitKeyIndex` files each unit under the key of its symbol and under
the key of its UCUM code, which covers both spellings, and under the key of
its QUDT local name ("HR", "M3-PER-HR"), which covers "hr", "sec", "lb".
"""

from __future__ import annotations

import re
from typing import Any

_SUPERSCRIPTS = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹⁻⁺", "0123456789-+")
_ANNOTATION = re.compile(r"\{[^}]*\}")  # UCUM: Btu{IT}, gal{US}
_DEGREE = re.compile(r"(?:\bdeg(?:rees?)?\.?|°|º|˚)\s*([CFKR])\b", re.IGNORECASE)
_ATOM = r"A-Za-zµ°"
# One factor: an atom, then an optional signed integer exponent ("m3", "s-1").
# The atom may hold digits as long as it does not end in one ("inH2O").
_FACTOR = re.compile(r"^(?P<atom>[^\d\s./()+\-][^\s./()]*?)(?P<exp>[+-]?\d+)?$")

_MAX_LENGTH = 48


def unit_key(text: str, fold_case: bool = False, singular: bool = False) -> str | None:
    """Canonical key of a unit expression, or ``None`` if *text* is not one.

    Case is kept unless ``fold_case``: "mW" and "MW" are different units.
    ``singular`` drops a plural "s" from each atom ("hrs", "gals/min").
    """
    parsed = _parse(text, fold_case, singular)
    return parsed[0] if parsed else None


def _parse(text: str, fold_case: bool, singular: bool) -> tuple[str, bool] | None:
    """``(key, reduced)``: *reduced* when an atom occurred more than once.

    "A/(A·h)" becomes "h^-1" and "m²/m" becomes "m^1": right for what a user
    typed, but no longer a description of the unit it came from.
    """
    t = text.strip()
    if not t or len(t) > _MAX_LENGTH:
        return None
    t = t.translate(_SUPERSCRIPTS).replace("μ", "µ")  # Greek mu -> micro sign
    t = _ANNOTATION.sub("", t)
    t = _DEGREE.sub(lambda m: "°" + m.group(1).upper(), t)  # "deg C", "ºc" -> "°C"
    t = re.sub(r"\bper\b", "/", t, flags=re.IGNORECASE)
    t = re.sub(r"\*\*|\^", "", t)  # "m^3", "m**3" -> "m3"
    # A hyphen between two atoms multiplies ("kW-h"); elsewhere it is a sign.
    t = re.sub(rf"(?<=[{_ATOM}])-(?=[{_ATOM}])", ".", t)
    t = re.sub(r"[·⋅*×]", ".", t)
    t = re.sub(r"\s*/\s*", "/", t)
    t = re.sub(r"\s+", ".", t.strip())  # "m s-1" -> "m.s-1"

    # "/" divides the next factor, or the next parenthesised group, only:
    # "kg/m.s" is kg.m-1.s (UCUM); QUDT symbols write "J/(kg.K)".
    factors: dict[str, int] = {}
    reduced = False
    group_sign, stack, divide = 1, [], False
    for token in re.findall(r"[./()]|[^./()]+", t):
        if token == "/":
            divide = True
        elif token == ".":
            continue
        elif token == "(":
            stack.append(group_sign)
            group_sign, divide = (-group_sign if divide else group_sign), False
        elif token == ")":
            if not stack:
                return None
            group_sign = stack.pop()
        else:
            m = _FACTOR.match(token)
            if not m:
                return None
            atom = m.group("atom").casefold() if fold_case else m.group("atom")
            if singular and len(atom) > 2 and atom[-1] in "sS" and atom[-2] not in "sS":
                atom = atom[:-1]
            sign, divide = (-group_sign if divide else group_sign), False
            reduced = reduced or atom in factors
            factors[atom] = factors.get(atom, 0) + sign * int(m.group("exp") or 1)
    if stack:
        return None
    parts = sorted((atom, exp) for atom, exp in factors.items() if exp)
    if not parts:
        return None
    return "|".join(f"{atom}^{exp}" for atom, exp in parts), reduced


def _local_name_expression(uri: str) -> str:
    """A QUDT unit's local name as an expression: "M3-PER-HR" -> "M3/HR"."""
    local = uri.rsplit("/", 1)[-1]
    local = re.sub(r"(^|-)PER-", "/", local)
    return local.replace("-", ".")


def _regional_rank(uri: str) -> int:
    """US customary units first, imperial ones last, among units sharing a key.

    "gal{US}", "gal{UK}" and "gal{Imp}" all read as "gal": it is the US gallon.
    """
    local = uri.rsplit("/", 1)[-1]
    if "_UK" in local or "_IMP" in local:
        return 2
    return 0 if "_US" in local else 1


class UnitKeyIndex:
    """Unit concepts filed under the keys of their symbol and UCUM code.

    Built from the unit concept dicts of ``QUDTStore.extract_concepts``
    (keys used: ``uri``, ``symbol``, ``ucum``, ``related``).
    """

    # Which spelling gave the key; a lower one leads among units sharing a key.
    _SYMBOL, _UCUM, _LOCAL_NAME = 0, 1, 2

    def __init__(self, units: list[dict[str, Any]]) -> None:
        # key -> [(concept, the spelling that gave the key, its source)]
        self._exact: dict[str, list[tuple[dict[str, Any], str, int]]] = {}
        self._folded: dict[str, list[tuple[dict[str, Any], str, int]]] = {}
        for concept in units:
            for field, source in (("symbol", self._SYMBOL), ("ucum", self._UCUM)):
                value = concept.get(field)
                if value:
                    self._file(concept, value, source, (self._exact, False), (self._folded, True))
            # The local name is upper-case by convention and typed in lower
            # case ("hr", "sec", "mi"), so that is its case-exact spelling.
            expression = _local_name_expression(concept["uri"]).lower()
            self._file(concept, expression, self._LOCAL_NAME, (self._exact, False), (self._folded, True))

    @staticmethod
    def _file(concept: dict[str, Any], spelling: str, source: int, *tables: tuple[dict, bool]) -> None:
        for table, fold in tables:
            parsed = _parse(spelling, fold, singular=False)
            # "A/(A·h)" reduces to "h^-1" and would answer "per hour" with a
            # current ratio; "m²/m" reduces to "m". Such a unit is not filed.
            if parsed and not parsed[1]:
                table.setdefault(parsed[0], []).append((concept, spelling, source))

    def __len__(self) -> int:
        return len(self._exact)

    def lookup(self, text: str) -> list[tuple[dict[str, Any], str]]:
        """Units whose symbol, UCUM code or local name spells the same expression as *text*.

        Returns ``(concept, matched spelling)`` pairs, best first: a
        case-exact reading before a case-folded one ("kg" is not "kG"), an
        unqualified spelling before an annotated one ("day" before
        "day{sidereal}", "ft" before "ft{US Survey}", "mi" before "mi{US}"), US
        customary before imperial units, a symbol before a UCUM code before a
        local name, and the unit with more quantity kinds.

        A text with no unit as written is read once more with plural atoms
        made singular ("hrs", "gals/min"). "ms" and "Gs" are units as written,
        so they never get that far.
        """
        return self._lookup(text, singular=False) or self._lookup(text, singular=True)

    def _lookup(self, text: str, singular: bool) -> list[tuple[dict[str, Any], str]]:
        found: list[tuple[int, tuple[dict[str, Any], str, int]]] = []
        for case_rank, (table, fold) in enumerate(((self._exact, False), (self._folded, True))):
            key = unit_key(text, fold_case=fold, singular=singular)
            if key is None:
                return []
            found += [(case_rank, entry) for entry in table.get(key, ())]
        found.sort(
            key=lambda f: (
                f[0],
                "{" in f[1][1],  # "day{sidereal}" is a qualified day: after a plain one
                _regional_rank(f[1][0]["uri"]),
                f[1][2],
                -len(f[1][0].get("related", ())),
                f[1][0]["uri"],
            )
        )
        out: list[tuple[dict[str, Any], str]] = []
        seen: set[str] = set()
        for _, (concept, matched, _source) in found:
            if concept["uri"] not in seen:
                seen.add(concept["uri"])
                out.append((concept, matched))
        return out
