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
the key of its UCUM code, which covers both spellings.
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


def unit_key(text: str, fold_case: bool = False) -> str | None:
    """Canonical key of a unit expression, or ``None`` if *text* is not one.

    Case is kept unless ``fold_case``: "mW" and "MW" are different units.
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
            sign, divide = (-group_sign if divide else group_sign), False
            factors[atom] = factors.get(atom, 0) + sign * int(m.group("exp") or 1)
    if stack:
        return None
    parts = sorted((atom, exp) for atom, exp in factors.items() if exp)
    return "|".join(f"{atom}^{exp}" for atom, exp in parts) if parts else None


def _regional_rank(uri: str, matched: str) -> int:
    """US customary units first, imperial and US survey ones last.

    Only orders units that share a key: "gal" is the US gallon, not the
    imperial one, and "ft" is the international foot, not the survey foot.
    """
    local = uri.rsplit("/", 1)[-1]
    if "survey" in matched.lower() or "_UK" in local or "_IMP" in local:
        return 2
    return 0 if "_US" in local else 1


class UnitKeyIndex:
    """Unit concepts filed under the keys of their symbol and UCUM code.

    Built from the unit concept dicts of ``QUDTStore.extract_concepts``
    (keys used: ``uri``, ``symbol``, ``ucum``, ``related``).
    """

    def __init__(self, units: list[dict[str, Any]]) -> None:
        # key -> [(concept, the symbol or code that gave the key, from_symbol)]
        self._exact: dict[str, list[tuple[dict[str, Any], str, bool]]] = {}
        self._folded: dict[str, list[tuple[dict[str, Any], str, bool]]] = {}
        for concept in units:
            for field in ("symbol", "ucum"):
                value = concept.get(field)
                if not value:
                    continue
                entry = (concept, value, field == "symbol")
                for table, fold in ((self._exact, False), (self._folded, True)):
                    key = unit_key(value, fold_case=fold)
                    if key is not None:
                        table.setdefault(key, []).append(entry)

    def __len__(self) -> int:
        return len(self._exact)

    def lookup(self, text: str) -> list[tuple[dict[str, Any], str]]:
        """Units whose symbol or UCUM code spells the same expression as *text*.

        Returns ``(concept, matched symbol or code)`` pairs, best first: a
        case-exact reading before a case-folded one ("kg" is not "kG"), then
        US customary before imperial and US survey units, a symbol before a
        UCUM code, and the unit with more quantity kinds.
        """
        out: list[tuple[dict[str, Any], str]] = []
        seen: set[str] = set()
        for table, fold in ((self._exact, False), (self._folded, True)):
            key = unit_key(text, fold_case=fold)
            if key is None:
                return out
            entries = sorted(
                table.get(key, ()),
                key=lambda e: (
                    _regional_rank(e[0]["uri"], e[1]),
                    not e[2],
                    -len(e[0].get("related", ())),
                    e[0]["uri"],
                ),
            )
            for concept, matched, _ in entries:
                if concept["uri"] not in seen:
                    seen.add(concept["uri"])
                    out.append((concept, matched))
        return out
