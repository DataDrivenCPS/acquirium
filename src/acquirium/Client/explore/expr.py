"""Attribute expressions: ``aq.attr.product_info.year >= 2015``.

``aq.attr`` is an :class:`AttrProxy` bound to the connected server. Reading
an attribute off it gives a :class:`Path` (``aq.attr.product_info.year``,
``aq.attr.tags[0]``); a comparison on a path gives an :class:`Expr`, and
expressions combine with ``&``, ``|`` and ``~``. ``Query.where`` takes them
next to the kwarg shorthand::

    q.where(aq.attr.unit == "mg/L")                       # same as where(unit="mg/L")
    q.where(aq.attr.product_info.year >= 2015)
    q.where((aq.attr.product_info.year >= 2015) | (aq.attr.manufacturer == "Siemens"))
    q.where(~aq.attr.last_cleaned.exists())

The proxy validates names against the query's :class:`~attributes.Registry`
(built-ins plus attributes discovered from the graph), so a typo fails on
the ``aq.attr`` line and ``aq.attr.<TAB>`` lists what exists. Expressions
themselves hold only attribute names and plain values, so a query graph
carrying them stays serialisable.

Every comparison means "some value of the attribute satisfies it": a node
with two ``tags`` matches ``aq.attr.tags == "lab"`` when either is ``lab``,
and ``!=`` is the negation of that, as the kwarg ``Not`` is. Python's
``and``/``or``/``not`` cannot be overloaded; an expression in boolean
context raises so the mistake is loud.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

from acquirium.Client.explore.attributes import Registry, check_key, valid_path

COMPARISONS = ("==", "!=", "<", "<=", ">", ">=", "in", "exists")
ORDERING = ("<", "<=", ">", ">=")
_BOOL_HELP = (
    "an attribute expression has no truth value; combine expressions with "
    "& (and), | (or) and ~ (not), each side in parentheses"
)


class Expr:
    """A boolean condition on one node's attributes."""

    def __and__(self, other: "Expr") -> "Expr":
        return BoolOp("and", (self, _expr(other)))

    def __or__(self, other: "Expr") -> "Expr":
        return BoolOp("or", (self, _expr(other)))

    def __invert__(self) -> "Expr":
        return BoolOp("not", (self,))

    def __bool__(self) -> bool:
        raise TypeError(_BOOL_HELP)

    def attributes(self) -> Tuple[str, ...]:
        """Attribute names this expression refers to, in order, no repeats."""
        raise NotImplementedError

    def to_data(self) -> Any:
        """Plain JSON-safe form (``Query.to_dict``)."""
        raise NotImplementedError


def _expr(x: Any) -> Expr:
    if not isinstance(x, Expr):
        raise TypeError(f"expected an attribute expression, got {x!r}; {_BOOL_HELP}")
    return x


@dataclass(frozen=True, eq=False)
class Compare(Expr):
    """``attr <op> value``. ``value`` is None for ``exists``; a list for ``in``."""

    attr: str
    op: str
    value: Any = None

    def attributes(self) -> Tuple[str, ...]:
        return (self.attr,)

    def to_data(self) -> Any:
        return {"attr": self.attr, "op": self.op, "value": self.value}

    def __repr__(self) -> str:
        if self.op == "exists":
            return f"attr.{self.attr}.exists()"
        if self.op == "in":
            return f"attr.{self.attr}.is_in({self.value!r})"
        return f"(attr.{self.attr} {self.op} {self.value!r})"


@dataclass(frozen=True, eq=False)
class BoolOp(Expr):
    """``and`` / ``or`` of two expressions, or ``not`` of one."""

    op: str
    operands: Tuple[Expr, ...]

    def attributes(self) -> Tuple[str, ...]:
        seen: list = []
        for e in self.operands:
            for a in e.attributes():
                if a not in seen:
                    seen.append(a)
        return tuple(seen)

    def to_data(self) -> Any:
        return {"op": self.op, "operands": [e.to_data() for e in self.operands]}

    def __repr__(self) -> str:
        if self.op == "not":
            return f"~{self.operands[0]!r}"
        sym = " & " if self.op == "and" else " | "
        return "(" + sym.join(repr(e) for e in self.operands) + ")"


def _has_prefix(registry: Registry, name: str) -> bool:
    """True when ``name`` is an attribute or the start of a nested one."""
    if name in registry:
        return True
    head = name + "."
    return any(n.startswith(head) for n in registry)


def _next_segments(registry: Registry, prefix: Optional[str]) -> list:
    head = "" if prefix is None else prefix + "."
    out: set = set()
    for n in registry:
        if n.startswith(head):
            seg = n[len(head):].split(".", 1)[0]
            if seg and not seg.isdigit():
                out.add(seg)
    return sorted(out)


class Path:
    """An attribute name under construction: ``aq.attr.product_info.year``.

    Attribute access descends into a nested key, ``[i]`` into a list index.
    Comparison operators and :meth:`is_in` / :meth:`exists` produce
    :class:`Expr`. The registry, when there is one, rejects a name that is
    neither an attribute nor a prefix of one.
    """

    __slots__ = ("_registry", "_name")

    def __init__(self, registry: Optional[Registry], name: str):
        object.__setattr__(self, "_registry", registry)
        object.__setattr__(self, "_name", name)

    @property
    def name(self) -> str:
        return self._name

    def _child(self, segment: str) -> "Path":
        if not segment.isdigit():
            check_key(segment)
        name = f"{self._name}.{segment}"
        if self._registry is not None and not _has_prefix(self._registry, name):
            raise AttributeError(
                f"unknown attribute {name!r}; known under {self._name!r}: "
                f"{_next_segments(self._registry, self._name)}"
            )
        return Path(self._registry, name)

    def __getattr__(self, segment: str) -> "Path":
        if segment.startswith("_"):
            raise AttributeError(segment)
        return self._child(segment)

    def __getitem__(self, index: int) -> "Path":
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise TypeError(f"list index must be a non-negative int, got {index!r}")
        return self._child(str(index))

    def __dir__(self) -> list:
        if self._registry is None:
            return []
        return _next_segments(self._registry, self._name)

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError("attribute paths are immutable")

    def __repr__(self) -> str:
        return f"attr.{self._name}"

    def __hash__(self) -> int:
        return hash(self._name)

    # ---- comparisons ----

    def _cmp(self, op: str, value: Any) -> Compare:
        if isinstance(value, (Path, Expr)):
            raise TypeError("compare an attribute with a value, not with another attribute")
        return Compare(self._name, op, value)

    def __eq__(self, value: Any) -> Compare:  # type: ignore[override]
        return self._cmp("==", value)

    def __ne__(self, value: Any) -> Compare:  # type: ignore[override]
        return self._cmp("!=", value)

    def __lt__(self, value: Any) -> Compare:
        return self._cmp("<", value)

    def __le__(self, value: Any) -> Compare:
        return self._cmp("<=", value)

    def __gt__(self, value: Any) -> Compare:
        return self._cmp(">", value)

    def __ge__(self, value: Any) -> Compare:
        return self._cmp(">=", value)

    def is_in(self, values: Iterable[Any]) -> Compare:
        """Some value of the attribute is one of ``values``."""
        items = [v for v in values if v is not None]
        if not items:
            raise ValueError("is_in: provide at least one value")
        return Compare(self._name, "in", items)

    def exists(self) -> Compare:
        """The node carries the attribute at all (``~`` for absence)."""
        return Compare(self._name, "exists")


class AttrProxy:
    """``aq.attr``: the root of attribute paths, bound to one server.

    ``aq.attr.unit`` is a :class:`Path`; ``aq.attr("product_info.year")``
    spells a path as a string, for keys that are not Python identifiers.
    """

    __slots__ = ("_registry",)

    def __init__(self, client=None):
        object.__setattr__(self, "_registry", Registry(client) if client is not None else None)

    def __getattr__(self, name: str) -> Path:
        if name.startswith("_"):
            raise AttributeError(name)
        return self(name)

    def __call__(self, name: str) -> Path:
        if not isinstance(name, str) or not name:
            raise TypeError("attribute name must be a non-empty string")
        if not valid_path(name) or name.split(".", 1)[0].isdigit():
            raise ValueError(
                f"invalid attribute path {name!r}: dot-separated keys (letters, digits, "
                "underscore, hyphen, starting with a letter or underscore) and list indices"
            )
        if self._registry is not None and not _has_prefix(self._registry, name):
            raise AttributeError(
                f"unknown attribute {name!r}; known: {_next_segments(self._registry, None)}"
            )
        return Path(self._registry, name)

    def __dir__(self) -> list:
        return _next_segments(self._registry, None) if self._registry is not None else []

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError("aq.attr is read-only")

    def __repr__(self) -> str:
        return "aq.attr"


def attr_name(x: Any) -> Any:
    """``Path`` -> its name; anything else unchanged (column-name arguments)."""
    return x.name if isinstance(x, Path) else x
