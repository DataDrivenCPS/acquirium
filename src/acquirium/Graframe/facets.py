"""Facet computation — summarising the neighbourhood of a Selection.

A *facet* answers "what can I query next?" for the current set of focus nodes.
Given a Selection, we group the edges leaving (or entering) the focus nodes and
count, per key, how many *distinct focus nodes* support that key (the
``support``) plus the raw edge count. Three key functions are offered:

* ``predicate``      — group by predicate.
* ``pred-obj``       — group by (predicate, object value).
* ``pred-obj-type``  — group by (predicate, rdf:type of object).

Support (distinct focus nodes) is the useful measure for exploratory analysis:
it tells you how many of the things you currently hold can take a given step.

An active :class:`~acquirium.Graframe.profile.Profile` curates the result:
hidden predicates/types are filtered out (in SPARQL, so ``LIMIT`` stays
correct), and the profile's **named virtual edges** are surfaced as extra rows
(``is_virtual=True``) that can be traversed with ``follow("<name>")``.

Facet rows are *actionable*: pick one with :meth:`Facets.row` and hand it
straight to :meth:`~acquirium.Graframe.selection.Selection.follow` /
:meth:`~acquirium.Graframe.selection.Selection.having` — the row already knows
its predicate, direction, and (for ``pred-obj`` / ``pred-obj-type`` facets) the
object value/type to filter on, so you never retype a URI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Sequence

from .algebra import Path, _is_uri, to_path
from .profile import Profile

if TYPE_CHECKING:
    import polars as pl

    from .selection import Selection

_BY_CHOICES = ("predicate", "pred-obj", "pred-obj-type")
_DIR_CHOICES = ("out", "in", "both")
_KEY_KIND = {"pred-obj": "value", "pred-obj-type": "type"}

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


@dataclass(frozen=True)
class FacetRow:
    direction: str  # "out", "in", or "virtual"
    predicate: str  # predicate URI, or the virtual-edge name when is_virtual
    support: int  # distinct focus nodes with this key
    edges: int  # total matching edges
    key: str | None = None  # object value or object type (None for by=predicate)
    is_virtual: bool = False  # True for named virtual-edge rows
    key_kind: str | None = None  # "value" (pred-obj) or "type" (pred-obj-type)


class Facets:
    """The computed facets of a :class:`Selection`, ready to inspect or drill."""

    def __init__(self, selection: "Selection", by: str, rows: list[FacetRow]):
        self._selection = selection
        self.by = by
        self.rows = rows

    # -- inspection -----------------------------------------------------
    def predicates(self, direction: str | None = None) -> list[str]:
        """Distinct predicates / edge names present, most-supported first."""
        seen: dict[str, int] = {}
        for r in self.rows:
            if direction and r.direction != direction:
                continue
            seen[r.predicate] = max(seen.get(r.predicate, 0), r.support)
        return [p for p, _ in sorted(seen.items(), key=lambda kv: -kv[1])]

    def row(
        self,
        selector: int | str | None = None,
        *,
        key: str | None = None,
        direction: str | None = None,
    ) -> FacetRow:
        """Pick a single :class:`FacetRow` to feed to ``follow``/``having``.

        ``selector`` is either an integer index into :attr:`rows`, or a
        predicate / virtual-edge name (matched against the compacted *or* full
        predicate). Disambiguate collisions with ``key=`` (the object value or
        type, compacted or full) and/or ``direction=`` (``"out"``/``"in"``/
        ``"virtual"``). Raises ``KeyError`` if nothing matches and ``ValueError``
        if the selection is ambiguous.

        Example::

            f = sensors.facets(by="pred-obj")
            sensors.having(f.row("s223:observes", key="qk:Power"))
        """
        if isinstance(selector, int):
            return self.rows[selector]
        matches: list[FacetRow] = []
        for r in self.rows:
            pred = r.predicate if r.is_virtual else self._compact(r.predicate)
            if selector is not None and selector not in (pred, r.predicate):
                continue
            if key is not None:
                rkey = self._compact(r.key) if r.key is not None else None
                if key not in (rkey, r.key):
                    continue
            if direction is not None and r.direction != direction:
                continue
            matches.append(r)
        if not matches:
            raise KeyError(
                f"no facet row matches selector={selector!r}"
                + (f" key={key!r}" if key is not None else "")
                + (f" direction={direction!r}" if direction is not None else "")
            )
        if len(matches) > 1:
            raise ValueError(
                f"{len(matches)} facet rows match selector={selector!r}; "
                f"disambiguate with key=/direction="
            )
        return matches[0]

    def to_polars(self) -> "pl.DataFrame":
        import polars as pl

        data = {
            "direction": [r.direction for r in self.rows],
            "predicate": [self._display_predicate(r) for r in self.rows],
        }
        if self.by != "predicate":
            data["object" if self.by == "pred-obj" else "object_type"] = [
                self._compact(r.key) for r in self.rows
            ]
        data["support"] = [r.support for r in self.rows]
        data["edges"] = [r.edges for r in self.rows]
        return pl.DataFrame(data)

    def show(self, limit: int = 25) -> "Facets":
        """Pretty-print the facets as a table (returns self for chaining)."""
        from rich.console import Console
        from rich.table import Table

        table = Table(title=f"Facets (by={self.by})")
        table.add_column("dir")
        table.add_column("predicate")
        if self.by == "pred-obj":
            table.add_column("object")
        elif self.by == "pred-obj-type":
            table.add_column("object type")
        table.add_column("support", justify="right")
        table.add_column("edges", justify="right")

        for r in self.rows[:limit]:
            pred = self._display_predicate(r)
            cells = [("↳ virtual" if r.is_virtual else r.direction), pred]
            if self.by != "predicate":
                cells.append(self._compact(r.key))
            cells += [str(r.support), str(r.edges)]
            table.add_row(*cells)
        Console().print(table)
        return self

    def _display_predicate(self, r: FacetRow) -> str:
        """Compacted predicate, ``^``-prefixed for ``in`` rows.

        The ``^`` matches this codebase's inverse-path syntax, so the printed
        value can be pasted straight into ``follow()``/``having()`` to walk
        the reverse edge. Uncompacted full URIs get ``<>``-wrapped so the
        result still parses as a single inverse-predicate path.
        """
        pred = r.predicate if r.is_virtual else self._compact(r.predicate)
        if r.direction != "in":
            return pred
        return f"^<{pred}>" if _is_uri(pred) else f"^{pred}"

    def _compact(self, x: Any) -> str:
        if x is None:
            return ""
        try:
            return self._selection.client.compact_uri(x)
        except Exception:
            return str(x)

    def __len__(self) -> int:
        return len(self.rows)

    def __repr__(self) -> str:
        return f"<Facets by={self.by} rows={len(self.rows)}>"


def compute_facets(
    selection: "Selection",
    *,
    by: str,
    direction: str,
    limit: int,
    only: Sequence[str] | None = None,
    hide: Sequence[str] | None = None,
    raw: bool = False,
    virtual: bool = True,
) -> Facets:
    if by not in _BY_CHOICES:
        raise ValueError(f"by must be one of {_BY_CHOICES}, got {by!r}")
    if direction not in _DIR_CHOICES:
        raise ValueError(f"direction must be one of {_DIR_CHOICES}, got {direction!r}")

    profile = None if raw else selection.profile
    if not raw and (only or hide):
        profile = (profile or Profile()).with_(allow=only or (), deny=hide or ())

    nsmap = _nsmap(selection.client)
    expand = selection._expand  # noqa: SLF001
    pred_filter = profile.predicate_filter("fp", nsmap, expand) if profile else None
    type_filter = profile.type_filter("ft", nsmap, expand) if profile else None

    rows: list[FacetRow] = []

    # Named virtual edges first (they are the curated, important paths).
    if profile and profile.edges and virtual and not raw:
        for name, value in profile.edges.items():
            path = to_path(value, expand)
            rows.extend(_virtual_facet(selection, name, path, by=by, limit=limit))

    dirs = ("out", "in") if direction == "both" else (direction,)
    for d in dirs:
        sparql = _facet_query(
            selection, by=by, direction=d, limit=limit,
            pred_filter=pred_filter, type_filter=type_filter,
        )
        res = selection.client.sparql_query(sparql, use_union=True)
        rows.extend(_parse(res, by=by, direction=d))

    # Virtual edges surface first, then by support.
    rows.sort(key=lambda r: (not r.is_virtual, -r.support))
    return Facets(selection, by, rows)


def _nsmap(client: Any) -> dict[str, str]:
    try:
        nm = client.namespace_manager()
        return {p: str(n) for p, n in nm.namespaces()}
    except Exception:
        return {}


def _facet_query(
    selection: "Selection",
    *,
    by: str,
    direction: str,
    limit: int,
    pred_filter: str | None = None,
    type_filter: str | None = None,
) -> str:
    focus = selection._state.focus  # noqa: SLF001
    body = selection._where_body()  # noqa: SLF001
    edge = f"?{focus} ?fp ?fo ." if direction == "out" else f"?fo ?fp ?{focus} ."

    support = f"(COUNT(DISTINCT ?{focus}) AS ?support)"
    edges = "(COUNT(*) AS ?edges)"
    lines = [body, edge]

    if by == "predicate":
        select = f"SELECT ?fp {support} {edges}"
        group = "GROUP BY ?fp"
    elif by == "pred-obj":
        select = f"SELECT ?fp ?fo {support} {edges}"
        group = "GROUP BY ?fp ?fo"
    else:  # pred-obj-type
        select = f"SELECT ?fp ?ft {support} {edges}"
        lines.append(f"?fo <{RDF_TYPE}> ?ft .")
        group = "GROUP BY ?fp ?ft"

    if pred_filter:
        lines.append(pred_filter)
    if type_filter and by == "pred-obj-type":
        lines.append(type_filter)

    where = "\n  ".join(x for x in lines if x)
    return (
        f"{select}\nWHERE {{\n  {where}\n}}\n{group}\n"
        f"ORDER BY DESC(?support)\nLIMIT {int(limit)}"
    )


def _virtual_facet(
    selection: "Selection", name: str, path: Path, *, by: str, limit: int
) -> list[FacetRow]:
    focus = selection._state.focus  # noqa: SLF001
    body = selection._where_body()  # noqa: SLF001
    pr = path.render()

    support = f"(COUNT(DISTINCT ?{focus}) AS ?support)"
    edges = "(COUNT(*) AS ?edges)"
    lines = [body, f"?{focus} {pr} ?fo ."]

    if by == "predicate":
        select = f"SELECT {support} {edges}"
        group = ""
    elif by == "pred-obj":
        select = f"SELECT ?fo {support} {edges}"
        group = "GROUP BY ?fo"
    else:  # pred-obj-type
        select = f"SELECT ?ft {support} {edges}"
        lines.append(f"?fo <{RDF_TYPE}> ?ft .")
        group = "GROUP BY ?ft"

    where = "\n  ".join(x for x in lines if x)
    q = f"{select}\nWHERE {{\n  {where}\n}}"
    if group:
        q += f"\n{group}\nORDER BY DESC(?support)\nLIMIT {int(limit)}"
    res = selection.client.sparql_query(q, use_union=True)

    out: list[FacetRow] = []
    for row in res.get("rows", []):
        if by == "predicate":
            support_v, edges_v, key = row[0], row[1], None
        else:
            key, support_v, edges_v = row[0], row[1], row[2]
        support_i = _int(support_v)
        if support_i == 0:
            continue  # no matches — don't surface an empty edge
        out.append(
            FacetRow(
                direction="virtual",
                predicate=name,
                support=support_i,
                edges=_int(edges_v),
                key=str(key) if key is not None else None,
                is_virtual=True,
                key_kind=_KEY_KIND.get(by),
            )
        )
    return out


def _parse(res: dict, *, by: str, direction: str) -> list[FacetRow]:
    key_kind = _KEY_KIND.get(by)
    rows_out: list[FacetRow] = []
    for row in res.get("rows", []):
        if by == "predicate":
            pred, support, edges = row[0], row[1], row[2]
            key = None
        else:
            pred, key, support, edges = row[0], row[1], row[2], row[3]
        rows_out.append(
            FacetRow(
                direction=direction,
                predicate=str(pred) if pred is not None else "",
                support=_int(support),
                edges=_int(edges),
                key=str(key) if key is not None else None,
                key_kind=key_kind,
            )
        )
    return rows_out


def _int(x: Any) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return 0
