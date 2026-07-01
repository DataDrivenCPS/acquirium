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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

    from .selection import Selection

_BY_CHOICES = ("predicate", "pred-obj", "pred-obj-type")
_DIR_CHOICES = ("out", "in", "both")

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


@dataclass(frozen=True)
class FacetRow:
    direction: str  # "out" or "in"
    predicate: str
    support: int  # distinct focus nodes with this key
    edges: int  # total matching edges
    key: str | None = None  # object value or object type (None for by=predicate)


class Facets:
    """The computed facets of a :class:`Selection`, ready to inspect or drill."""

    def __init__(self, selection: "Selection", by: str, rows: list[FacetRow]):
        self._selection = selection
        self.by = by
        self.rows = rows

    # -- inspection -----------------------------------------------------
    def predicates(self, direction: str | None = None) -> list[str]:
        """Distinct predicates present, most-supported first."""
        seen: dict[str, int] = {}
        for r in self.rows:
            if direction and r.direction != direction:
                continue
            seen[r.predicate] = max(seen.get(r.predicate, 0), r.support)
        return [p for p, _ in sorted(seen.items(), key=lambda kv: -kv[1])]

    def to_polars(self) -> "pl.DataFrame":
        import polars as pl

        data = {
            "direction": [r.direction for r in self.rows],
            "predicate": [self._compact(r.predicate) for r in self.rows],
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

        title = f"Facets (by={self.by})"
        table = Table(title=title)
        table.add_column("dir")
        table.add_column("predicate")
        if self.by == "pred-obj":
            table.add_column("object")
        elif self.by == "pred-obj-type":
            table.add_column("object type")
        table.add_column("support", justify="right")
        table.add_column("edges", justify="right")

        for r in self.rows[:limit]:
            cells = [r.direction, self._compact(r.predicate)]
            if self.by != "predicate":
                cells.append(self._compact(r.key))
            cells += [str(r.support), str(r.edges)]
            table.add_row(*cells)
        Console().print(table)
        return self

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
    selection: "Selection", *, by: str, direction: str, limit: int
) -> Facets:
    if by not in _BY_CHOICES:
        raise ValueError(f"by must be one of {_BY_CHOICES}, got {by!r}")
    if direction not in _DIR_CHOICES:
        raise ValueError(f"direction must be one of {_DIR_CHOICES}, got {direction!r}")

    dirs = ("out", "in") if direction == "both" else (direction,)
    rows: list[FacetRow] = []
    for d in dirs:
        sparql = _facet_query(selection, by=by, direction=d, limit=limit)
        res = selection.client.sparql_query(sparql, use_union=True)
        rows.extend(_parse(res, by=by, direction=d))
    rows.sort(key=lambda r: -r.support)
    return Facets(selection, by, rows)


def _facet_query(selection: "Selection", *, by: str, direction: str, limit: int) -> str:
    focus = selection._state.focus  # noqa: SLF001 - internal access within package
    body = selection._where_body()  # noqa: SLF001

    if direction == "out":
        edge = f"?{focus} ?fp ?fo ."
    else:
        edge = f"?fo ?fp ?{focus} ."

    support = f"(COUNT(DISTINCT ?{focus}) AS ?support)"
    edges = "(COUNT(*) AS ?edges)"

    if by == "predicate":
        select = f"SELECT ?fp {support} {edges}"
        extra = ""
        group = "GROUP BY ?fp"
    elif by == "pred-obj":
        select = f"SELECT ?fp ?fo {support} {edges}"
        extra = ""
        group = "GROUP BY ?fp ?fo"
    else:  # pred-obj-type
        select = f"SELECT ?fp ?ft {support} {edges}"
        extra = f"?fo <{RDF_TYPE}> ?ft ."
        group = "GROUP BY ?fp ?ft"

    where = "\n  ".join(x for x in [body, edge, extra] if x)
    return (
        f"{select}\nWHERE {{\n  {where}\n}}\n{group}\n"
        f"ORDER BY DESC(?support)\nLIMIT {int(limit)}"
    )


def _parse(res: dict, *, by: str, direction: str) -> list[FacetRow]:
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
            )
        )
    return rows_out


def _int(x: Any) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return 0
