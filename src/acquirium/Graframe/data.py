"""Bridge from a Graframe Selection to the timeseries data plane.

Graframe answers *which points*; this module hands those points to the existing
:class:`~acquirium.Client.data_object.DataObject` machinery to fetch the
*values*. The connection point is a Selection whose focus nodes are data points
(they carry ``ref:hasExternalReference``): we resolve each point's external
reference and unit annotations, then build ``BindingInfo`` records that
DataObject materializes on demand.

Marks on the Selection become context columns, so
``selection.mark("system")...data().by("system")`` groups series by that
waypoint — the same grouping the classic query builder offers. Internally the
column is carried as ``entity__<name>`` (a collision-guard prefix), but it
surfaces to the user under the bare mark name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from acquirium.Client.data_object import (
    RESERVED_DATA_COLUMNS,
    BindingInfo,
    DataObject,
)
from acquirium.Client.query_graph import QueryGraph
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE, HAS_UNIT

if TYPE_CHECKING:
    from .selection import Selection


def _data_sparql(selection: "Selection", entity_marks: dict[str, str]) -> str:
    focus = selection._state.focus  # noqa: SLF001
    body = selection._where_body()  # noqa: SLF001

    projections = [
        f"(?{focus} AS ?point)",
        "(?gref AS ?ref)",
        "(?gunit AS ?unit)",
        "(?gextunit AS ?extunit)",
    ]
    for name, var in entity_marks.items():
        projections.append(f"(?{var} AS ?entity__{name})")

    lines = [
        body,
        f"?{focus} <{HAS_EXTERNAL_REFERENCE}> ?gref .",
        f"OPTIONAL {{ ?{focus} <{HAS_UNIT}> ?gunit . }}",
        f"OPTIONAL {{ ?gref <{HAS_UNIT}> ?gextunit . }}",
    ]
    where = "\n  ".join(x for x in lines if x)
    return "SELECT DISTINCT " + " ".join(projections) + f"\nWHERE {{\n  {where}\n}}"


def build_data_object(
    selection: "Selection",
    *,
    start: Any = None,
    end: Any = None,
    limit: int | None = None,
    order: str = "asc",
    cast_value: str | None = "float",
    value_mode: str = "default",
) -> DataObject:
    """Materialize a :class:`DataObject` from a Selection's focus data points."""
    client = selection.client
    entity_marks = {
        name: var
        for name, var in selection._state.marks.items()  # noqa: SLF001
        if var != selection._state.focus  # noqa: SLF001
    }
    clashes = sorted(n for n in entity_marks if n in RESERVED_DATA_COLUMNS)
    if clashes:
        raise ValueError(
            f"mark name(s) {clashes} collide with reserved data columns "
            f"{sorted(RESERVED_DATA_COLUMNS)}; rename the mark(s) before calling .data()"
        )
    entity_columns = sorted(f"entity__{n}" for n in entity_marks)

    res = client.sparql_query(_data_sparql(selection, entity_marks), use_union=True)
    cols = res.get("columns", [])
    idx = {c: i for i, c in enumerate(cols)}
    rows = res.get("rows", [])

    def cell(row: list, name: str) -> Any:
        i = idx.get(name)
        return row[i] if i is not None and i < len(row) else None

    # Collect unique (point, ref) bindings with their units + per-row contexts.
    order_keys: list[tuple[str, str]] = []
    units: dict[tuple[str, str], tuple[str | None, str | None]] = {}
    contexts: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in rows:
        point = cell(row, "point")
        ref = cell(row, "ref")
        if point is None or ref is None:
            continue
        key = (str(point), str(ref))
        if key not in units:
            order_keys.append(key)
            u = cell(row, "unit")
            eu = cell(row, "extunit")
            units[key] = (str(u) if u is not None else None, str(eu) if eu is not None else None)
        ctx = {
            ec: str(cell(row, ec))
            for ec in entity_columns
            if cell(row, ec) is not None
        }
        contexts.setdefault(key, []).append(ctx)

    if not order_keys:
        return DataObject._empty(QueryGraph(), cast_value=cast_value)  # noqa: SLF001

    stats = client.timeseries_info_batch(list({ref for _, ref in order_keys}))

    bindings: list[BindingInfo] = []
    for nid, key in enumerate(order_keys):
        point_uri, ref_uri = key
        prop_unit, ref_unit = units[key]
        info = stats.get(ref_uri)
        ctx_list = _dedupe(contexts.get(key, [{}]) or [{}])
        bindings.append(
            BindingInfo(
                nid=nid,
                point_uri=point_uri,
                ref_uri=ref_uri,
                alias=_alias(client, point_uri),
                entity_contexts=ctx_list,
                row_count=info.row_count if info else 0,
                earliest=info.earliest if info else None,
                latest=info.latest if info else None,
                property_unit=prop_unit,
                ref_unit=ref_unit,
            )
        )

    return DataObject(
        _bindings=bindings,
        _entity_columns=entity_columns,
        _query_graph=QueryGraph(),
        _client=client,
        _query_params={
            "start": start,
            "end": end,
            "limit": limit,
            "order": order,
            "cast_value": cast_value,
            "value_mode": value_mode,
        },
        _tall=None,
        _materialized=False,
    )


def _alias(client: Any, point_uri: str) -> str:
    try:
        return client.compact_uri(point_uri)
    except Exception:
        return point_uri


def _dedupe(contexts: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple] = set()
    out: list[dict[str, str]] = []
    for ctx in contexts:
        key = tuple(sorted(ctx.items()))
        if key not in seen:
            seen.add(key)
            out.append(ctx)
    return out
