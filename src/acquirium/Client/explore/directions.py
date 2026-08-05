"""The step patterns that ``direction="upstream"/"downstream"`` infer.

These constants are the single source of truth for what a directional query
actually walks, in program-IR form: a tuple of alternative step *chains*,
where a chain is ``((predicate_uri, node_class_uri | None), ...)`` and a
``"^"`` prefix inverts the predicate. One direction step matches if any
chain does.

**Entity steps** — the next entity along the flow (used as the repeatable
segment of nearest-measurement searches). Downstream, per step:

    A s223:connectedTo B
    B s223:connectedFrom A
    A s223:connectedThrough C . C s223:connectsTo B      (via a Connection)
    C s223:connectsFrom A . B s223:connectedThrough C

**Property steps** — where a direction's measurements live (the final
segment of nearest-measurement searches). Downstream, from entity A:

    A s223:hasConnectionPoint CP . CP a s223:OutletConnectionPoint .
        CP s223:hasProperty P                            (own outlet)
    C s223:connectsFrom A . C s223:hasProperty P         (on the connection)
    C s223:connectsFrom A . C s223:connectsAt CP .
        CP a s223:InletConnectionPoint . CP s223:hasProperty P
                                                         (next entity's inlet)
    A s223:connectedTo B . B s223:hasProperty P          (next entity itself)

Upstream mirrors both sets. Non-nearest directional queries compile the
equivalent property paths server-side (see ``compile._direction_edge_pattern``).
"""
from __future__ import annotations

from acquirium.internals.internals_namespaces import S223

_CT = str(S223.connectedTo)
_CF = str(S223.connectedFrom)
_CTH = str(S223.connectedThrough)
_CST = str(S223.connectsTo)
_CSF = str(S223.connectsFrom)
_CSA = str(S223.connectsAt)
_HAS_CP = str(S223.hasConnectionPoint)
_HAS_PROP = str(S223.hasProperty)
_OUTLET = str(S223.OutletConnectionPoint)
_INLET = str(S223.InletConnectionPoint)

DOWNSTREAM_EQUIPMENT = (
    ((_CT, None),),
    ((f"^{_CF}", None),),
    ((_CTH, None), (_CST, None)),
    ((f"^{_CSF}", None), (f"^{_CTH}", None)),
)

UPSTREAM_EQUIPMENT = (
    ((f"^{_CT}", None),),
    ((_CF, None),),
    ((_CTH, None), (_CSF, None)),
    ((f"^{_CST}", None), (f"^{_CTH}", None)),
)

DOWNSTREAM_PROPERTY = (
    ((_HAS_CP, _OUTLET), (_HAS_PROP, None)),
    ((f"^{_CSF}", None), (_HAS_PROP, None)),
    ((f"^{_CSF}", None), (_CSA, _INLET), (_HAS_PROP, None)),
    ((_CT, None), (_HAS_PROP, None)),
)

UPSTREAM_PROPERTY = (
    ((_HAS_CP, _INLET), (_HAS_PROP, None)),
    ((f"^{_CST}", None), (_HAS_PROP, None)),
    ((f"^{_CST}", None), (_CSA, _OUTLET), (_HAS_PROP, None)),
    ((f"^{_CT}", None), (_HAS_PROP, None)),
)

EQUIPMENT_STEPS = {"downstream": DOWNSTREAM_EQUIPMENT, "upstream": UPSTREAM_EQUIPMENT}
PROPERTY_STEPS = {"downstream": DOWNSTREAM_PROPERTY, "upstream": UPSTREAM_PROPERTY}
