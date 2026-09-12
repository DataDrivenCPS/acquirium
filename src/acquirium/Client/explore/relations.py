"""Named relations for ``Query.context()``: how a measurement reaches the
plant entity it is about.

A relation is a tuple of alternative step *chains*, in the same program IR
``directions.py`` and ``related(via=)`` use: a chain is
``((predicate_uri, node_class_uri | None), ...)``, a ``"^"`` prefix inverts
the predicate, and a class on a step constrains the node that step lands
on. A relation matches if any chain does. Every chain is a fixed set of
predicates, so relations compile to plain SPARQL (no client-side walk).

Three relations ship by default:

- ``entity``: the entity the point hangs off, directly (``hasProperty``) or
  through one of its connection points (``hasConnectionPoint/hasProperty``).
- ``upstream``: the entity the point is directly downstream of. This is
  :data:`~acquirium.Client.explore.directions.DOWNSTREAM_PROPERTY` read
  backwards: the four chains that say where an entity's downstream
  measurements live, followed from the measurement to the entity.
- ``downstream``: the entity the point is directly upstream of
  (``UPSTREAM_PROPERTY`` read backwards).

For a point on a pipe the chain that fires is ``^hasProperty`` to the
Connection, then ``connectsFrom`` (upstream) or ``connectsTo``
(downstream). For a point on an outlet connection point ``upstream`` is the
equipment that owns the connection point: the outlet pressure is downstream
of the pump.

Deployments add their own with :func:`register_relation`.
"""
from __future__ import annotations

from typing import Dict, Tuple

from acquirium.Client.explore.directions import DOWNSTREAM_PROPERTY, UPSTREAM_PROPERTY
from acquirium.internals.internals_namespaces import S223

Chain = Tuple[Tuple[str, "str | None"], ...]
Chains = Tuple[Chain, ...]

_HAS_PROP = str(S223.hasProperty)
_HAS_CP = str(S223.hasConnectionPoint)


def _invert(pred: str) -> str:
    return pred[1:] if pred.startswith("^") else f"^{pred}"


def reverse_chains(chains: Chains) -> Chains:
    """Read every chain backwards: from its end node to its start node.

    Each predicate is inverted and the order is flipped. A class constraint
    stays on the node it constrains: in the forward chain the class on step
    *i* applies to the node step *i* lands on, which the reversed chain
    reaches with step *n - i*, i.e. one step earlier. The forward chain's
    start node carries no class (it is the query variable), so the reversed
    chain's last step has none; the forward chain's final class (if any) is
    dropped for the same reason.
    """
    out = []
    for chain in chains:
        preds = [p for p, _ in chain]
        classes = [c for _, c in chain]
        # class landed on by reversed step k = class of forward node before
        # forward step n-1-k, i.e. classes[n-2-k] (None for k = n-1)
        n = len(chain)
        rev = tuple(
            (_invert(preds[n - 1 - k]), classes[n - 2 - k] if n - 2 - k >= 0 else None)
            for k in range(n)
        )
        out.append(rev)
    return tuple(out)


_DEFAULTS: Dict[str, Chains] = {
    "entity": (
        ((f"^{_HAS_PROP}", None),),
        ((f"^{_HAS_PROP}", None), (f"^{_HAS_CP}", None)),
    ),
    "upstream": reverse_chains(DOWNSTREAM_PROPERTY),
    "downstream": reverse_chains(UPSTREAM_PROPERTY),
}

RELATIONS: Dict[str, Chains] = dict(_DEFAULTS)


def _validate_chains(name: str, chains) -> Chains:
    if not isinstance(chains, (list, tuple)) or not chains:
        raise ValueError(f"relation {name!r}: chains must be a non-empty tuple of step chains")
    norm = []
    for chain in chains:
        if not isinstance(chain, (list, tuple)) or not chain:
            raise ValueError(f"relation {name!r}: every chain must be a non-empty tuple of steps")
        steps = []
        for step in chain:
            if (not isinstance(step, (list, tuple)) or len(step) != 2
                    or not isinstance(step[0], str) or not step[0].strip("^")):
                raise ValueError(
                    f"relation {name!r}: a step is (predicate_uri, node_class_uri | None), got {step!r}"
                )
            if step[0].lstrip("^") == "*":
                raise ValueError(f"relation {name!r}: wildcard steps are not allowed in relations")
            steps.append((step[0], step[1] if step[1] is None else str(step[1])))
        norm.append(tuple(steps))
    return tuple(norm)


def register_relation(name: str, chains) -> None:
    """Add or replace a named relation usable as ``context(via=name)``.

    ``chains`` follows the program IR described in the module docstring.
    Predicates must be full URIs (no text resolution at registration time).
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("relation name must be a non-empty string")
    RELATIONS[name] = _validate_chains(name, chains)


def unregister_relation(name: str) -> None:
    """Remove a relation. Removing a default one restores nothing; call
    :func:`reset_relations` to get the defaults back."""
    RELATIONS.pop(name, None)


def reset_relations() -> None:
    """Restore the default relations, dropping every registered one."""
    RELATIONS.clear()
    RELATIONS.update(_DEFAULTS)


def relation_names() -> tuple:
    return tuple(RELATIONS)
