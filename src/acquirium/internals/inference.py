"""Model compilation — materialize a graph's SHACL-AF inference to a fixed point.

Acquirium stores each model's **base** graph exactly as it was provided. Queries,
however, run against the **compiled** graph: the base plus every triple entailed
by the model's SHACL-AF (``sh:rule``) rules — e.g. ASHRAE 223 connection / role
materialization. This module is the thin, pure bridge to :mod:`shifty`
(``pyshifty``) that produces that compiled graph.

It is deliberately storage-agnostic: :mod:`acquirium.Storage.graph_store` calls
:func:`compile_graph` when it (re)materializes the query graph and caches the
result, recompiling only when the source graph changes.
"""

from __future__ import annotations

from typing import Any

import shifty
from rdflib import Graph


def compile_graph(data: Any, shapes: Any = None) -> Graph:
    """Return ``data`` plus all triples entailed by its SHACL-AF rules.

    ``data`` and ``shapes`` are anything pyshifty accepts — Turtle ``str``/
    ``bytes``, a :class:`pathlib.Path`, or an :class:`rdflib.Graph`. When
    ``shapes`` is ``None`` the rules are taken from ``data`` itself; in Acquirium
    the graph handed in already includes the imported ontologies, which carry the
    ``sh:rule`` definitions, so no separate shapes graph is needed.

    Inference runs to a fixed point. It is fast enough (seconds for typical
    models) to compute synchronously.
    """
    return shifty.infer(data, shapes).graph()
