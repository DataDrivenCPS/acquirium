"""Graph and dataflow changes applied through the public client.

Each function changes the server and the harness's :class:`oracle.World`
together, so the oracle always knows what the server should now contain.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from experiments import benicia, views as V
from experiments.benicia import Point, Replica
from experiments.oracle import World
from experiments.replay import Replay

S223 = "http://data.ashrae.org/standard223#"
QUDT = "http://qudt.org/schema/qudt/"
UNIT = "http://qudt.org/vocab/unit/"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
FAR = datetime(2100, 1, 1, tzinfo=timezone.utc)

UNIT_FOR = {V.CONCENTRATION: f"{UNIT}MilliGM-PER-L", V.FLOW: f"{UNIT}GAL_US", V.ACIDITY: f"{UNIT}PH"}


def remove_point(client: Any, world: World, ref_uri: str) -> Point:
    """Decommission a measurement point: drop every triple about it in the plant graph."""
    point = world.points.pop(ref_uri)
    client.sparql_update(f"DELETE WHERE {{ <{point.uri}> ?p ?o }}", source_id="plant")
    client.sparql_update(f"DELETE WHERE {{ ?s ?p <{point.uri}> }}", source_id="plant")
    return point


def add_point(client: Any, world: World, replica: Replica, replay: Replay, *, quantity_kind: str,
              host: str, name: str, rows: pl.DataFrame | None = None) -> Point:
    """Add a new measurement point to ``host`` (a connection point or unit), register and feed it."""
    uri = f"{replica.namespace}{name}"
    unit = UNIT_FOR[quantity_kind]
    client.sparql_update(
        f"INSERT DATA {{ <{uri}> a <{S223}QuantifiableObservableProperty> ; "
        f"<{QUDT}hasQuantityKind> <{quantity_kind}> ; <{QUDT}hasUnit> <{unit}> . "
        f"<{host}> <{S223}hasProperty> <{uri}> . }}", source_id="plant")
    client.register_streams([{"source_id": replica.source_id, "ref_name": name, "point_uri": uri,
                              "value_kind": "numeric", "label": f"{replica.source_id} {name}"}])
    point = Point(uri, name, unit, quantity_kind, False)
    world.points[replica.ref_uri(name)] = point
    if rows is not None and not rows.is_empty():
        replay.columns.append(name)
        if hasattr(replay, "_names"):
            replay._names[replica.ref_uri(name)] = name
        replay._send([(ts, name, float(value)) for ts, value in rows.select("time", "value").iter_rows()],
                     "fresh", rows["time"].max())
    return point


def swap_point(client: Any, world: World, replica: Replica, replay: Replay, ref_uri: str,
               rng: random.Random) -> Point:
    """Replace a sensor: the old point goes away, a new one with the same role carries on."""
    old = remove_point(client, world, ref_uri)
    host = _host_of(replica, old.uri)
    history = replay.truth_frames().get(ref_uri, V.EMPTY)
    jitter = rng.uniform(0.9, 1.1)
    rows = history.with_columns((pl.col("value") * jitter).alias("value")) if not history.is_empty() else None
    return add_point(client, world, replica, replay, quantity_kind=old.quantity_kind, host=host,
                     name=f"{old.ref_name}-replacement-{rng.randrange(10**6)}", rows=rows)


def _host_of(replica: Replica, point_uri: str) -> str:
    import rdflib
    for host, _, _ in replica.graph.triples((None, rdflib.URIRef(f"{S223}hasProperty"), rdflib.URIRef(point_uri))):
        return str(host)
    raise KeyError(point_uri)


def delete_window(client: Any, replica: Replica, replay: Replay, ref_uri: str,
                  start: datetime, end: datetime) -> int:
    """Remove a stream's readings in ``[start, end]`` by replacing the whole stream."""
    name = replay._name_of(ref_uri)
    values = replay.truth.get(ref_uri, {})
    for ts in [ts for ts in values if start <= ts <= end]:
        del values[ts]
    client.insert_timeseries(replica.source_id, name, sorted(values.items()), replace=True)
    replay.sent.append({"kind": "replace", "t": __import__("time").time(), "rows": len(values)})
    return len(values)


def deploy_view(client: Any, world: World, view: type, parameters: dict | None = None) -> None:
    V.deploy(client, [view], parameters={view.name: parameters} if parameters else None)
    world.deployed[view.name] = dict(parameters or {})


def remove_view(client: Any, world: World, view: type) -> None:
    client.remove_app(view.name)
    world.deployed.pop(view.name, None)


def reparameterize(client: Any, world: World, view: type, parameters: dict) -> None:
    """Redeploy with new parameters and reprocess retained history so they apply everywhere."""
    deploy_view(client, world, view, parameters)
    client.reprocess_app(view.name, EPOCH, FAR)


def reprocess(client: Any, view: type, start: datetime, end: datetime) -> None:
    client.reprocess_app(view.name, start, end)
