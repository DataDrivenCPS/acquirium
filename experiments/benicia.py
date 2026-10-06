"""The Benicia model and generated data, with k replicas for scale runs.

Replica ``i`` is the same model under the namespace ``urn:ex{i}/`` with its
own seed, registered under source ``benicia{i}``. Replica 0 keeps the
original ``urn:ex/`` namespace and source ``benicia``. The data generator
is the one the deployment's live driver uses, imported from
``deployments/BENICIA/scripts``.
"""
from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import pyarrow as pa
import rdflib

from acquirium.internals.models import compute_ref_uri

REPO = Path(__file__).resolve().parent.parent
DEPLOYMENT = REPO / "deployments" / "BENICIA"
MODEL_PATH = DEPLOYMENT / "benicia-model-100.ttl"
if str(DEPLOYMENT / "scripts") not in sys.path:
    sys.path.insert(0, str(DEPLOYMENT / "scripts"))

from benicia_generator import (  # noqa: E402
    build_state_for_property, get_properties, get_unit_and_qk, is_enumeration, local_name,
)

S223 = "http://data.ashrae.org/standard223#"
QUDT = "http://qudt.org/schema/qudt/"
QK = "http://qudt.org/vocab/quantitykind/"
START = datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Point:
    uri: str
    ref_name: str
    unit: str | None
    quantity_kind: str | None
    enumeration: bool


@dataclass
class Replica:
    index: int
    graph: rdflib.Graph
    turtle: str
    points: list[Point]

    @property
    def source_id(self) -> str:
        return "benicia" if self.index == 0 else f"benicia{self.index}"

    @property
    def namespace(self) -> str:
        return "urn:ex/" if self.index == 0 else f"urn:ex{self.index}/"

    def ref_uri(self, ref_name: str) -> str:
        return str(compute_ref_uri(self.source_id, ref_name))

    def numeric_points(self) -> list[Point]:
        return [point for point in self.points if not point.enumeration]


def load_replica(index: int = 0) -> Replica:
    text = MODEL_PATH.read_text()
    if index:
        text = text.replace("@prefix wbs: <urn:ex/> .", f"@prefix wbs: <urn:ex{index}/> .")
        text = text.replace("<urn:ex/", f"<urn:ex{index}/")
    graph = rdflib.Graph().parse(data=text, format="turtle")
    points = []
    for prop in get_properties(graph):
        unit, qk = get_unit_and_qk(graph, prop)
        points.append(Point(str(prop), local_name(prop), unit, f"{QK}{qk}" if qk else None,
                            is_enumeration(graph, prop)))
    return Replica(index, graph, text, points)


def generate(replica: Replica, rows: int, *, interval: timedelta = timedelta(minutes=1),
             start: datetime = START, seed: int | None = None,
             excursion_rate: float = 0.02, step_frac: float = 0.02) -> pl.DataFrame:
    """A wide frame: ``timestamp`` plus one column per point, like the deployment's parquet."""
    rng = random.Random(42 + replica.index if seed is None else seed)
    states = {}
    for prop in get_properties(replica.graph):
        states[local_name(prop)] = (None if is_enumeration(replica.graph, prop)
                                    else build_state_for_property(rng, replica.graph, prop, step_frac=step_frac))
    timestamps = [start + i * interval for i in range(rows)]
    columns: dict[str, Any] = {"timestamp": timestamps}
    for name, state in states.items():
        if state is None:
            columns[name] = [float(rng.choice([0, 1])) for _ in range(rows)]
        else:
            columns[name] = [state.next_value(rng, excursion_rate) for _ in range(rows)]
    return pl.DataFrame(columns).with_columns(pl.col("timestamp").cast(pl.Datetime("us", "UTC")))


def melt(wide: pl.DataFrame) -> pl.DataFrame:
    """Long ``ts``/``ref_name``/``value`` rows, the shape the Arrow insert takes."""
    return (wide.unpivot(index="timestamp", variable_name="ref_name", value_name="value")
            .rename({"timestamp": "ts"}).drop_nulls("value"))


def install(client: Any, replica: Replica) -> None:
    """Insert the model under the plant source and register every point's stream."""
    client.insert_graph(replica.turtle, format="turtle", replace=replica.index == 0, source_id="plant")
    client.register_datasource(replica.source_id)
    client.register_streams([
        # A label per point: derived streams are labelled after their input, and
        # aq.align names columns by label, so unlabeled points would collide.
        {"source_id": replica.source_id, "ref_name": point.ref_name, "point_uri": point.uri,
         "value_kind": "numeric", "label": f"{replica.source_id} {point.ref_name}"}
        for point in replica.points
    ])


def insert(client: Any, replica: Replica, long: pl.DataFrame, *, publication_id: str | None = None) -> int:
    if long.is_empty():
        return 0
    table = pa.Table.from_pandas(long.select("ts", "ref_name", "value").to_pandas(), preserve_index=False)
    result = client.insert_timeseries_arrow(replica.source_id, table, publication_id=publication_id)
    return int(result.get("rows_inserted", 0))


def install_all(client: Any, replicas: Iterable[Replica]) -> None:
    for replica in replicas:
        install(client, replica)
