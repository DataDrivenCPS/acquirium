"""Self-driving Benicia stream simulator.

The migrated form of the old ``stream_simulator.py`` MQTT publisher: instead of
publishing to a broker, this is a :class:`PollingIngestDriver` that generates a
fresh value for every model property each tick and pushes them straight to
Acquirium. No broker, no external process — ``acquirium server`` runs it as a
Ray actor.

Each generated stream is registered with its ontology point URI
(``<namespace><property-local-name>``, e.g. ``urn:ex/Effluent_Pump-out-ph``), so
the rows land on the model's points and meaning-first queries reach them. The
model's s223/QUDT graph is inserted on setup so those points carry their unit
and quantity-kind semantics.

Config keys (under ``[[drivers]]`` / ``self.config["driver"]``):

    spec            = "scripts/simulator_driver.py:BeniciaSimulatorDriver"
    interval        = 1.0            # seconds between generated samples
    model           = "benicia-model-100.ttl"   # relative to this config file
    source_id       = "benicia-live"
    namespace       = "urn:ex/"      # property URI namespace
    insert_graph    = true           # insert the model graph on setup
    seed            = 42
    excursion_rate  = 0.02           # prob. per sample of a limit excursion
    step_frac       = 0.02           # random-walk step as a fraction of range
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone

import polars as pl
import rdflib

from acquirium.Drivers.BuiltInDrivers.watertap import _guess_rdf_format
from acquirium.Drivers.Driver import PollingIngestDriver

from benicia_generator import (
    build_state_for_property,
    get_properties,
    is_enumeration,
    local_name,
)

logger = logging.getLogger("acquirium.benicia.simulator")


class BeniciaSimulatorDriver(PollingIngestDriver):
    """Generate a value per model property each tick and push to Acquirium."""

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self.source_id = str(cfg.get("source_id", "benicia-live"))
        self._namespace = str(cfg.get("namespace", "urn:ex/"))
        self._excursion_rate = float(cfg.get("excursion_rate", 0.02))
        step_frac = float(cfg.get("step_frac", 0.02))
        self._rng = random.Random(int(cfg.get("seed", 42)))

        model_path = self.config_dir() / str(cfg.get("model", "benicia-model-100.ttl"))
        if not model_path.exists():
            raise FileNotFoundError(f"Benicia model not found: {model_path}")
        graph = rdflib.Graph().parse(model_path, format=_guess_rdf_format(model_path))
        self._properties = get_properties(graph)

        # Insert the model's ontology so the point nodes exist with their
        # unit / quantity-kind semantics before streams are linked to them.
        if bool(cfg.get("insert_graph", True)):
            self.insert_graph(graph.serialize(format="turtle"), format="turtle", replace=False)

        self.aq.register_datasource(self.source_id)

        # Per-property state (None for enumeration properties, which emit 0/1).
        self._states = {}
        self._enums = []
        for prop in self._properties:
            ref_name = local_name(prop)
            if is_enumeration(graph, prop):
                self._enums.append(ref_name)
            else:
                self._states[ref_name] = build_state_for_property(
                    self._rng, graph, prop, step_frac=step_frac
                )

        # Register every stream with its point URI so rows link to the model.
        self.aq.register_streams([
            {
                "source_id": self.source_id,
                "ref_name": local_name(prop),
                "point_uri": str(prop),
                "value_kind": "numeric",
            }
            for prop in self._properties
        ])

        logger.info(
            "benicia simulator ready: source_id=%s, %d properties (%d series, %d enums) from %s",
            self.source_id, len(self._properties), len(self._states), len(self._enums),
            model_path.name,
        )

    def collect(self) -> pl.DataFrame:
        ts = datetime.now(timezone.utc)
        ref_names: list[str] = []
        values: list[float] = []
        for ref_name, state in self._states.items():
            ref_names.append(ref_name)
            values.append(state.next_value(self._rng, self._excursion_rate))
        for ref_name in self._enums:
            ref_names.append(ref_name)
            values.append(float(self._rng.choice([0, 1])))

        logger.debug("benicia simulator: generated %d values at %s", len(values), ts.isoformat())
        return pl.DataFrame({
            "ts": [ts] * len(ref_names),
            "ref_name": ref_names,
            "value": values,
        })
