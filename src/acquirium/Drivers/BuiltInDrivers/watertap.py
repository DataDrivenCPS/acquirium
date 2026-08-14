from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module, util as importlib_util
from pathlib import Path
from typing import Any, Callable
import json
import logging

import polars as pl

from acquirium.Drivers.Driver import PollingIngestDriver
from acquirium.internals.internals_namespaces import HAS_PYOMO_VAR

logger = logging.getLogger("acquirium.watertap")


@dataclass(frozen=True)
class WaterTAPPointSpec:
    point_uri: str
    ref_name: str
    pyomo_var: str


class WaterTAPDriver(PollingIngestDriver):
    """Drive a WaterTAP ``build -> change_inputs -> solve`` model and ingest outputs.

    Each tick the driver builds the model, optionally applies inputs, solves it,
    then reads the RDF-mapped Pyomo variables. This matches the model interface
    in ``deployments/WATERTAP/models/<name>/build-and-solve.py``:
    ``build() -> model``, ``change_inputs(model, inputs)``, ``solve(model)``.

    Points come from the model's ``watertap-mapping.json`` (the ``properties``
    table of ontology-point-URI -> Pyomo-path). On setup the driver registers a
    stream per point; registration writes each point's external reference, its
    Pyomo variable (``acq:hasPyomoVar``), and the ``ref:hasExternalReference``
    link via Acquirium's insert-graph interface — so no hand-authored reference
    graph is needed.

    Required config keys under ``[driver]`` or ``[[drivers]]``:
      - ``watertap_mapping_path``: model ``watertap-mapping.json`` with a
        ``namespace`` and a ``properties`` table of point-URI -> Pyomo-path
      - ``watertap_build_spec``: ``module.path:callable`` or ``path/to/file.py:callable``
        resolving to ``build``
      - ``watertap_solve_spec``: ``module:callable`` resolving to ``solve``,
        applied as ``solve(model)`` after build

    Optional keys:
      - ``watertap_source_id``: datasource id, default ``"watertap"``
      - ``watertap_build_kwargs``: TOML table of kwargs passed to the build fn
      - ``watertap_change_inputs_spec``: ``module:callable`` resolving to
        ``change_inputs``, applied as ``change_inputs(model, inputs)`` after build
      - ``watertap_inputs``: TOML table passed as the ``inputs`` dict to
        ``watertap_change_inputs_spec`` (skipped when empty)
      - ``watertap_graph_path``: model s223 ontology graph to insert on setup so
        point nodes carry domain semantics (sensors, equipment, units)
      - ``watertap_insert_graph``: insert ``watertap_graph_path`` on setup, default ``false``
      - ``watertap_insert_graph_replace``: replace this driver's graph when inserting, default ``false``
      - ``watertap_register_streams``: register mapped streams on setup, default ``true``
      - ``watertap_result_attr``: attribute to read from the build fn result
    """

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self.source_id = str(cfg.get("watertap_source_id", "watertap"))
        self._mapping_path = resolve_path(
            cfg.get("watertap_mapping_path"), "watertap_mapping_path"
        )
        self._build_spec = str(_require_config(cfg.get("watertap_build_spec"), "watertap_build_spec"))
        self._solve_spec = str(_require_config(cfg.get("watertap_solve_spec"), "watertap_solve_spec"))
        self._change_inputs_spec = cfg.get("watertap_change_inputs_spec")
        self._build_kwargs = dict(cfg.get("watertap_build_kwargs", {}))
        self._inputs = dict(cfg.get("watertap_inputs", {}))
        self._graph_path = cfg.get("watertap_graph_path")
        self._insert_graph = bool(cfg.get("watertap_insert_graph", False))
        self._insert_graph_replace = bool(cfg.get("watertap_insert_graph_replace", False))
        self._register_streams = bool(cfg.get("watertap_register_streams", True))
        self._result_attr = cfg.get("watertap_result_attr")

        self._build_fn = _load_callable(self._build_spec)
        self._solve_fn = _load_callable(self._solve_spec)
        self._change_inputs_fn = (
            _load_callable(str(self._change_inputs_spec)) if self._change_inputs_spec else None
        )
        # Points come from the model's watertap-mapping.json: each property maps
        # an ontology point URI to the Pyomo path the driver reads each tick.
        self._point_specs = _load_point_specs_from_mapping(self._mapping_path, self.source_id)

        self.aq.register_datasource(self.source_id)

        # Optionally insert the model's s223 ontology graph so the point nodes
        # carry their domain semantics (sensors, equipment, units).
        if self._insert_graph and self._graph_path:
            self.insert_graph(
                resolve_path(self._graph_path, "watertap_graph_path"),
                replace=self._insert_graph_replace,
            )

        # Registering streams writes each point's external reference, its Pyomo
        # variable, and the hasExternalReference link straight from the mapping,
        # using Acquirium's insert-graph interface under the hood.
        if self._register_streams:
            self.aq.register_streams([
                {
                    "source_id": self.source_id,
                    "ref_name": spec.ref_name,
                    "point_uri": spec.point_uri,
                    "value_kind": "numeric",
                    "properties": {HAS_PYOMO_VAR: spec.pyomo_var},
                }
                for spec in self._point_specs
            ])

    def collect(self) -> pl.DataFrame:
        logger.debug("watertap collect: building model via %s", self._build_spec)
        result = self._build_fn(**self._build_kwargs)
        model = self._extract_model(result)
        if self._change_inputs_fn is not None and self._inputs:
            self._change_inputs_fn(model, self._inputs)
        self._solve_fn(model)
        ts = datetime.now(timezone.utc)

        rows: list[tuple[datetime, str, Any]] = []
        missing = 0
        for spec in self._point_specs:
            # Read whatever the graph mapped this point to instead of baking
            # WaterTAP model structure into the driver.
            found, value = get_observation_from_model(model, spec.pyomo_var)
            if not found:
                missing += 1
                continue
            rows.append((ts, spec.ref_name, value))

        logger.debug("watertap collect: %d rows from %d specs (%d missing)", len(rows), len(self._point_specs), missing)
        if missing:
            logger.warning("watertap: skipped %d unmapped model values", missing)
        return pl.DataFrame(
            rows,
            schema={
                "ts": pl.Datetime("us", "UTC"),
                "ref_name": pl.Utf8,
                "value": pl.Object,
            },
            orient="row",
        )

    def _extract_model(self, result: Any) -> Any:
        if self._result_attr:
            try:
                return getattr(result, str(self._result_attr))
            except AttributeError as exc:
                raise ValueError(
                    f"watertap_result_attr={self._result_attr!r} not found on build result"
                ) from exc
        if isinstance(result, tuple | list):
            if not result:
                raise ValueError("WaterTAP build function returned an empty sequence")
            return result[0]
        return result


def get_value_from_model(model: Any, component_name: str) -> float | None:
    """Best-effort extraction of a numeric value from a Pyomo-like model."""
    found, value = get_observation_from_model(model, component_name)
    if not found or value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_observation_from_model(model: Any, component_name: str) -> tuple[bool, Any]:
    """Best-effort extraction of an observed value from a Pyomo-like model."""
    comp = None
    try:
        comp = model.find_component(component_name)
    except Exception:
        comp = None

    component_uid = _load_component_uid()
    if comp is None and component_uid is not None:
        try:
            comp = component_uid(component_name).find_component_on(model)
        except Exception:
            comp = None

    if comp is None:
        return False, None

    pyomo_value = _load_pyomo_value()
    if pyomo_value is not None:
        try:
            value = pyomo_value(comp)
            if value is not None:
                return True, value
        except Exception:
            pass

    try:
        raw = comp.value
    except Exception:
        return False, None
    if raw is None:
        return False, None
    return True, raw


def _load_point_specs_from_mapping(
    mapping_path: Path, source_id: str
) -> list[WaterTAPPointSpec]:
    """Build point specs from a model's ``watertap-mapping.json``.

    The mapping's ``properties`` table maps each ontology point URI to the Pyomo
    variable path to read. The source-local ref name is the point URI with the
    mapping ``namespace`` prefix stripped, and the canonical reference URI is
    minted deterministically from ``(source_id, ref_name)`` — the same value the
    server computes when streams are registered and rows inserted.
    """
    mapping = json.loads(mapping_path.read_text())
    namespace = mapping.get("namespace", "")
    properties = mapping.get("properties")
    if not properties:
        raise ValueError(
            f"No 'properties' found in {mapping_path}. Expected a watertap-mapping.json "
            "with a 'properties' table of point-URI -> Pyomo-path."
        )

    point_specs: list[WaterTAPPointSpec] = []
    for point_uri, pyomo_var in properties.items():
        ref_name = (
            point_uri[len(namespace):]
            if namespace and point_uri.startswith(namespace)
            else point_uri
        )
        point_specs.append(
            WaterTAPPointSpec(
                point_uri=str(point_uri),
                ref_name=str(ref_name),
                pyomo_var=str(pyomo_var),
            )
        )
    return point_specs


def _load_callable(spec: str) -> Callable[..., Any]:
    module_name, sep, attr_name = spec.partition(":")
    if not sep or not attr_name:
        raise ValueError(
            "watertap_build_spec must be in the form 'module.path:callable' or "
            "'path/to/file.py:callable'"
        )

    if module_name.endswith(".py") or Path(module_name).exists():
        # Support deployment-local scripts without requiring them to be
        # importable packages.
        module_path = Path(module_name).expanduser().resolve()
        if not module_path.exists():
            raise FileNotFoundError(f"WaterTAP build file not found: {module_path}")
        spec_obj = importlib_util.spec_from_file_location(
            f"acquirium_watertap_{module_path.stem}",
            module_path,
        )
        if spec_obj is None or spec_obj.loader is None:
            raise ImportError(f"Could not load WaterTAP build module from {module_path}")
        module = importlib_util.module_from_spec(spec_obj)
        spec_obj.loader.exec_module(module)
    else:
        module = import_module(module_name)

    try:
        candidate = getattr(module, attr_name)
    except AttributeError as exc:
        raise AttributeError(f"Callable {attr_name!r} not found in {module_name!r}") from exc
    if not callable(candidate):
        raise TypeError(f"{spec!r} resolved to a non-callable object")
    return candidate


def _load_pyomo_value() -> Callable[[Any], Any] | None:
    try:
        from pyomo.environ import value as pyomo_value
    except Exception:
        return None
    return pyomo_value


def _load_component_uid() -> Callable[[str], Any] | None:
    try:
        from pyomo.core.base.componentuid import ComponentUID
    except Exception:
        return None
    return ComponentUID


def _require_config(value: Any, key: str) -> Any:
    if value in (None, ""):
        raise ValueError(f"Missing required config key: {key}")
    return value


def resolve_path(raw_path: Any, key: str) -> Path:
    """Resolve a required file path from driver config, naming *key* on failure.

    Relative paths resolve against the current working directory. Raises
    ValueError when the key is missing, FileNotFoundError when the file is not.
    """
    value = _require_config(raw_path, key)
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        raise FileNotFoundError(f"{key} not found: {path}")
    return path.resolve()
