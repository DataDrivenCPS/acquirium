from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module, util as importlib_util
from pathlib import Path
from typing import Any, Callable
import logging

import polars as pl
from rdflib import Graph

from acquirium.Driver import PollingIngestDriver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    HAS_EXTERNAL_REFERENCE,
    HAS_PYOMO_VAR,
)
from acquirium.internals.models import compute_ref_uri

logger = logging.getLogger("acquirium.watertap")


@dataclass(frozen=True)
class WaterTAPPointSpec:
    point_uri: str
    ref_uri: str
    ref_name: str
    pyomo_var: str


class WaterTAPDriver(PollingIngestDriver):
    """Run a configurable WaterTAP build/solve function and ingest mapped outputs.

    Required config keys under ``[driver]`` or ``[[drivers]]``:
      - ``watertap_graph_path``: RDF file containing
        ``ref:hasExternalReference`` and ``acquirium:hasPyomoVar`` triples
      - ``watertap_build_spec``: ``module.path:callable`` or ``path/to/file.py:callable``

    Optional keys:
      - ``watertap_source_id``: datasource id, default ``"watertap"``
      - ``watertap_build_kwargs``: TOML table of kwargs passed to the build fn
      - ``watertap_insert_graph``: insert the RDF graph on setup, default ``false``
      - ``watertap_insert_graph_replace``: replace main graph when inserting, default ``false``
      - ``watertap_register_streams``: register mapped streams on setup, default ``true``
      - ``watertap_result_attr``: attribute to read from the build fn result
    """

    def setup(self) -> None:
        cfg = self.config.get("driver", {})
        self.source_id = str(cfg.get("watertap_source_id", "watertap"))
        self._graph_path = _resolve_path(cfg.get("watertap_graph_path"), "watertap_graph_path")
        self._build_spec = str(_require_config(cfg.get("watertap_build_spec"), "watertap_build_spec"))
        self._build_kwargs = dict(cfg.get("watertap_build_kwargs", {}))
        self._insert_graph = bool(cfg.get("watertap_insert_graph", False))
        self._insert_graph_replace = bool(cfg.get("watertap_insert_graph_replace", False))
        self._register_streams = bool(cfg.get("watertap_register_streams", True))
        self._result_attr = cfg.get("watertap_result_attr")

        self._build_fn = _load_callable(self._build_spec)
        # The RDF model is the driver contract: each point supplies both the
        # external reference to ingest under and the Pyomo path to read.
        self._point_specs = _load_point_specs(self._graph_path)

        self.aq.register_datasource(self.source_id)
        if self._insert_graph:
            self.aq.insert_graph(
                self._graph_path.read_text(),
                format=_guess_rdf_format(self._graph_path),
                replace=self._insert_graph_replace,
            )
        if self._register_streams:
            for spec in self._point_specs:
                self.aq.register_stream(
                    source_id=self.source_id,
                    ref_name=spec.ref_name,
                    value_kind="numeric",
                )

    def collect(self) -> pl.DataFrame:
        result = self._build_fn(**self._build_kwargs)
        model = self._extract_model(result)
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


def _load_point_specs(graph_path: Path) -> list[WaterTAPPointSpec]:
    graph = Graph().parse(graph_path, format=_guess_rdf_format(graph_path))
    point_specs: list[WaterTAPPointSpec] = []

    # A single point can advertise its ingestion reference and its model lookup
    # path independently; the driver joins them here into executable specs.
    for point_uri, _, ref_uri in graph.triples((None, HAS_EXTERNAL_REFERENCE, None)):
        ref_name_obj = graph.value(ref_uri, ACQUIRIUM_REF_NAME)
        if ref_name_obj is None:
            raise ValueError(
                f"WaterTAP reference {ref_uri} is missing acq:refName. "
                "Canonical external references must declare the source-local ref name."
            )
        ref_name = str(ref_name_obj)
        source_id_obj = graph.value(ref_uri, ACQUIRIUM_SOURCE_ID)
        source_id = str(source_id_obj) if source_id_obj is not None else None
        for _, _, pyomo_var in graph.triples((point_uri, HAS_PYOMO_VAR, None)):
            point_specs.append(
                WaterTAPPointSpec(
                    point_uri=str(point_uri),
                    ref_uri=str(ref_uri),
                    ref_name=ref_name,
                    pyomo_var=str(pyomo_var),
                )
            )
        if source_id is not None:
            expected = str(compute_ref_uri(source_id, ref_name))
            if str(ref_uri) != expected:
                raise ValueError(
                    f"WaterTAP reference {ref_uri} does not match canonical URI {expected} "
                    f"for source_id={source_id!r} ref_name={ref_name!r}"
                )

    if not point_specs:
        raise ValueError(
            f"No WaterTAP point mappings found in {graph_path}. Expected "
            "ref:hasExternalReference + acquirium:hasPyomoVar triples."
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


def _guess_rdf_format(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".ttl": "turtle",
        ".n3": "n3",
        ".xml": "xml",
        ".trix": "trix",
    }.get(suffix, "turtle")


def _require_config(value: Any, key: str) -> Any:
    if value in (None, ""):
        raise ValueError(f"Missing required config key: {key}")
    return value


def _resolve_path(raw_path: Any, key: str) -> Path:
    value = _require_config(raw_path, key)
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        raise FileNotFoundError(f"{key} not found: {path}")
    return path.resolve()
