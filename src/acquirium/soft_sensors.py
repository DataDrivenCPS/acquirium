from __future__ import annotations

import importlib
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Iterable
import types
import hashlib
import json
import logging

import polars as pl
from contextlib import nullcontext

from typing import Any

from acquirium.Internals.models import SoftSensorSpec
from acquirium.Storage import TimeseriesStore
from acquirium.Storage.graph_store import OxigraphGraphStore

_CURRENT_RUNNER: "SoftSensorRunner | None" = None
_CURRENT_REGISTERED: list[str] | None = None
_CURRENT_REGISTRY_PATH: Path | None = None


class SoftSensorRunner:
    """Executes registered soft sensors locally using TimescaleDB views as inputs."""

    def __init__(self, timescale: TimeseriesStore, graph: OxigraphGraphStore, module_dir: str | Path | None = None):
        self.timescale = timescale
        self.graph = graph
        self.module_dir = Path(module_dir).resolve() if module_dir else None
        self._callable_cache: dict[str, Any] = {}

        if self.module_dir:
            # Ensure the directory exists so users can drop files into it while the server is running.
            self.module_dir.mkdir(parents=True, exist_ok=True)
            self._ensure_sys_path(self.module_dir)

    def load_registry(self) -> None:
        """Load and register soft sensors from a registry package inside module_dir.

        Expected layout:
        - `<module_dir>/soft_sensors/` (a package) containing any number of modules.
        - Within that package, we look for `SOFT_SENSORS` lists and/or a callable `register(register_fn)`.
        This allows definitions to be split across multiple files.
        """

        if not self.module_dir:
            return

        # Ensure we don't reuse stale bytecode when registry files are rewritten on disk.
        self._purge_bytecode(self.module_dir)

        import logging
        started = datetime.now(timezone.utc)
        logging.info("acquirium: loading soft sensor registry from %s", self.module_dir)
        registered: list[str] = []
        existing = {spec.uri for spec in self.list_specs()}

        pkg_dir = self.module_dir / "soft_sensors"
        self._ensure_sys_path(self.module_dir)
        importlib.invalidate_caches()

        if pkg_dir.is_dir():
            for py_file in pkg_dir.glob("*.py"):
                module_name = f"soft_sensors.{py_file.stem}"
                self._load_registry_module(module_name, py_file, registered)

        # Also load any .py files directly in module_dir (convenience / legacy)
        for py_file in self.module_dir.glob("*.py"):
            if py_file.name == "soft_sensor_registry.py":
                continue
            module_name = f"_soft_sensor_{py_file.stem}_{abs(hash(py_file))}"
            self._load_registry_module(module_name, py_file, registered)

        # Legacy single-file registry support
        registry_path = self.module_dir / "soft_sensor_registry.py"
        if registry_path.exists():
            module_name = f"_soft_sensor_registry_{abs(hash(registry_path))}"
            self._load_registry_module(module_name, registry_path, registered)

        # Remove any soft sensors that were registered previously but not this round.
        registered_set = set(registered)
        stale = existing - registered_set
        for uri in stale:
            self._remove_soft_sensor(uri)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds() * 1000
        logging.info(
            "acquirium: registry load completed new=%d stale=%d elapsed_ms=%.1f",
            len(registered_set - existing),
            len(stale),
            elapsed,
        )

    def _load_registry_module(self, module_name: str, path: Path, registered: list[str]) -> None:
        global _CURRENT_RUNNER, _CURRENT_REGISTERED
        before = len(registered)
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            return
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        global _CURRENT_RUNNER, _CURRENT_REGISTERED, _CURRENT_REGISTRY_PATH
        _CURRENT_RUNNER = self
        _CURRENT_REGISTERED = registered
        _CURRENT_REGISTRY_PATH = path
        try:
            spec.loader.exec_module(module)
        finally:
            _CURRENT_RUNNER = None
            _CURRENT_REGISTERED = None
            _CURRENT_REGISTRY_PATH = None

        # Only run the sensors that were registered by this module load.
        new_regs = registered[before:]
        if new_regs:
            self.run(new_regs)

    def _purge_bytecode(self, root: Path) -> None:
        """Delete __pycache__ and .pyc files under the registry directory to avoid stale loads."""
        for cache_dir in root.rglob("__pycache__"):
            shutil.rmtree(cache_dir, ignore_errors=True)
        for pyc in root.rglob("*.pyc"):
            try:
                pyc.unlink()
            except FileNotFoundError:
                pass

    def list_specs(self) -> list[SoftSensorSpec]:
        return [SoftSensorSpec(**row) for row in self.timescale.list_soft_sensors()]

    def get_spec(self, uri: str) -> SoftSensorSpec | None:
        row = self.timescale.get_soft_sensor(uri)
        return SoftSensorSpec(**row) if row else None

    def register(self, spec: SoftSensorSpec) -> None:
        # Validate import path eagerly
        fn = self._load_callable(spec.module_path)
        self._callable_cache.setdefault(spec.module_path, fn)
        self.timescale.ensure_stream_handle(spec.uri)
        version = self._compute_version(spec)
        self.timescale.upsert_soft_sensor(
            uri=spec.uri,
            module_path=spec.module_path,
            sources=spec.sources,
            params=spec.params,
            schedule=spec.schedule,
        )
        # Register as a virtual point in the graph
        self.graph.register_virtual_point(spec.uri, spec.sources)
        self.graph.upsert_soft_sensor_metadata(spec.uri, spec.sources, spec.module_path, version)
        import logging
        logging.info("acquirium: registered soft sensor uri=%s sources=%s module=%s", spec.uri, spec.sources, spec.module_path)

    def run(self, uris: list[str] | None = None) -> list[dict[str, Any]]:
        all_specs = {s.uri: s for s in self.list_specs()}
        requested = set(uris) if uris is not None else None
        order = self._topo_order(all_specs)
        if requested is not None:
            order = [u for u in order if u in requested]

        results: list[dict[str, Any]] = []
        sensor_ctx = getattr(self.timescale, "sensor_context", None)
        for uri in order:
            spec = all_specs[uri]
            started = datetime.now(timezone.utc)
            ctx = sensor_ctx() if sensor_ctx else nullcontext()
            with ctx:
                try:
                    self.timescale.begin()
                    rows_out = self._execute_spec(spec, started=started)
                    duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
                    self.timescale.record_soft_run(spec.uri, started, duration, rows_out, status="ok")
                    self.timescale.commit()
                    results.append({"uri": spec.uri, "inserted": rows_out, "rows": rows_out, "status": "ok"})
                except Exception as exc:  # pragma: no cover - defensive
                    self.timescale.rollback()
                    duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
                    # Persist failure log even if compute transaction failed
                    self.timescale.begin()
                    self.timescale.record_soft_run(spec.uri, started, duration, rows_out=0, status="error", message=str(exc))
                    self.timescale.commit()
                    results.append({"uri": spec.uri, "inserted": 0, "rows": 0, "status": "error", "error": str(exc)})
        return results

    def _topo_order(self, specs: dict[str, SoftSensorSpec]) -> list[str]:
        produced = set(specs.keys())
        adj: dict[str, list[str]] = {u: [] for u in produced}
        indeg: dict[str, int] = {u: 0 for u in produced}
        for spec in specs.values():
            for src in spec.sources:
                if src in produced:
                    adj[src].append(spec.uri)
                    indeg[spec.uri] += 1

        queue = [u for u, d in indeg.items() if d == 0]
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for v in adj.get(node, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    queue.append(v)

        if len(order) != len(produced):
            raise RuntimeError("Cycle detected in soft sensor dependencies")
        return order

    def run_scheduled(self, updated_sources: list[str] | None = None) -> list[dict[str, Any]]:
        """Run soft sensors in dependency order, only when stale.

        A sensor is dirty when:
        - Its computed version differs from stored HAS_VERSION
        - It has never run
        - Any upstream stream last_reported is newer than its last_run
        - An explicit updated source is listed (propagated downstream)
        """
        logging.info("acquirium: scheduled run invoked updated_sources=%s", updated_sources or [])
        specs = {s.uri: s for s in self.list_specs()}
        produced = set(specs.keys())

        # Build adjacency for topo sort (soft->soft)
        adj: dict[str, list[str]] = {u: [] for u in produced}
        indeg: dict[str, int] = {u: 0 for u in produced}
        for spec in specs.values():
            for src in spec.sources:
                if src in produced:
                    adj[src].append(spec.uri)
                    indeg[spec.uri] += 1

        # Topological order (Kahn)
        queue = [u for u, d in indeg.items() if d == 0]
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for v in adj.get(node, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    queue.append(v)
        if len(order) != len(produced):
            raise RuntimeError("Cycle detected in soft sensor dependencies")

        versions = self._soft_sensor_versions()
        dirty: set[str] = set()
        updated_set = set(updated_sources or [])
        for uri, spec in specs.items():
            version = versions.get(uri)
            stored_version = self.graph.get_soft_sensor_version(uri)
            last_run = self.graph.get_last_run(uri)
            if version != stored_version or last_run is None:
                dirty.add(uri)
                continue
            # data freshness
            for src in spec.sources:
                last_input = self.graph.get_last_input_change(src)
                if last_input and last_run and last_input > last_run:
                    dirty.add(uri)
                    break
                if src in updated_set:
                    dirty.add(uri)
                    break

        # propagate dirtiness downstream
        stack = list(dirty)
        while stack:
            cur = stack.pop()
            for v in adj.get(cur, []):
                if v not in dirty:
                    dirty.add(v)
                    stack.append(v)

        logging.info(
            "acquirium: scheduled plan total=%d dirty=%d order=%s",
            len(order),
            len(dirty),
            order,
        )

        results: list[dict[str, Any]] = []
        for uri in order:
            if uri not in dirty:
                continue
            spec = specs[uri]
            started = datetime.now(timezone.utc)
            try:
                self.timescale.begin()
                rows_out = self._execute_spec(spec, started=started)
                duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
                self.timescale.record_soft_run(spec.uri, started, duration, rows_out, status="ok")
                self.timescale.commit()
                logging.info(
                    "acquirium: ran soft sensor uri=%s rows=%d duration_ms=%.1f",
                    spec.uri,
                    rows_out,
                    duration,
                )
                results.append({"uri": spec.uri, "inserted": rows_out, "rows": rows_out, "status": "ok"})
                self.graph.upsert_soft_sensor_metadata(spec.uri, spec.sources, spec.module_path, versions.get(uri, ""))

            except Exception as exc:  # pragma: no cover
                self.timescale.rollback()
                duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
                self.timescale.begin()
                self.timescale.record_soft_run(spec.uri, started, duration, rows_out=0, status="error", message=str(exc))
                self.timescale.commit()
                results.append({"uri": spec.uri, "inserted": 0, "rows": 0, "status": "error", "error": str(exc)})
        return results

    # -------------------- internals --------------------
    def _execute_spec(self, spec: SoftSensorSpec, *, started: datetime | None = None) -> int:
        params = spec.params or {}
        tall_frames: list[pl.DataFrame] = []
        for src in spec.sources:
            rows = self.timescale.timeseries(src)
            if not rows:
                continue
            df = pl.DataFrame(
                {
                    "time": [r["ts"] for r in rows],
                    "value": [r["value"] for r in rows],
                    "source": [src] * len(rows),
                }
            )
            tall_frames.append(df)

        if tall_frames:
            tall = pl.concat(tall_frames)
        else:
            tall = pl.DataFrame({"time": [], "value": [], "source": []})

        if params.get("data_shape") == "narrow":
            combined = tall.rename({"source": "id"})
            if {"time", "value", "id"} <= set(combined.columns):
                combined = combined.select("time", "value", "id")
        else:
            if {"time", "value", "source"} <= set(tall.columns):
                combined = tall.pivot(values="value", index="time", on="source", aggregate_function="first")
            else:
                combined = pl.DataFrame({"time": []})

        fn = self._load_callable(spec.module_path)
        output = fn(df=combined, params=params)
        rows = self._normalize_output(output)
        if not rows:
            return 0
        inserted = self.timescale.replace_rows(spec.uri, rows)
        # update last_reported
        latest_ts = max(r[0] for r in rows)
        self.graph.record_last_reported(spec.uri, latest_ts)
        recorded_start = started or datetime.now(timezone.utc)
        self.graph.set_last_run(spec.uri, recorded_start)
        return inserted

    def _normalize_output(self, output: Any) -> list[tuple[datetime, Any]]:
        if output is None:
            return []
        if hasattr(output, "fetchall"):
            rows = output.fetchall()
        elif isinstance(output, pl.DataFrame):
            # Prefer time/value if present, otherwise flatten first non-time column
            if "time" in output.columns and "value" in output.columns:
                rows = list(zip(output.get_column("time"), output.get_column("value")))
            else:
                value_cols = [c for c in output.columns if c != "time"]
                if not value_cols:
                    return []
                col = value_cols[0]
                rows = list(zip(output.get_column("time"), output.get_column(col)))
        elif hasattr(output, "itertuples"):
            rows = [(getattr(r, "ts"), getattr(r, "value")) for r in output.itertuples(index=False)]
        else:
            rows = list(output)

        normalized: list[tuple[datetime, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                ts = row.get("ts") or row.get("time")
                val = row.get("value")
            else:
                ts, val = row[0], row[1]
            normalized.append((ts, val))
        return normalized

    def _remove_soft_sensor(self, uri: str) -> None:
        # Drop catalog entry, any stored rows, run history, and graph point.
        self.timescale.delete_soft_sensor(uri)
        self.graph.delete_point(uri)
    def _load_callable(self, path: str):
        if path in self._callable_cache:
            return self._callable_cache[path]
        module_name, func_name = path.split(":", 1)
        if self.module_dir:
            importlib.invalidate_caches()
            candidate = self.module_dir.joinpath(*module_name.split(".")).with_suffix(".py")
            if candidate.exists():
                module = sys.modules.get(module_name)
                if module is None:
                    module = types.ModuleType(module_name)
                    module.__file__ = str(candidate)
                    package, _, _ = module_name.rpartition(".")
                    module.__package__ = package or None
                    sys.modules[module_name] = module
                    source = candidate.read_text()
                    exec(compile(source, str(candidate), "exec"), module.__dict__)
                return getattr(module, func_name)

        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    def _literal(self, value: str) -> str:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    def _ensure_sys_path(self, path: Path) -> None:
        path_str = str(path)
        if path_str not in sys.path:
            # Prepend so the configured directory wins over site-packages for these modules.
            sys.path.insert(0, path_str)

    def _compute_version(self, spec: SoftSensorSpec) -> str:
        payload = {
            "module_path": spec.module_path,
            "params": spec.params or {},
        }
        digest = hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        return f"sha1:{digest}"

    def _soft_sensor_versions(self) -> dict[str, str]:
        versions = {}
        for spec in self.list_specs():
            versions[spec.uri] = self._compute_version(spec)
        return versions


def register_soft_sensor(
    fn,
    *,
    ids: list[str] | None = None,
    sparql: str | None = None,
    uri: str | None = None,
    params: dict | None = None,
    name: str | None = None,
    module_path: str | None = None,
    mode: str = "many-to-one",
    data_shape: str = "wide",
) -> list[str] | None:
    """Registry helper called inside soft_sensors modules.

    - ids: explicit list of source URIs/handles.
    - sparql: query returning sources in first column (evaluated at load time).
    - uri: optional explicit output URI; otherwise derived from function name + hash of sources.
    - params: passed to the sensor function.
    - name: optional slug used in generated URI.
    - mode: "many-to-one" (default) or "one-to-one" to create per-source outputs.
    - data_shape: "wide" (default) or "narrow" to control input dataframe layout.
    """
    if _CURRENT_RUNNER is None:
        raise RuntimeError("register_soft_sensor must be called during registry loading")

    sources: list[str] = []
    if sparql:
        res = _CURRENT_RUNNER.graph.sparql_query(sparql, use_union=True)
        rows = res.get("rows", []) if isinstance(res, dict) else []
        sources.extend([r[0] for r in rows])
    if ids:
        sources.extend(ids)

    seen = set()
    uniq_sources = []
    for s in sources:
        if s not in seen:
            uniq_sources.append(s)
            seen.add(s)

    if not uniq_sources:
        return None

    import hashlib

    slug = name or fn.__name__
    if module_path is None:
        module_name = fn.__module__
        if module_name.startswith("_soft_sensor_") and _CURRENT_REGISTRY_PATH:
            module_name = Path(_CURRENT_REGISTRY_PATH).stem
        module_path = f"{module_name}:{fn.__name__}"

    created: list[str] = []
    if mode == "one-to-one":
        base_params = params or {}
        if "data_shape" not in base_params:
            base_params = {**base_params, "data_shape": data_shape}
        for src in uniq_sources:
            suffix = hashlib.sha1(src.encode("utf-8")).hexdigest()[:8]
            target_uri = uri or f"virtual/{slug}_{suffix}"
            spec = SoftSensorSpec(
                uri=target_uri,
                sources=[src],
                module_path=module_path,
                params=base_params,
                schedule=None,
            )
            _CURRENT_RUNNER._callable_cache[module_path] = fn
            _CURRENT_RUNNER.register(spec)
            created.append(spec.uri)
            if _CURRENT_REGISTERED is not None:
                _CURRENT_REGISTERED.append(spec.uri)
    else:
        suffix = hashlib.sha1("|".join(uniq_sources).encode("utf-8")).hexdigest()[:8]
        target_uri = uri or f"virtual/{slug}_{suffix}"
        merged_params = dict(params) if params else {}
        merged_params.setdefault("data_shape", data_shape)
        spec = SoftSensorSpec(
            uri=target_uri,
            sources=uniq_sources,
            module_path=module_path,
            params=merged_params,
            schedule=None,
        )
        _CURRENT_RUNNER._callable_cache[module_path] = fn
        _CURRENT_RUNNER.register(spec)
        created.append(spec.uri)
        if _CURRENT_REGISTERED is not None:
            _CURRENT_REGISTERED.append(spec.uri)

    return created or None
