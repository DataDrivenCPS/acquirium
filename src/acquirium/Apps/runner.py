from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import ray
import pyarrow as pa
import polars as pl
from rdflib import Graph, Literal, URIRef

from acquirium.Apps.base import App, Output, app_source_id
from acquirium.Apps.execution import ContinuousGuard, resolved_mappings, validate_outputs
from acquirium.Apps.input_batch import InputBatch
from acquirium.Apps.mapped import MappedApp, MappedStream, StreamMapping
from acquirium.Apps.output_emission import normalize_trigger_url
from acquirium.internals._log import configure_logging, timed_debug as _timed_debug
from acquirium.internals.app_utils import app_uri_for, app_type_uri, add_literal_or_uri
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import AppOutputSpec, AppSpec, compute_ref_uri
from acquirium.Storage.continuous.types import MUTATION_SCHEMA
from acquirium.Storage.values import split_value

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium
    from acquirium.Drivers.Driver import Driver

logger = logging.getLogger("acquirium.apps.runner")

# Consecutive process_pending failures (transform raising deterministically)
# before an app is marked 'failed' and dispatch stops until reset
# (continuous_batch_plan.md Phase 3a).
MAX_CONSECUTIVE_FAILURES = 3


@ray.remote
class AppRunner:
    """One Ray actor per registered app; owns that app's lifecycle.

    Constructed with the app's :class:`AppSpec` (which carries the app's
    Python source). ``register()`` persists the source under the app storage
    dir and writes the app's registration graph back to the server. Once
    started (:meth:`start`), the actor is driven entirely by
    ``process_pending`` turns dispatched by the server's ``ChangeRouter``;
    there is no keep-alive loop or per-run scheduling.
    """

    def __init__(
        self,
        spec: AppSpec,
        app_storage_root: Path,
        acquirium_cli: "Acquirium",
    ):
        # Ray workers don't inherit the server process's logging config.
        configure_logging()
        self.spec = spec
        # The app's RDF registration/build state has a separate owner from
        # output stream data. Expose it on both this actor and loaded App.
        self.source_id = app_source_id(spec.name)
        self.app_storage_root = Path(app_storage_root)
        self.acquirium_cli = acquirium_cli
        self.logger = logging.getLogger(f"acquirium.app.{spec.name}")

        # Populated by setup(): the loaded App instance, its resolved query
        # bundle, and whatever build_app() returns (e.g. a trained model).
        self.app: Any | None = None
        self.query: Any | None = None
        self.queries: dict[str, Any] = {}
        self.state: Any | None = None
        self.source_version = 0
        self._params: dict[str, Any] = {}
        self._build_status = "pending"

        # Continuous-batch runtime state, all resolved once in setup() (and
        # refreshed only on an explicit source-version change) rather than
        # re-read live every turn -- continuous transforms must not depend
        # on unversioned reads.
        self._mappings: dict[str, StreamMapping] = {}
        self._output_ref_uris: list[str] = []
        self._run_params: dict[str, Any] = {}
        self._consecutive_failures = 0
        self._active_bootstrap_id: str | None = None

    @staticmethod
    def _safe_entry_file(entry_file: str | None) -> str:
        ef = (entry_file or "app.py").replace("\\", "/")
        if ef.startswith("/") or ".." in ef.split("/"):
            ef = "app.py"
        return ef

    def _persist_source(self) -> None:
        """Write the shipped app source (and load metadata) under the app dir."""
        entry_file = self._safe_entry_file(self.spec.entry_file)
        app_dir = self.app_storage_root / self.spec.name
        app_dir.mkdir(parents=True, exist_ok=True)
        if self.spec.source_code:
            (app_dir / entry_file).write_text(self.spec.source_code)
        meta = {
            "entry_file": entry_file,
            "app_class": self.spec.app_class,
            "source_spec": self.spec.source_spec,
        }
        (app_dir / "app.json").write_text(
            json.dumps(meta, ensure_ascii=True, sort_keys=True)
        )

    def register(self) -> dict[str, Any]:
        """Persist the app's source and write its registration graph.

        Called synchronously (``ray.get``) by :class:`AppSupervisor` so the
        graph write completes — and races with other apps' writes are
        serialized by the supervisor's lock — before registration returns.
        """
        self._persist_source()
        graph = self._app_spec_graph(self.spec)
        self.insert_graph(
            graph.serialize(format="turtle"),
            format="turtle",
            replace=False,
        )
        self.logger.info(
            "Registered app '%s' (%d output stream(s))",
            self.spec.name, len(self.spec.outputs),
        )
        return {
            "name": self.spec.name,
            "outputs": [o.point_uri for o in self.spec.outputs],
        }

    def deregister(self) -> dict[str, Any]:
        """Inverse of :meth:`register`: strip this app's registration triples.

        Removes every triple describing the app node, the virtual points it
        produces, and those points' external references, then (server-side)
        advances the source generation so keep-alive workers rebuild. Driven only by
        the app URI, so it also cleans up triples the build phase may have
        added on the points, not just what register() wrote.
        """
        app_uri = app_uri_for(self.spec.name)
        query = f"""
        DELETE {{
          ?app ?ap ?ao .
          ?point ?pp ?po .
          ?ref ?rp ?ro .
        }} WHERE {{
          VALUES ?app {{ <{app_uri}> }}
          {{ ?app ?ap ?ao . }}
          UNION {{ ?app <{PRODUCES}> ?point . ?point ?pp ?po . }}
          UNION {{
            ?app <{PRODUCES}> ?point .
            ?point <{HAS_EXTERNAL_REFERENCE}> ?ref .
            ?ref ?rp ?ro .
          }}
        }}
        """
        self.sparql_update(query)
        self.logger.info("Deregistered app '%s' from the graph", self.spec.name)
        return {"name": self.spec.name}

    def insert_graph(self, rdf_graph: str, *, format: str = "turtle", replace: bool = False) -> None:
        """Write RDF to this app's graph; ownership is never caller-selected."""
        self.acquirium_cli.insert_graph(
            rdf_graph,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def insert_graph_file(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        replace: bool = False,
    ) -> None:
        """Read an RDF file into this app's graph; ownership is fixed."""
        self.acquirium_cli.insert_graph_file(
            path,
            format=format,
            replace=replace,
            source_id=self.source_id,
        )

    def sparql_update(self, update: str) -> dict[str, Any]:
        """Apply an update only to this app's graph."""
        return self.acquirium_cli.sparql_update(update, source_id=self.source_id)

    def _app_spec_graph(self, spec: AppSpec) -> Graph:
        app_uri = URIRef(app_uri_for(spec.name))
        source_id = app_source_id(spec.name)
        graph = Graph()

        graph.add((app_uri, RDF.type, APP))
        graph.add((app_uri, RDFS.label, Literal(spec.name)))
        if spec.app_type:
            graph.add((app_uri, RDF.type, app_type_uri(spec.app_type)))

        if spec.version:
            graph.add((app_uri, HAS_VERSION, Literal(spec.version)))
        if spec.queries:
            graph.add((app_uri, APP_QUERY, Literal(json.dumps(spec.queries, sort_keys=True, ensure_ascii=True))))
        if spec.params:
            graph.add((app_uri, APP_PARAMS, Literal(json.dumps(spec.params, sort_keys=True, ensure_ascii=True))))

        for dep in spec.depends_on:
            graph.add((app_uri, DEPENDS_ON, URIRef(dep)))

        for out in spec.outputs:
            point_uri = URIRef(out.point_uri)
            ref_name = out.ref_name or out.point_uri
            ref_uri = compute_ref_uri(source_id, ref_name)

            graph.add((app_uri, PRODUCES, point_uri))
            graph.add((point_uri, RDF.type, VIRTUAL_POINT))
            graph.add((point_uri, HAS_EXTERNAL_REFERENCE, ref_uri))
            graph.add((ref_uri, ACQUIRIUM_SOURCE_ID, Literal(source_id)))
            graph.add((ref_uri, ACQUIRIUM_REF_NAME, Literal(ref_name)))
            graph.add((ref_uri, RDF.type, STREAM))
            if out.kind in {"event", "trigger"}:
                graph.add((ref_uri, RDF.type, EVENT_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal(out.value_kind or "text")))
            else:
                graph.add((ref_uri, RDF.type, TIMESERIES_STREAM))
                graph.add((ref_uri, ACQUIRIUM_VALUE_KIND, Literal(out.value_kind or "numeric")))

            graph.add((ref_uri, STORAGE_BACKEND, Literal(out.storage_backend or "timescale")))

            add_literal_or_uri(graph, point_uri, HAS_QUANTITY_KIND, out.quantity_kind)
            add_literal_or_uri(graph, point_uri, HAS_UNIT, out.unit)
            add_literal_or_uri(graph, point_uri, DATA_SOURCE, out.data_source)
            for dep in out.depends_on or spec.depends_on:
                graph.add((point_uri, IS_CALCULATED_FROM, URIRef(dep)))
        return graph

    # ─────────────────────── build phase ───────────────────────

    def _load_app(self):
        """Load the App class from the persisted source and instantiate it.

        The client ships ``source_code`` + ``app_class``; ``register()`` wrote
        both to the app dir. We import that file and pick the class by name
        (falling back to the sole App subclass if no name was recorded).
        """
        app_dir = self.app_storage_root / self.spec.name
        entry_file = self.spec.entry_file
        app_class = self.spec.app_class
        meta_path = app_dir / "app.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                entry_file = entry_file or meta.get("entry_file")
                app_class = app_class or meta.get("app_class")
            except Exception:
                self.logger.warning("Failed to read %s", meta_path, exc_info=True)

        path = app_dir / self._safe_entry_file(entry_file)
        # Make the app dir importable so multi-file apps resolve siblings.
        if str(app_dir) not in sys.path:
            sys.path.insert(0, str(app_dir))

        module_spec = importlib.util.spec_from_file_location(
            f"acquirium_app_{self.spec.name}", str(path)
        )
        if module_spec is None or module_spec.loader is None:
            raise ValueError(f"Unable to load app file {path}")
        module = importlib.util.module_from_spec(module_spec)
        # register_pickle_by_value (below) requires the module to be reachable
        # through sys.modules under its own name.
        sys.modules[module_spec.name] = module
        module_spec.loader.exec_module(module)
        # The app class is defined in this dynamically-loaded module; pin it
        # to pickle by value so a heavy-parallel app's sharded Ray tasks
        # (App.parallelism > 1) can ship the class intact.
        ray.cloudpickle.register_pickle_by_value(module)

        if app_class:
            cls = getattr(module, app_class, None)
            if cls is None:
                raise ValueError(f"App class {app_class!r} not found in {path}")
        else:
            candidates = [
                obj for obj in vars(module).values()
                if isinstance(obj, type) and issubclass(obj, App) and obj is not App
            ]
            if not candidates:
                raise ValueError(f"No App subclass found in {path}")
            cls = candidates[0]

        self.app = cls()
        self.app.validate_definition()
        self.app._bind_graph_api(self.acquirium_cli, self.source_id)
        self.logger.info("Loaded app '%s' (%s)", self.spec.name, cls.__name__)
        return self.app

    def _make_context(self, *, params: dict[str, Any], inputs: InputBatch | None = None):
        from acquirium.internals.models import AppContext

        return AppContext(
            app_id=self.spec.name,
            started_at=datetime.now(timezone.utc),
            start=None,
            end=None,
            query=self.query,
            params=params or {},
            queries=self.queries,
            state=self.state,
            inputs=inputs,
        )

    def build_query(self) -> None:
        """Resolve the app's query bundle against the current graph and cache it."""
        if self.app is None:
            raise RuntimeError("build_query called before the app was loaded")
        bundle = self.app.build_query(self.acquirium_cli)
        if isinstance(bundle, dict):
            self.queries = bundle
            self.query = bundle.get("default") or (
                next(iter(bundle.values())) if bundle else None
            )
        else:
            self.query = bundle
            self.queries = {"default": bundle}
        self.logger.info(
            "Built %d query/queries for app '%s'", len(self.queries), self.spec.name
        )

    def _sync_dynamic_outputs(self) -> None:
        """Discover and register newly matched outputs of a mapped app."""
        if self.app is None:
            return
        resolver = getattr(self.app, "resolve_output_specs", None)
        if not callable(resolver):
            return
        from acquirium.Apps.execution import normalize_output_specs

        resolved = normalize_output_specs(resolver(self.queries))
        existing = {out.point_uri for out in self.spec.outputs}
        additions = [out for out in resolved if out.point_uri not in existing]
        if not additions:
            return
        self.spec.outputs.extend(additions)
        self.spec.depends_on = sorted({
            *self.spec.depends_on,
            *(dep for out in additions for dep in out.depends_on),
        })
        # Registration is additive: historical derived streams remain valid
        # even if a later graph version no longer matches their input.
        self.insert_graph(
            self._app_spec_graph(self.spec).serialize(format="turtle"),
            format="turtle",
            replace=False,
        )
        self.logger.info(
            "Registered %d newly mapped output stream(s) for '%s'",
            len(additions),
            self.spec.name,
        )

    def build_app(self) -> None:
        """Run the app's one-time build phase and cache whatever it returns.

        This is where a stateful app does expensive setup (e.g. training a
        model). The return value is held on the actor as ``self.state`` for
        every process_pending turn to reuse.
        """
        if self.app is None:
            raise RuntimeError("build_app called before the app was loaded")
        ctx = self._make_context(params=self._params)
        with _timed_debug(self.logger, "build_app app=%s", self.spec.name):
            self.state = self.app.build_app(ctx)
        self.logger.info(
            "build_app complete for '%s' (state=%s)",
            self.spec.name,
            type(self.state).__name__ if self.state is not None else "None",
        )

    def _resolve_mappings(self) -> None:
        """Resolve this app's input ref_uris -> StreamMapping, and its
        output ref_uris -- cached on the actor and reused for every
        process_pending turn (never re-queried live: continuous transforms
        must not depend on unversioned reads, and this is topology, not
        data). Prefers the app's own ``resolve_mappings`` (MappedApp's
        bindings already carry a resolved ref_uri); a plain App's declared
        ``depends_on`` point_uris are resolved to storage keys via one
        batched call.
        """
        raw = resolved_mappings(self.app, self.queries)
        unresolved_points = sorted({m.input_point_uri for m in raw if m.input_ref_uri is None})
        resolved_keys: dict[str, str] = {}
        if unresolved_points:
            resolved_keys = self.acquirium_cli.client.resolve_storage_keys(unresolved_points)

        mappings: dict[str, StreamMapping] = {}
        for m in raw:
            ref_uri = m.input_ref_uri or resolved_keys.get(m.input_point_uri)
            if ref_uri is None:
                self.logger.warning(
                    "Could not resolve input %s to a storage key; it will not be subscribed",
                    m.input_point_uri,
                )
                continue
            mappings[ref_uri] = m if m.input_ref_uri else StreamMapping(
                input_point_uri=m.input_point_uri,
                input_ref_uri=ref_uri,
                output_point_uri=m.output_point_uri,
                output_ref_name=m.output_ref_name,
                input_unit=m.input_unit,
            )
        self._mappings = mappings
        self._output_ref_uris = sorted({
            str(compute_ref_uri(self.source_id, out.ref_name or out.point_uri))
            for out in self.spec.outputs
        })

    def setup(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Load the app and run its build phase (build_query + build_app).

        Called once and serialized under the supervisor lock so build-time
        graph reads/writes don't race. The resolved query bundle, mapping
        table, and any state produced by ``build_app`` are cached on the
        actor for every process_pending turn.

        Params default to those registered with the app (``spec.params``), so
        the build phase sees the same configuration after a server restart
        restores the app from the graph.
        """
        self._params = params if params is not None else dict(self.spec.params)
        self._load_app()
        # The shipped source is authoritative for runtime validation. This is
        # especially important for apps restored from older registration
        # graphs, where trigger and event declarations were indistinguishable.
        from acquirium.Apps.execution import output_specs
        source_outputs = output_specs(self.app)
        if self.spec.source_code is None and source_outputs:
            self.spec.outputs = source_outputs
        self.build_query()
        self._sync_dynamic_outputs()
        self.build_app()
        self._resolve_mappings()
        # Seed the source generation the query was built against so process_pending
        # can detect a stale query after later graph mutations.
        try:
            self.source_version = int(self.acquirium_cli.graph_status()["source_version"])
        except Exception:
            self.source_version = 0
        self._build_status = "ready"
        return {
            "name": self.spec.name,
            "queries": list(self.queries.keys()),
            "state": type(self.state).__name__ if self.state is not None else None,
        }

    # ─────────────────────── lifecycle ───────────────────────

    async def start(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Begin (or resume) continuous execution.

        Determines from durable state whether to bootstrap (never started,
        or a reconciled-away generation), resume (a retained cursor), or
        reconcile (a stopped cursor whose manifests were compacted away) --
        continuous_batch.md's ``start_app``. Idempotent: called again while
        already active/bootstrapping just confirms the current state.
        """
        self._run_params = params or {}
        if self.app is None:
            await asyncio.to_thread(self.setup, dict(self.spec.params))

        runtime = await asyncio.to_thread(self.acquirium_cli.client.app_runtime, self.spec.name)
        if runtime is None:
            raise RuntimeError(f"app {self.spec.name!r} has no runtime state; register it first")
        if runtime["status"] in ("active", "bootstrapping"):
            return {"name": self.spec.name, "status": runtime["status"], "generation": runtime["generation"]}

        generation = runtime["generation"]
        resume = await asyncio.to_thread(
            self.acquirium_cli.client.resume_status, self.spec.name, generation
        )
        if resume["has_subscriptions"] and not resume["resumable"]:
            reset = await asyncio.to_thread(self.acquirium_cli.client.reset_app_runtime, self.spec.name)
            generation = reset["generation"]
            resume["has_subscriptions"] = False  # a fresh generation starts with none

        if resume["has_subscriptions"]:
            await asyncio.to_thread(self.acquirium_cli.client.set_app_status, self.spec.name, "active")
            self._consecutive_failures = 0
            return {"name": self.spec.name, "status": "active", "generation": generation}

        state = await asyncio.to_thread(
            self.acquirium_cli.client.begin_bootstrap,
            self.spec.name,
            input_ref_uris=list(self._mappings.keys()),
            output_ref_uris=self._output_ref_uris,
        )
        self._active_bootstrap_id = state["bootstrap_id"]
        self._consecutive_failures = 0
        return {"name": self.spec.name, "status": "bootstrapping", "generation": state["generation"]}

    def request_stop(self) -> dict[str, Any]:
        """Durably stop this app at the next transaction boundary.

        Ray serializes calls to one actor (no ``max_concurrency`` is
        declared), so no ``process_pending`` turn is ever concurrently in
        flight while this method runs -- by the time it executes, the actor
        is already at a safe boundary.
        """
        self.acquirium_cli.client.set_app_status(self.spec.name, "stopping")
        self.acquirium_cli.client.set_app_status(self.spec.name, "stopped")
        self.logger.info("App '%s' stopped", self.spec.name)
        return {"name": self.spec.name, "stopped": True}

    def request_reset(self) -> dict[str, Any]:
        """Start a new generation and begin reconciling from canonical
        history (explicit reset, topology replacement, or code replace)."""
        reset = self.acquirium_cli.client.reset_app_runtime(self.spec.name)
        self._active_bootstrap_id = None
        self._consecutive_failures = 0
        state = self.acquirium_cli.client.begin_bootstrap(
            self.spec.name,
            input_ref_uris=list(self._mappings.keys()),
            output_ref_uris=self._output_ref_uris,
        )
        self._active_bootstrap_id = state["bootstrap_id"]
        self.logger.info("App '%s' reset to generation %d", self.spec.name, reset["generation"])
        return {"name": self.spec.name, "status": "bootstrapping", "generation": state["generation"]}

    # ─────────────────────── continuous processing ───────────────────────

    async def process_pending(self) -> dict[str, Any]:
        """One continuous-batch turn: fetch the next pending batch (tail or
        bootstrap page), transform it, and commit.

        Returns ``{"processed": int, "has_more": bool, "status": str}``; the
        router re-dispatches while ``has_more`` is True. The normal path
        creates no stateless compute or separate commit task -- only one
        batch is in flight at a time, and this coroutine yields at each
        ``await`` for stop/status/recovery.
        """
        runtime = await asyncio.to_thread(self.acquirium_cli.client.app_runtime, self.spec.name)
        if runtime is None:
            return {"processed": 0, "has_more": False, "status": "unregistered"}
        status = runtime["status"]

        if status == "bootstrapping":
            return await self._process_bootstrap_turn(runtime)
        if status != "active":
            return {"processed": 0, "has_more": False, "status": status}

        await self._maybe_refresh_query()

        batch = await asyncio.to_thread(
            self.acquirium_cli.client.next_app_batch, self.spec.name, runtime["generation"]
        )
        if batch is None:
            return {"processed": 0, "has_more": False, "status": "active"}

        try:
            outputs_table, webhook_intents = self._transform_tail_batch(batch)
        except Exception:
            self.logger.exception(
                "process_pending: transform failed for tail batch %s", batch["batch_id"]
            )
            return await self._record_failure()

        await asyncio.to_thread(
            self.acquirium_cli.client.commit_app_batch,
            self.spec.name,
            batch["batch_id"],
            generation=runtime["generation"],
            batch_kind="tail",
            rows=outputs_table,
            inputs=batch["inputs"],
            webhook_intents=webhook_intents,
        )
        self._consecutive_failures = 0
        return {"processed": batch["rows"].num_rows, "has_more": batch["has_more"], "status": "active"}

    async def _process_bootstrap_turn(self, runtime: dict[str, Any]) -> dict[str, Any]:
        batch = await asyncio.to_thread(
            self.acquirium_cli.client.next_app_batch, self.spec.name, runtime["generation"]
        )
        if batch is None:
            # Staging exhausted by the last committed page -- finalize now,
            # atomically replacing this app's output streams and going
            # active. Report has_more so the router immediately re-dispatches
            # into what is now the tail path.
            if self._active_bootstrap_id is not None:
                await asyncio.to_thread(
                    self.acquirium_cli.client.finalize_bootstrap, self._active_bootstrap_id
                )
                self._active_bootstrap_id = None
            return {"processed": 0, "has_more": True, "status": "bootstrapping"}

        self._active_bootstrap_id = batch["bootstrap_id"]
        try:
            outputs_table = self._transform_bootstrap_page(batch)
        except Exception:
            self.logger.exception(
                "process_pending: transform failed for bootstrap page %s", batch["batch_id"]
            )
            return await self._record_failure()

        await asyncio.to_thread(
            self.acquirium_cli.client.commit_app_batch,
            self.spec.name,
            batch["batch_id"],
            generation=runtime["generation"],
            batch_kind="bootstrap",
            rows=outputs_table,
            bootstrap_id=batch["bootstrap_id"],
            end_ordinal=batch["end_ordinal"],
        )
        self._consecutive_failures = 0
        return {"processed": batch["rows"].num_rows, "has_more": True, "status": "bootstrapping"}

    async def _record_failure(self) -> dict[str, Any]:
        """A batch that raised deterministically must not hot-loop: after
        ``MAX_CONSECUTIVE_FAILURES`` the app is marked failed and dispatch
        stops until an explicit reset."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            await asyncio.to_thread(self.acquirium_cli.client.set_app_status, self.spec.name, "failed")
            self.logger.error(
                "App '%s' marked failed after %d consecutive batch failures",
                self.spec.name, self._consecutive_failures,
            )
            return {"processed": 0, "has_more": False, "status": "failed"}
        return {"processed": 0, "has_more": False, "status": "active"}

    async def _maybe_refresh_query(self) -> None:
        """Rebuild the query bundle and mappings when the server's source
        generation has advanced since setup/the last refresh -- at most once
        per turn, mirroring the old keep-alive loop's poll but now folded
        into process_pending. A refresh failure keeps the previous query
        rather than skip the batch.
        """
        try:
            source_version = int(self.acquirium_cli.graph_status()["source_version"])
        except Exception:
            self.logger.exception("query refresh: graph_status failed; keeping previous query")
            return
        if source_version == self.source_version:
            return
        self.source_version = source_version
        try:
            self.build_query()
            self._sync_dynamic_outputs()
            self._resolve_mappings()
        except Exception:
            self.logger.exception("query refresh failed; keeping previous query")

    def _run_transform(self, input_batch: InputBatch, *, allow_triggers: bool) -> list[Output]:
        """Run the app's transform against one pinned InputBatch, bound to a
        ContinuousGuard so it cannot mutate storage or read live timeseries
        outside ``ctx.inputs``.

        MappedApp gets the continuous per-stream dispatch (upserts +
        propagated deletes); any other App gets ``ctx.inputs`` and its
        ordinary ``run(ctx)``. Trigger outputs are dropped during a
        bootstrap page (reprocessing history should not fire webhooks).
        """
        ctx = self._make_context(params=self._run_params, inputs=input_batch)
        guard = ContinuousGuard(self.acquirium_cli)
        self.app._bind_graph_api(guard, self.source_id)
        try:
            if isinstance(self.app, MappedApp):
                outputs = self._transform_mapped(input_batch, ctx)
            else:
                outputs = self.app.run(ctx)
        finally:
            self.app._bind_graph_api(self.acquirium_cli, self.source_id)

        validated = validate_outputs(outputs, self.spec.outputs)
        if not allow_triggers:
            triggers = [o for o in validated if o.kind == "trigger"]
            if triggers:
                self.logger.warning(
                    "Dropping %d trigger output(s) produced while reprocessing history", len(triggers)
                )
            validated = [o for o in validated if o.kind != "trigger"]
        return validated

    def _transform_mapped(self, batch: InputBatch, ctx) -> list[Output]:
        outputs: list[Output] = []
        for ref_uri in batch.ref_uris():
            mapping = self._mappings.get(ref_uri)
            if mapping is None:
                self.logger.warning(
                    "process_pending: batch touched unmapped ref %s; skipping", ref_uri
                )
                continue

            upserts = batch.upserts_frame(ref_uri, cast_value=self.app.cast_value)
            if upserts.height:
                stream = MappedStream(
                    input_alias=self.app.input_alias,
                    input_point_uri=mapping.input_point_uri,
                    input_ref_uri=ref_uri,
                    input_unit=mapping.input_unit,
                    output_point_uri=mapping.output_point_uri,
                    output_ref_name=mapping.output_ref_name,
                    values=upserts,
                )
                transformed = self.app.transform(stream, ctx)
                out = self.app.wrap_transform_result(stream, transformed)
                if out is not None:
                    outputs.append(out)

            deleted = batch.delete_timestamps(ref_uri)
            if deleted:
                delete_stream = MappedStream(
                    input_alias=self.app.input_alias,
                    input_point_uri=mapping.input_point_uri,
                    input_ref_uri=ref_uri,
                    input_unit=mapping.input_unit,
                    output_point_uri=mapping.output_point_uri,
                    output_ref_name=mapping.output_ref_name,
                    values=None,
                )
                outputs.extend(self.app.resolve_deletes(delete_stream, deleted))
        return outputs

    def _transform_tail_batch(self, batch: dict[str, Any]) -> tuple[pa.Table, list[dict[str, Any]]]:
        input_batch = InputBatch.from_arrow(batch["rows"])
        outputs = self._run_transform(input_batch, allow_triggers=True)
        return self._mutations_from_outputs(outputs)

    def _transform_bootstrap_page(self, batch: dict[str, Any]) -> pa.Table:
        input_batch = InputBatch.from_arrow(batch["rows"])
        outputs = self._run_transform(input_batch, allow_triggers=False)
        table, _ = self._mutations_from_outputs(outputs)
        return table

    @staticmethod
    def _to_utc(ts: datetime) -> datetime:
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)

    def _mutations_from_outputs(
        self, outputs: list[Output]
    ) -> tuple[pa.Table, list[dict[str, Any]]]:
        """Convert validated Output objects into a MUTATION_SCHEMA table
        (ready to commit as this batch's outputs) plus any webhook intents."""
        spec_by_point = {o.point_uri: o for o in self.spec.outputs}
        op_col: list[str] = []
        ref_col: list[str] = []
        ts_col: list[datetime] = []
        num_col: list[float | None] = []
        txt_col: list[str | None] = []
        webhook_intents: list[dict[str, Any]] = []

        def add(op: str, ref_uri: str, ts: datetime, numeric: float | None, text: str | None) -> None:
            op_col.append(op)
            ref_col.append(ref_uri)
            ts_col.append(self._to_utc(ts))
            num_col.append(numeric)
            txt_col.append(text)

        for out in outputs:
            if out.kind == "timeseries":
                point_uri = out.payload["point_uri"]
                ref_name = out.payload.get("ref_name") or point_uri
                ref_uri = str(compute_ref_uri(self.source_id, ref_name))
                spec = spec_by_point.get(point_uri)
                value_kind = spec.value_kind if spec is not None else None
                for ts, value in out.payload["rows"]:
                    numeric_value, text_value = split_value(value, value_kind)
                    add("upsert", ref_uri, ts, numeric_value, text_value)
            elif out.kind == "event":
                point_uri = out.payload["point_uri"]
                ref_uri = str(compute_ref_uri(self.source_id, point_uri))
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                text_value = json.dumps(
                    {
                        "severity": out.payload.get("severity", "INFO"),
                        "message": out.payload.get("message"),
                        "data": out.payload.get("data") or {},
                    },
                    ensure_ascii=True,
                )
                add("upsert", ref_uri, ts, None, text_value)
            elif out.kind == "delete":
                point_uri = out.payload["point_uri"]
                ref_name = out.payload.get("ref_name") or point_uri
                ref_uri = str(compute_ref_uri(self.source_id, ref_name))
                for ts in out.payload["timestamps"]:
                    add("delete", ref_uri, ts, None, None)
            elif out.kind == "trigger":
                url = normalize_trigger_url(out.payload["url"])
                ts = out.payload.get("ts") or datetime.now(timezone.utc)
                webhook_intents.append({
                    "url": url,
                    "payload": {
                        "message": out.payload.get("message"),
                        "ts": self._to_utc(ts).isoformat(),
                    },
                })

        if not op_col:
            table = pa.table(
                {"operation": [], "ref_uri": [], "ts": [], "numeric_value": [], "text_value": []},
                schema=MUTATION_SCHEMA,
            )
            return table, webhook_intents

        df = pl.DataFrame(
            {
                "operation": pl.Series(op_col, dtype=pl.Utf8),
                "ref_uri": pl.Series(ref_col, dtype=pl.Utf8),
                "ts": pl.Series(ts_col, dtype=pl.Datetime("us", "UTC")),
                "numeric_value": pl.Series(num_col, dtype=pl.Float64),
                "text_value": pl.Series(txt_col, dtype=pl.Utf8),
            }
        )
        return df.to_arrow(), webhook_intents

    def status(self) -> dict[str, Any]:
        """Report build/run status for this app (the actor answers directly)."""
        return {
            "name": self.spec.name,
            "spec": self.spec.source_spec,
            "build": self._build_status,
            "queries": list(self.queries.keys()),
            "state": type(self.state).__name__ if self.state is not None else None,
            "mappings": [
                {
                    "input_ref_uri": ref_uri,
                    "input_point_uri": mapping.input_point_uri,
                    "output_point_uri": mapping.output_point_uri,
                    "output_ref_name": mapping.output_ref_name,
                }
                for ref_uri, mapping in self._mappings.items()
            ],
            "consecutive_failures": self._consecutive_failures,
        }
