from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence, Callable, Optional
import inspect

from rdflib import Graph as RDFGraph, URIRef

import warnings

from acquirium.Client.query import Query
from acquirium.Client.client import AcquiriumClient
from acquirium.Apps.base import App
from acquirium.internals.app_utils import make_stream_ref_uri
from acquirium.internals.models import AppOutputSpec, AppSpec
@dataclass
class Acquirium:
    """
    High level entry point for Acquirium.

    This class is intended to be the **user-facing client API**. It connects to the server and exposes
    a small set of convenience methods that should feel natural for end users.

    """


    # ---------- construction ----------
    def __init__(
            self,
            server_url: str = "localhost",
            server_port: int = 8000,
            use_ssl: bool = False,
            lexicon_path: Optional[Path] = None,
        ) -> Acquirium:
        if lexicon_path is not None:
            warnings.warn(
                "lexicon_path is deprecated and ignored. "
                "Text resolution now uses server-side embeddings via /resolve_text.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.client = AcquiriumClient(
            server_url=server_url,
            server_port=server_port,
            use_ssl=use_ssl,
        )

    # ------------------------------------------------------------------
    # GRAPH API
    # ------------------------------------------------------------------

    def insert_graph(self, rdf_graph: str, format: str = "turtle", replace = True, wait_for_embedding: bool = False) -> None:
        """
        Insert RDF graph into the graph store to the main graph

        Args:
            :param rdf_graph: `pathlib.Path` like object, or string.
            In the case of a string the string it can be either:
                - graph content as text
                - location of the source file
            format: Format of the RDF data [turtle | n3 | xml | trix]
            replace: If True, replaces the existing main graph. If False, appends to it.
            wait_for_embedding: If True, blocks until the server finishes rebuilding
                the embedding index. Default False (background rebuild).
        """
        self.client.insert_graph(rdf_graph, format=format, replace=replace, wait_for_embedding=wait_for_embedding)

    def query(self) -> Query:
        """Create a new empty Query bound to this Acquirium instance."""
        return Query(client=self.client)

    def find_entity(
        self,
        *,
        _class: Optional[str] = None,
        alias: Optional[str] = None,
        uri: str | URIRef | None = None,
    ) -> "Query":
        q = Query(client=self.client).find_entity(_class=_class, alias=alias, uri=uri)
        return q
    
    def find_all_data(self, *, _class: Optional[str] = None, uri: str | URIRef | None = None) -> "Query":
        q = Query(client=self.client).find_all_data(_class=_class, uri=uri)
        return q

    # ------------------------------------------------------------------
    # TIMESERIES API
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # ACQUIRIUM APPS API
    # ------------------------------------------------------------------

    def register_app(
        self,
        app: App,
        *,
        app_type: str | None = None,
        docker_image: str | None = None,
        entrypoint: str | None = None,
        command: str | None = None,
        outputs: list[AppOutputSpec | dict[str, Any]] | None = None,
        depends_on: list[str] | None = None,
        resolve_dependencies: bool = True,
        queries: dict[str, Query] | None = None,
        source_code: str | None = None,
        entry_file: str | None = None,
    ) -> dict[str, Any]:
        """Register an Acquirium App with the server."""
        query_bundle = queries if queries is not None else app.build_query(self)
        if isinstance(query_bundle, Query):
            query_bundle = {"default": query_bundle}

        query_specs = {name: q.to_dict() for name, q in query_bundle.items()}

        deps = depends_on or []
        if not depends_on and resolve_dependencies:
            dep_set: set[str] = set()
            for q in query_bundle.values():
                dep_set.update(q.resolved_nodes())
            deps = sorted(dep_set)

        output_specs: list[AppOutputSpec] = []
        output_items = outputs if outputs is not None else list(getattr(app, "outputs", []) or [])
        for item in output_items:
            if isinstance(item, AppOutputSpec):
                spec_item = item
            elif isinstance(item, dict):
                spec_item = AppOutputSpec(**item)
            else:
                raise TypeError("outputs must be AppOutputSpec or dict")
            if spec_item.ref_uri is None:
                spec_item.ref_uri = make_stream_ref_uri(spec_item.point_uri)
            output_specs.append(spec_item)

        code = source_code or getattr(app, "source_code", None)
        entry = entry_file or getattr(app, "entry_file", None)

        if code is None:
            try:
                src_path = inspect.getsourcefile(app.__class__)
                if src_path:
                    code = Path(src_path).read_text()
                    if entry is None:
                        try:
                            rel = Path(src_path).resolve().relative_to(Path.cwd().resolve())
                            entry = rel.as_posix()
                        except Exception:
                            entry = Path(src_path).name
                        if entry:
                            entry = entry.replace("\\", "/")
            except Exception:
                code = None
        docker_image = docker_image or getattr(app, "docker_image", None)
        if docker_image is None:
            docker_image = "acquirium-acquirium:latest"
        spec = AppSpec(
            name=app.name,
            version=getattr(app, "version", "0.0"),
            app_type=app_type or getattr(app, "app_type", "soft_sensor"),
            docker_image=docker_image,
            module=app.__module__,
            app_class=app.__class__.__name__,
            entrypoint=entrypoint or getattr(app, "entrypoint", None),
            command=command or getattr(app, "command", None),
            source_code=code,
            entry_file=entry,
            queries=query_specs,
            outputs=output_specs,
            depends_on=deps,
        )
        return self.client.register_app(spec)

    def run_app(
        self,
        app_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        params: dict[str, Any] | None = None,
        keep_alive: bool = False,
        interval: float = 10.0,
    ) -> dict[str, Any]:
        """Trigger an app execution in its own container via the server."""
        return self.client.run_app(
            app_id,
            start=start,
            end=end,
            params=params or {},
            keep_alive=keep_alive,
            interval=interval,
        )

    def stop_app(self, *, run_id: str | None = None, app_id: str | None = None) -> dict[str, Any]:
        """Stop a keep-alive app loop by run_id or all loops for an app_id."""
        return self.client.stop_app(run_id=run_id, app_id=app_id)

    def list_app_runs(self, *, app_id: str | None = None) -> dict[str, Any]:
        """List active keep-alive app runs."""
        return self.client.list_app_runs(app_id=app_id)

    def generate_grafana_dashboard(self, grafana_server, api_key):
        return self.client.generate_grafana_dashboard(grafana_server, api_key)

    # ------------------------------------------------------------------
    # LOGGING API (plant-level convenience methods)
    # ------------------------------------------------------------------

    def insert_log(
        self,
        message: str,
        log_time: str | datetime | None = None,
        observation_start: str | datetime | None = None,
        observation_end: str | datetime | None = None,
    ) -> dict:
        """Insert a plant-level log entry (no specific point URI needed).

        Args:
            message: The log message.
            log_time: Timestamp of the log entry (ISO 8601 string or datetime).
                Defaults to now.
            observation_start: Optional observation period start.
            observation_end: Optional observation period end.
        """
        def _to_iso(v: str | datetime | None) -> str | None:
            if v is None:
                return None
            return v.isoformat() if isinstance(v, datetime) else v

        return self.client.insert_log(
            log_time=_to_iso(log_time),
            log_message=message,
            observation_start=_to_iso(observation_start),
            observation_end=_to_iso(observation_end),
        )

    def read_logs(
        self,
        log_time_start: str | datetime | None = None,
        log_time_end: str | datetime | None = None,
        observation_start: str | datetime | None = None,
        observation_end: str | datetime | None = None,
    ) -> list:
        """Read plant-level log entries (no query/object needed).

        Returns a list of LogEntry objects for the generic plant URI.
        """
        import polars as pl

        def _to_iso(v: str | datetime | None) -> str | None:
            if v is None:
                return None
            return v.isoformat() if isinstance(v, datetime) else v

        logs = self.client.query_logs(
            log_time_start=_to_iso(log_time_start),
            log_time_end=_to_iso(log_time_end),
            observation_start=_to_iso(observation_start),
            observation_end=_to_iso(observation_end),
        )
        if not logs:
            return pl.DataFrame({"point_uri": [], "message": [], "log_time": [], "observation_start": [], "observation_end": []})
        frames = [log.to_dict() for log in logs]
        schema = {
            "point_uri": pl.Utf8,
            "message": pl.Utf8,
            "log_time": pl.Datetime(time_zone="UTC"),
            "observation_start": pl.Datetime(time_zone="UTC"),
            "observation_end": pl.Datetime(time_zone="UTC"),
        }
        return pl.concat([pl.DataFrame(f, schema=schema) for f in frames], how="vertical").sort("log_time")

    def delete_logs(self) -> dict:
        """Delete all plant-level log entries."""
        return self.client.delete_logs()

    # ------------------------------------------------------------------
    # SPARQL / GRAPH UTILITIES
    # ------------------------------------------------------------------
