from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from rdflib import URIRef

from acquirium.internals.models import compute_ref_uri

if TYPE_CHECKING:
    from acquirium.Client.acquirium import Acquirium


class Driver(ABC):
    """Base class for data-collection / processing drivers.

    The ``acquirium run`` CLI calls :meth:`setup` once at startup, then calls
    :meth:`loop` repeatedly, sleeping for ``interval`` seconds between calls.
    Override :meth:`stop` for cleanup on Ctrl-C or SIGTERM.

    Example::

        from acquirium import Driver

        class MyDriver(Driver):
            def setup(self):
                self.aq.register_datasource("my-source")

            def loop(self):
                ts = datetime.now(timezone.utc)
                self.aq.insert_timeseries_batch("my-source", {"temp": [(ts, 21.5)]})

    Invoke with::

        acquirium run my_module:MyDriver --config acquirium.toml
    """

    def __init__(self, aq: "Acquirium", config: dict) -> None:
        self.aq = aq
        # Full parsed TOML dict so drivers can read their own config sections.
        self.config = config

    def source_id(self) -> str:
        """Return this driver's configured datasource id.

        By convention built-in drivers store this on one of ``_source_id``,
        ``source_id``, or ``src_id`` during ``setup()``. Override this method
        if your driver keeps the datasource id somewhere else.
        """
        for attr in ("_source_id", "source_id", "src_id"):
            value = getattr(self, attr, None)
            if callable(value):
                continue
            if value:
                return str(value)
        raise AttributeError(
            "Driver source id is not set. Assign self._source_id (or self.source_id / self.src_id) "
            "during setup(), or override source_id()."
        )

    def reference_uri(self, ref_name: str) -> URIRef:
        """Return the canonical Acquirium reference URI for ``ref_name``."""
        return compute_ref_uri(self.source_id(), ref_name)

    def config_dir(self) -> Path:
        """Return the directory containing the loaded config file, if known."""
        return Path(self.config.get("__config_dir", Path.cwd()))

    @abstractmethod
    def setup(self) -> None:
        """One-time initialisation: register_datasource, insert RDF, etc.

        Called once before the loop starts.
        """

    @abstractmethod
    def loop(self) -> None:
        """Single collection iteration.

        Collect data and call ``self.aq.insert_timeseries_batch()``.
        The CLI sleeps for ``interval`` seconds between successive calls.
        Do *not* put a ``time.sleep`` or ``while True`` here.
        """

    def on_graph_change(self) -> None:
        """Called by the CLI when the server's graph version advances.

        Override to react to graph mutations (e.g. re-query for new streams).
        Default is a no-op. Never called during setup() — use setup() for
        initial graph queries. Only fired on subsequent changes.
        """

    def stop(self) -> None:
        """Optional cleanup called on shutdown (Ctrl-C or SIGTERM).

        Default is a no-op.  Override to close file ref URIs, flush buffers, etc.
        """
