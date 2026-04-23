from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

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

    def stop(self) -> None:
        """Optional cleanup called on shutdown (Ctrl-C or SIGTERM).

        Default is a no-op.  Override to close file handles, flush buffers, etc.
        """
