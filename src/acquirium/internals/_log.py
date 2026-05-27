"""Logging helpers shared by the server, drivers, and storage.

`configure_logging` is the single entry point for setting up the acquirium
log handlers and level (called from cli.py and the FastAPI lifespan).

`timed_debug` is a context manager used to bracket heavy operations with
entry/exit DEBUG lines plus elapsed milliseconds. Designed to be cheap when
the logger isn't at DEBUG (the time call still runs, but the formatting is
skipped by stdlib logging).
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Iterator


_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(verbose: bool | None = None) -> None:
    """Install handler + set level for the `acquirium` logger tree.

    If `verbose` is None, falls back to ACQUIRIUM_VERBOSE in the environment.
    Re-callable — uses `force=True` so a previously installed basicConfig
    (e.g., from a module-level call during import) doesn't pin us at INFO.
    """
    if verbose is None:
        verbose = os.environ.get("ACQUIRIUM_VERBOSE") == "1"

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(level=level, format=_LOG_FORMAT, force=True)
    # Explicit set, since basicConfig only configures root and a prior INFO-level
    # call would have already won without force=True; this also lets us tune the
    # acquirium tree independently of uvicorn / fastapi loggers in the future.
    logging.getLogger("acquirium").setLevel(level)


@contextmanager
def timed_debug(logger: logging.Logger, msg: str, *args) -> Iterator[None]:
    """DEBUG-log entry + exit (with elapsed ms) around a block.

    Skip the cost entirely when DEBUG isn't enabled.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        yield
        return
    start = time.perf_counter()
    logger.debug("→ " + msg, *args)
    try:
        yield
    finally:
        logger.debug("← " + msg + " (%.1f ms)", *args, (time.perf_counter() - start) * 1000.0)
