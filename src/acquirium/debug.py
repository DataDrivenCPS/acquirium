"""Interactive debugging helpers for app and driver authors."""
from __future__ import annotations

import code
import logging
import sys
from typing import Any

logger = logging.getLogger("acquirium.debug")


def _namespace(frame: Any) -> dict[str, Any]:
    """Merge a frame's globals and locals, locals winning, as a plain dict."""
    namespace = dict(frame.f_globals)
    namespace.update(frame.f_locals)
    return namespace


def console(banner: str | None = None, *, depth: int = 1) -> None:
    """Open an interactive console holding the caller's variables.

    Call it anywhere you want to look around — inside ``transform``, that is
    ``inputs``, ``output``, ``context`` and ``self``, already loaded::

        def transform(self, inputs, output, context):
            frame = inputs["temperature"].df()
            aq.console()          # poke at `frame` here
            output["out"] = ...

    Ctrl-D (or ``exit()``) closes the console and the app continues. The
    console sees a snapshot: rebinding a name in it does not change the
    variable in the running function, though mutating an object does.

    The app has to be running in a terminal for this to mean anything. A
    check runs on the server by default, where there is no console to open;
    run it with ``--local`` to bring the app into your own process. When
    there is no interactive input — a server, a test, a pipeline — this logs
    that it was skipped and returns rather than hanging.

    ``depth`` selects whose frame to show, for helpers that wrap this one.
    """
    try:
        # depth 1 is this function's caller, which is the common case.
        frame = sys._getframe(depth)
    except ValueError as error:
        raise ValueError(f"no caller frame {depth} level(s) up") from error

    where = f"{frame.f_code.co_name} at {frame.f_code.co_filename}:{frame.f_lineno}"
    if not (sys.stdin is not None and sys.stdin.isatty()):
        # Left in a deployed app, or reached during a server-side check: say
        # so plainly instead of blocking on input nobody can supply.
        logger.warning(
            "acquirium.console() skipped in %s: no interactive terminal attached. "
            "Run the app in your own process (acquirium app check --local) to get a console.",
            where,
        )
        return

    try:  # optional: arrow keys and history where the platform has it
        import readline  # noqa: F401
    except ImportError:
        pass

    names = ", ".join(sorted(frame.f_locals)) or "nothing local"
    default_banner = (
        f"acquirium console — {where}\n"
        f"in scope: {names}\n"
        f"Ctrl-D (or exit()) resumes."
    )
    code.InteractiveConsole(_namespace(frame)).interact(
        banner=default_banner if banner is None else banner,
        exitmsg="resuming.",
    )
