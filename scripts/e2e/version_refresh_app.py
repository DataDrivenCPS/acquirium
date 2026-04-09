"""Tiny App used by the graph_version e2e test.

Each call to ``build_query`` records a build counter in the app instance
state. The query itself is intentionally trivial — we only care that the
worker observes the counter advancing after a graph mutation.
"""

from __future__ import annotations

import logging

from acquirium import App, AppContext, Output

logger = logging.getLogger("acquirium.e2e.version_refresh")


class VersionRefreshApp(App):
    name = "version_refresh_e2e"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(self) -> None:
        self._build_count = 0

    def build_query(self, aq):
        self._build_count += 1
        logger.warning("E2E_BUILD_QUERY count=%d", self._build_count)
        # Trivial query — its actual contents are irrelevant for this test.
        return aq.query()

    def run(self, ctx: AppContext):
        logger.warning(
            "E2E_RUN app_id=%s build_count_seen=%d",
            ctx.app_id,
            self._build_count,
        )
        return []
