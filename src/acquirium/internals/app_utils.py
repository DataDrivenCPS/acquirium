from __future__ import annotations

from urllib.parse import quote

from acquirium.internals.internals_namespaces import ACQUIRIUM_NS


def _safe_fragment(value: str) -> str:
    return quote(value, safe="")


def app_uri_for(name: str) -> str:
    return str(ACQUIRIUM_NS[f"app/{_safe_fragment(name)}"])
