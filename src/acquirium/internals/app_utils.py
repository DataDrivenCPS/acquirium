from __future__ import annotations

import hashlib
from urllib.parse import quote

from acquirium.internals.internals_namespaces import ACQUIRIUM_NS


def _safe_fragment(value: str) -> str:
    return quote(value, safe="")


def app_uri_for(name: str) -> str:
    return str(ACQUIRIUM_NS[f"app/{_safe_fragment(name)}"])


def make_stream_ref_uri(point_uri: str) -> str:
    digest = hashlib.sha1(point_uri.encode("utf-8")).hexdigest()[:12]
    return str(ACQUIRIUM_NS[f"stream/{digest}"])
