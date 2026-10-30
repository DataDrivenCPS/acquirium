"""Pydantic models for the streaming API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal, Any
from pydantic import BaseModel, ConfigDict, Field
from acquirium.internals.internals_namespaces import ACQUIRIUM_NS
from rdflib import URIRef
# Fixed UUID namespace for deterministic reference URI generation.
# All (source_id, ref_name) pairs are hashed within this namespace so ref URIs
# are globally unique and reproducible without any state.
_REF_URI_NAMESPACE = uuid.UUID("6a8f3c2e-4b1d-5e7f-9012-3a4b5c6d7e8f")


def looks_like_uri(value: object) -> bool:
    """True if *value* is a string already in URI form.

    The single canonical check (strict prefix form). The looser
    ``"://" in value`` variant scattered elsewhere also matches arbitrary
    text containing ``://`` and should converge here.
    """
    return isinstance(value, str) and value.startswith(
        ("http://", "https://", "urn:")
    )


def compute_ref_uri(source_id: str, ref_name: str) -> URIRef:
    """Return a deterministic UUID5 ref URI for a (source_id, ref_name) pair.

    The ref URI is the canonical stream identity exposed by the graph and
    client APIs. A point links to this reference node through
    ``ref:hasExternalReference``. Escapement uses this reference as its stream
    key. It is stable across restarts and
    can be recomputed from the same inputs.
    """
    ref_uri_str = str(uuid.uuid5(_REF_URI_NAMESPACE, f"{source_id}:{ref_name}"))
    return ACQUIRIUM_NS[ref_uri_str]


class TimeseriesInfo(BaseModel):
    row_count: int
    earliest: datetime | None = None
    latest: datetime | None = None



class Point(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    uri: str
    handle: str | None = None
    ref_uri: str | URIRef | list[str] | list[URIRef] | None = None
    types: list[str] = Field(default_factory=list)
    unit: str | None = None
    last_reported: datetime | None = None
    stream: TimeseriesInfo | None = None


class PointCreateRequest(BaseModel):
    uri: str
    types: list[str] = Field(default_factory=list)
    unit: str | None = None


class RegisterDatasourceRequest(BaseModel):
    """Request to register a named datasource."""
    source_id: str


class StreamInsert(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str = Field(min_length=1)
    ref_name: str = Field(min_length=1)
    values: list[tuple[Any, Any]]
    request_id: str = Field(min_length=1)


Order = Literal["asc", "desc"]
