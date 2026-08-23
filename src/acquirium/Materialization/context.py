"""Immutable data supplied to materialization compute code."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping
import polars as pl
import pyarrow as pa
from acquirium.Materialization.impact import TimeRange
from acquirium.Materialization.outputs import OutputSet


@dataclass(frozen=True)
class InputStream:
    alias: str
    ref_uri: str
    values: pl.DataFrame
    point_uri: str | None = None
    unit: str | None = None
    key: str = ""


@dataclass(frozen=True)
class InputSet:
    """Normalized query inputs for one whole-query or per-row invocation."""

    streams: Mapping[str, tuple[InputStream, ...]]
    key: str = ""

    def __getitem__(self, alias: str) -> InputStream | tuple[InputStream, ...]:
        values = self.streams[alias]
        return values[0] if len(values) == 1 else values

    @property
    def values(self) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for alias, streams in self.streams.items():
            for stream in streams:
                if stream.values.is_empty():
                    continue
                frames.append(stream.values.with_columns(
                    pl.lit(alias).alias("alias"),
                    pl.lit(stream.ref_uri).alias("ref_uri"),
                ))
        if not frames:
            return pl.DataFrame({"time": [], "value": [], "alias": [], "ref_uri": []})
        return pl.concat(frames, how="diagonal")

    @property
    def ref_uri(self) -> str:
        streams = [stream for values in self.streams.values() for stream in values]
        if len(streams) != 1:
            raise AttributeError("ref_uri is only available for a single-stream query row")
        return streams[0].ref_uri

    @property
    def point_uri(self) -> str | None:
        streams = [stream for values in self.streams.values() for stream in values]
        if len(streams) != 1:
            raise AttributeError("point_uri is only available for a single-stream query row")
        return streams[0].point_uri

@dataclass(frozen=True)
class TransformContext:
    binding_id: str
    execution_id: str
    write_interval: TimeRange
    read_interval: TimeRange
    input_versions: Mapping[str, int]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    state_revision: str | None = None
    outputs: OutputSet | None = None

@dataclass(frozen=True)
class ComputeRequest:
    inputs: pa.Table
    context: TransformContext
    output_refs: frozenset[str]
    invocation: str = "whole_query"
    input_set: InputSet | None = None
    output_specs: Mapping[str, Any] = field(default_factory=dict)
    artifact_bytes: bytes | None = None
