"""Unit tests for ContinuousGuard and Output.delete/validate_outputs
(Apps/execution.py, Apps/base.py)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from acquirium.Apps.base import Output
from acquirium.Apps.execution import (
    AppContractError,
    ContinuousGuard,
    DryRunMutationError,
    UnversionedReadError,
    validate_outputs,
)
from acquirium.internals.models import AppOutputSpec


def test_output_delete_requires_point_uri_via_validate_outputs():
    with pytest.raises(AppContractError, match="requires point_uri"):
        validate_outputs([Output(kind="delete", payload={"timestamps": [datetime.now(timezone.utc)]})])


def test_output_delete_requires_nonempty_timestamps():
    with pytest.raises(AppContractError, match="timestamps"):
        validate_outputs([Output.delete(point_uri="urn:p", timestamps=[])])


def test_output_delete_valid():
    ts = datetime.now(timezone.utc)
    out = validate_outputs([Output.delete(point_uri="urn:p", ref_name="r", timestamps=[ts])])
    assert out[0].kind == "delete"
    assert out[0].payload["timestamps"] == [ts]


def test_output_delete_matches_declared_timeseries_output():
    ts = datetime.now(timezone.utc)
    spec = AppOutputSpec(kind="timeseries", point_uri="urn:p", ref_name="r")
    out = validate_outputs([Output.delete(point_uri="urn:p", ref_name="r", timestamps=[ts])], [spec])
    assert out[0].kind == "delete"


def test_output_delete_rejected_against_trigger_destination():
    ts = datetime.now(timezone.utc)
    spec = AppOutputSpec(kind="trigger", point_uri="urn:p")
    with pytest.raises(AppContractError, match="cannot delete on trigger"):
        validate_outputs([Output.delete(point_uri="urn:p", timestamps=[ts])], [spec])


def test_output_delete_rejected_for_undeclared_point():
    ts = datetime.now(timezone.utc)
    spec = AppOutputSpec(kind="timeseries", point_uri="urn:other", ref_name="r")
    with pytest.raises(AppContractError, match="undeclared point_uri"):
        validate_outputs([Output.delete(point_uri="urn:p", timestamps=[ts])], [spec])


class TestContinuousGuard:
    def _guard(self):
        target = MagicMock()
        target.timeseries.return_value = "live-data"
        target.graph_version.return_value = 7
        target.client = MagicMock()
        target.client.timeseries.return_value = "live-data-via-client"
        return ContinuousGuard(target), target

    def test_blocks_timeseries_read(self):
        guard, _ = self._guard()
        with pytest.raises(UnversionedReadError):
            guard.timeseries("urn:x")

    def test_blocks_timeseries_info(self):
        guard, _ = self._guard()
        with pytest.raises(UnversionedReadError):
            guard.timeseries_info("urn:x")

    def test_blocks_sql_query(self):
        guard, _ = self._guard()
        with pytest.raises(UnversionedReadError):
            guard.sql_query("SELECT 1")

    def test_blocks_mutations_like_read_only_acquirium(self):
        guard, _ = self._guard()
        with pytest.raises(DryRunMutationError):
            guard.insert_timeseries()

    def test_allows_graph_reads(self):
        guard, target = self._guard()
        assert guard.graph_version() == 7
        target.graph_version.assert_called_once()

    def test_nested_client_is_also_guarded(self):
        guard, _ = self._guard()
        with pytest.raises(UnversionedReadError):
            guard.client.timeseries("urn:x")
