"""Tests for acquirium.Grafana.grafana_dashboard_creator."""

import pytest
from unittest.mock import patch, MagicMock

from acquirium.Grafana.grafana_dashboard_creator import GrafanaDashboardCreator


@pytest.fixture
def creator():
    return GrafanaDashboardCreator(
        title="Test Dashboard",
        tags=["test"],
        timezone="browser",
        refresh_interval="30s",
    )


class TestGrafanaInit:
    def test_defaults(self, creator):
        assert creator.title == "Test Dashboard"
        assert creator.panels == []
        assert "gauge" in creator.panels_meta
        assert "time_series" in creator.panels_meta
        assert creator.panels_meta["gauge"] == []
        assert creator.panels_meta["time_series"] == []

    def test_all_panel_types_present(self, creator):
        expected_keys = {"gauge", "bar_chart", "state_timeline", "time_series"}
        assert set(creator.panels_meta.keys()) == expected_keys


class TestAddPanels:
    def test_add_gauge(self, creator):
        prop = {"uri": "urn:test:p1", "title": "Temperature"}
        creator.add_gauge(prop)
        assert len(creator.panels_meta["gauge"]) == 1
        assert creator.panels_meta["gauge"][0] is prop

    def test_add_time_series(self, creator):
        props = [{"uri": "urn:test:p1"}, {"uri": "urn:test:p2"}]
        creator.add_time_series("Flow Rates", props)
        assert len(creator.panels_meta["time_series"]) == 1
        title, stored_props = creator.panels_meta["time_series"][0]
        assert title == "Flow Rates"
        assert len(stored_props) == 2

    def test_multiple_panels(self, creator):
        creator.add_gauge({"uri": "urn:g1"})
        creator.add_gauge({"uri": "urn:g2"})
        creator.add_time_series("TS1", [{"uri": "urn:t1"}])
        assert len(creator.panels_meta["gauge"]) == 2
        assert len(creator.panels_meta["time_series"]) == 1


class TestGenerateDashboard:
    @patch("acquirium.Grafana.grafana_dashboard_creator.pc")
    def test_creates_dashboard(self, mock_pc, creator):
        mock_pc.create_gauge.return_value = MagicMock()
        mock_pc.create_time_series.return_value = MagicMock()

        creator.add_gauge({"uri": "urn:test:g1"})
        creator.add_time_series("TS", [{"uri": "urn:test:t1"}])
        creator.generate_dashboard()

        assert creator.dashboard is not None
        assert mock_pc.create_gauge.call_count == 1
        assert mock_pc.create_time_series.call_count == 1


class TestUploadDashboard:
    @patch("acquirium.Grafana.grafana_dashboard_creator.upload_dashboard")
    def test_calls_upload(self, mock_upload, creator):
        creator.dashboard = MagicMock()
        creator.upload_dashboard("http://grafana:3000", "api-key-123")
        mock_upload.assert_called_once_with(creator.dashboard, "http://grafana:3000", "api-key-123")
