import pytest
from acquirium import Acquirium
from acquirium.Client.data_object import DataObject
from acquirium.internals.internals_namespaces import *
import polars as pl
import time
from datetime import datetime
from zoneinfo import ZoneInfo


@pytest.fixture
def acquirium_client_csv():
    """Fixture to create an Acquirium client for testing."""
    acq = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )

    acq.insert_graph("tests/test_model_csv.ttl")
    time.sleep(1)
    status = acq.client.ingest_status()
    done = status['done']
    total = status['total']
    error = status['error']

    while done < total - error:
        time.sleep(2)
        status = acq.client.ingest_status()
        print(status)
        done = status['done']

    return acq


# ---- Alias-based access ----

def test_getitem_returns_correct_columns(acquirium_client_csv):
    """data['alias'] should return a DataFrame with [time, value] columns."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    assert not data.is_empty()
    assert len(data.aliases) >= 1

    alias = data.aliases[0]
    df = data[alias]
    assert "time" in df.columns
    assert "value" in df.columns
    assert not df.is_empty()


def test_getitem_nonexistent_alias_returns_empty(acquirium_client_csv):
    """Accessing a nonexistent alias should return an empty DataFrame."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data["nonexistent_alias_xyz"]
    assert df.is_empty()
    assert "time" in df.columns
    assert "value" in df.columns


# ---- Grouping via by() ----

def test_by_grouping(acquirium_client_csv):
    """by() should yield entity-scoped sub-DataObjects."""
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    entity_BE = entity_B.find_related(_class=ACQUIRIUM_NS.E, alias="e")
    query = entity_BE.find_all_data()

    data = query.data()

    if "b" in data.entity_aliases:
        count = 0
        for entity_uri, group in data.by("b"):
            assert isinstance(entity_uri, str)
            assert isinstance(group, DataObject)
            assert not group.is_empty()
            count += 1
        assert count > 0


def test_by_invalid_entity_raises(acquirium_client_csv):
    """by() with a nonexistent entity alias should raise KeyError."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    with pytest.raises(KeyError):
        list(data.by("nonexistent_entity"))


# ---- dataframe() ----

def test_dataframe_wide(acquirium_client_csv):
    """dataframe() should return a wide DataFrame with alias-named columns."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data.dataframe(shape="wide")
    assert "time" in df.columns
    assert not df.is_empty()

    # Column names should include data aliases (not raw URIs)
    non_time_cols = [c for c in df.columns if c != "time"]
    assert len(non_time_cols) > 0


def test_dataframe_narrow(acquirium_client_csv):
    """dataframe(shape='narrow') should return the enriched tall frame."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    df = data.dataframe(shape="narrow")
    assert "data_alias" in df.columns
    assert "point_uri" in df.columns
    assert "ref_uri" in df.columns
    assert "time" in df.columns
    assert "value_numeric" in df.columns
    assert "value_text" in df.columns


# ---- iter() ----

def test_iter_yields_per_point(acquirium_client_csv):
    """iter() should yield (point_uri, DataFrame) pairs for each unique point."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    count = 0
    for point_uri, df in data.iter(alias):
        assert isinstance(point_uri, str)
        assert "time" in df.columns
        assert "value" in df.columns
        assert not df.is_empty()
        count += 1
    assert count >= 1


# ---- metadata() ----

def test_metadata(acquirium_client_csv):
    """metadata() should return unique rows with alias and URI info."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    meta = data.metadata()
    assert "data_alias" in meta.columns
    assert "point_uri" in meta.columns
    assert "ref_uri" in meta.columns
    assert not meta.is_empty()


# ---- ref_info ----

def test_ref_info(acquirium_client_csv):
    """ref_info() should return indexed ref URIs."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    refs = data.ref_info(alias)
    assert len(refs) >= 1
    assert refs[0][0] == 0  # first index is 0
    assert isinstance(refs[0][1], str)


# ---- latest() ----

def test_latest(acquirium_client_csv):
    """latest() should return the most recent row for an alias."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()

    alias = data.aliases[0]
    latest = data.latest(alias)
    assert not latest.is_empty()
    assert latest.shape[0] == 1
    assert "time" in latest.columns
    assert "value" in latest.columns


# ---- Edge cases ----

def test_empty_data_object(acquirium_client_csv):
    """A query with no data nodes should produce an empty DataObject."""
    acq = acquirium_client_csv
    query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    # No data nodes — just an entity query
    data = query.data()

    assert data.is_empty()
    assert data.aliases == []
    assert data.entity_aliases == []
    df = data.dataframe()
    assert df.is_empty()


def test_single_data_node_no_entity(acquirium_client_csv):
    """find_all_data with no prior entities should still work."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data(limit=5, order="desc")

    assert not data.is_empty()
    # With find_all_data (no entity), entity_aliases should be empty
    # (there are no non-data nodes to group by)


def test_data_with_entity_context(acquirium_client_csv):
    """Query with entity + data should produce entity columns."""
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    data_query = entity_B.find_data(alias="b_data")
    data = data_query.data(limit=5)

    assert not data.is_empty()
    assert "b_data" in data.aliases

    # Should have entity context
    if data.entity_aliases:
        assert "b" in data.entity_aliases


def test_query_data_method_exists(acquirium_client_csv):
    """Query.data() method should exist and return a DataObject."""
    acq = acquirium_client_csv
    query = acq.find_all_data()
    data = query.data()
    assert isinstance(data, DataObject)
