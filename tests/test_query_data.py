import pytest
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import *
from acquirium.Client.query import Query
import shutil
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
        lexicon_path="ontologies/lexicon.json",
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


def acquirium_client_stream():
    """Fixture to create an Acquirium client for testing."""
    acq = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )

    acq.insert_graph("tests/test_model_stream.ttl")
    return acq

##### Find all data tests #####

def test_find_all_data_1(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()

    assert len(query.query_graph.data_nodes) == 1
    assert len(query.query_graph.nodes) == 1

    meta = query.metadata()
    assert len(meta) == 10

    df_long = query.dataframe(shape="narrow")
    assert df_long.shape == (10*365*24, 3)

    df_long_6months = query.dataframe(shape="narrow", start="2023-01-01", end="2023-06-30 23:00:00")
    assert df_long_6months.shape == (10*24*181, 3)

    df_wide = query.dataframe(shape="wide")
    assert df_wide.shape == (365*24, 11)

    df_wide_3months = query.dataframe(shape="wide", start="2023-01-01", end="2023-03-31 23:00:00")
    assert df_wide_3months.shape == (24*90, 11)

    df_wide_desc = query.dataframe(shape="wide", order="desc", limit=1)
    assert df_wide_desc.shape == (1, 11)
    assert df_wide_desc["time"][0] == datetime(2023, 12, 31, 23, 0, 0 , tzinfo=ZoneInfo("UTC"))


def test_find_all_data_2(acquirium_client_csv):
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    entity_BE = entity_B.find_entity(_class=ACQUIRIUM_NS.E, alias="e")

    assert entity_BE is not None
    all_data_query = entity_BE.find_all_data()

    assert len(all_data_query.query_graph.data_nodes) == 2
    assert len(all_data_query.query_graph.nodes) == 4

    meta = all_data_query.metadata()

    assert len(meta) == 8
    assert len(meta.columns) == 4

    df_long = all_data_query.dataframe(shape="narrow")
    assert df_long.shape == (6*365*24, 3)
    df_wide = all_data_query.dataframe(shape="wide")
    assert df_wide.shape == (365*24, 7)

#### Test Filters
def test_data_filters(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()

    filt_medium_1 = query.filter_by_medium(ACQUIRIUM_NS.Medium4)
    meta_1 = filt_medium_1.metadata()
    assert len(meta_1) == 2
    assert "point_9" in meta_1["0"].to_list()

    filt_medium_2 = query.filter_by_medium(ACQUIRIUM_NS.Medium1)
    meta_2 = filt_medium_2.metadata()
    assert len(meta_2) == 2
    assert "point_2" in meta_2["0"].to_list()

    filt_substance_1 = query.filter_by_substance(ACQUIRIUM_NS.Substance2)
    meta_3 = filt_substance_1.metadata()
    assert len(meta_3) == 3
    assert "point_3" in meta_3["0"].to_list()

    filt_unit_1 = query.filter_by_unit(ACQUIRIUM_NS.Unit0)
    meta_4 = filt_unit_1.metadata()
    assert len(meta_4) == 3
    assert "point_3" in meta_4["0"].to_list()

    filt_quantity_kind_1 = query.filter_by_quantity_kind(ACQUIRIUM_NS.QuantityKind2)
    meta_5 = filt_quantity_kind_1.metadata()
    assert len(meta_5) == 3
    assert "point_8" in meta_5["0"].to_list()

    filt_enumeration_kind_1 = query.filter_by_enumeration_kind(ACQUIRIUM_NS.EnumerationKind1)
    meta_6 = filt_enumeration_kind_1.metadata()
    assert len(meta_6) == 2
    assert "point_10" in meta_6["0"].to_list()

    filt_random = query.filter_data_nodes(predicate=ACQUIRIUM_NS.hasExternalReference, value="urn:ex/point_10_csv_ref")
    meta_7 = filt_random.metadata()
    assert len(meta_7) == 1
    assert "point_10" in meta_7["0"].to_list()


##### Find data tests #####

def test_find_data_1(acquirium_client_csv):
    acq = acquirium_client_csv
    entity_B = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    entity_BE = entity_B.find_entity(_class=ACQUIRIUM_NS.E, alias="e")

    entity_BE_data1 = entity_BE.find_data()

    assert len(entity_BE_data1.query_graph.data_nodes) == 1
    assert len(entity_BE_data1.query_graph.nodes) == 3

    meta = entity_BE_data1.metadata()
    assert len(meta) == 4
    assert "e_data" in meta.columns

    entity_BE_data2 = entity_BE.find_data(_from ="b", alias = "data_of_b")

    assert len(entity_BE_data2.query_graph.data_nodes) == 1
    assert len(entity_BE_data2.query_graph.nodes) == 3

    meta = entity_BE_data2.metadata()
    assert len(meta) == 8
    assert "data_of_b" in meta.columns


