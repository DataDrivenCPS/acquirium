import pytest
import polars as pl
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import *
from acquirium.internals.models import compute_ref_uri
from acquirium.Client.query import Query
from datetime import datetime
from zoneinfo import ZoneInfo
from conftest import ACQUIRIUM_TEST_SERVER_HOST, ACQUIRIUM_TEST_SERVER_PORT, _CSV_SOURCE_ID

##### Find all data tests #####

def test_find_all_data_1(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()

    assert len(query.query_graph.data_nodes) == 1
    assert len(query.query_graph.nodes) == 1

    meta = query.metadata()
    assert len(meta) == 10

    df_long = query.dataframe(shape="narrow")
    assert df_long.shape == (10*365*24, 5)

    df_long_6months = query.dataframe(shape="narrow", start="2023-01-01", end="2023-06-30 23:00:00")
    assert df_long_6months.shape == (10*24*181, 5)

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

    meta_full = all_data_query.metadata(include_internals=True)
    assert len(meta_full.columns) == 10

    df_long = all_data_query.dataframe(shape="narrow")
    assert df_long.shape == (6*365*24, 5)
    df_wide = all_data_query.dataframe(shape="wide")
    assert df_wide.shape == (365*24, 7)

#### Test Filters
def test_data_filters(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()

    filt_medium_1 = query.filter_by_medium(ACQUIRIUM_NS.Medium4)
    meta_1 = filt_medium_1.metadata()
    assert len(meta_1) == 2
    assert "ex1:point_9" in meta_1["0"].to_list()

    filt_medium_2 = query.filter_by_medium(ACQUIRIUM_NS.Medium1)
    meta_2 = filt_medium_2.metadata()
    assert len(meta_2) == 2
    assert "ex1:point_2" in meta_2["0"].to_list()

    filt_substance_1 = query.filter_by_substance(ACQUIRIUM_NS.Substance2)
    meta_3 = filt_substance_1.metadata()
    assert len(meta_3) == 3
    assert "ex1:point_3" in meta_3["0"].to_list()

    filt_unit_1 = query.filter_by_unit(ACQUIRIUM_NS.Unit0)
    meta_4 = filt_unit_1.metadata()
    assert len(meta_4) == 3
    assert "ex1:point_3" in meta_4["0"].to_list()

    filt_quantity_kind_1 = query.filter_by_quantity_kind(ACQUIRIUM_NS.QuantityKind2)
    meta_5 = filt_quantity_kind_1.metadata()
    assert len(meta_5) == 3
    assert "ex1:point_8" in meta_5["0"].to_list()

    filt_enumeration_kind_1 = query.filter_by_enumeration_kind(ACQUIRIUM_NS.EnumerationKind1)
    meta_6 = filt_enumeration_kind_1.metadata()
    assert len(meta_6) == 2
    assert "ex1:point_10" in meta_6["0"].to_list()

    filt_random = query.filter_data_nodes(predicate=HAS_EXTERNAL_REFERENCE, value=str(compute_ref_uri(_CSV_SOURCE_ID, "point_10")))
    meta_7 = filt_random.metadata()
    assert len(meta_7) == 1
    assert "ex1:point_10" in meta_7["0"].to_list()


##### Namespace transfer + CURIE helper tests #####

def test_namespaces_transferred_on_insert(acquirium_client_csv):
    """insert_graph propagates @prefix bindings into /namespace/list."""
    acq = acquirium_client_csv
    ns = acq.list_namespaces()

    # Prefixes declared in tests/test_model_csv.ttl must reach the query store.
    assert ns.get("acq") == "urn:acquirium#"
    assert "qudt" in ns
    assert "s223" in ns
    # ex: <urn:ex/> is bound (possibly under a de-duplicated prefix, e.g. ex1).
    assert any(uri == "urn:ex/" for uri in ns.values())


def test_compact_uri_roundtrip(acquirium_client_csv):
    """compact_uri returns prefix:local and round-trips through expand_uri."""
    acq = acquirium_client_csv

    compact = acq.compact_uri("urn:ex/point_1")
    assert compact.endswith(":point_1")
    assert ":" in compact and compact != "point_1"  # keeps prefix, unlike strip_namespace
    assert acq.expand_uri(compact) == "urn:ex/point_1"

    # acq:* (urn:acquirium#) also compacts to a CURIE that round-trips back.
    compact_med = acq.compact_uri(str(ACQUIRIUM_NS.Medium4))
    assert compact_med.endswith(":Medium4")
    assert acq.expand_uri(compact_med) == str(ACQUIRIUM_NS.Medium4)


def test_compact_uri_passthrough_and_fallback(acquirium_client_csv):
    """Non-URIs pass through; unbound namespaces fall back to the local name."""
    acq = acquirium_client_csv

    assert acq.compact_uri("just text") == "just text"        # not a URI
    assert acq.compact_uri(42) == "42"                         # non-string cast
    assert acq.compact_uri("urn:unbound/thing") == "thing"    # no bound prefix -> bare local


def test_expand_uri_passthrough(acquirium_client_csv):
    """expand_uri leaves full URIs / colon-free / unbound-prefix inputs unchanged."""
    acq = acquirium_client_csv

    assert acq.expand_uri("urn:ex/point_1") == "urn:ex/point_1"          # already a URI
    assert acq.expand_uri("http://example.com/x") == "http://example.com/x"
    assert acq.expand_uri("point_1") == "point_1"                        # no colon
    assert acq.expand_uri("nope:thing") == "nope:thing"                  # prefix not bound


def test_metadata_renders_compact_curie(acquirium_client_csv):
    """metadata() renders point ids as CURIEs (prefix:local), not bare local names."""
    acq = acquirium_client_csv
    meta = acq.find_all_data().metadata()

    ids = meta["0"].to_list()
    assert all(":" in pid for pid in ids)
    assert "ex1:point_1" in ids


def test_normalize_instance_uri_expands_curie(acquirium_client_csv):
    """find_entity(uri=...) accepts CURIEs via bound-prefix expansion."""
    acq = acquirium_client_csv
    query = acq.find_all_data()

    # CURIE expands using bound prefixes.
    assert query._normalize_instance_uri("acq:Medium4") == str(ACQUIRIUM_NS.Medium4)
    assert query._normalize_instance_uri("ex1:point_1") == "urn:ex/point_1"

    # Already-full URIs and URIRefs pass through unchanged.
    assert query._normalize_instance_uri("urn:ex/point_1") == "urn:ex/point_1"
    assert query._normalize_instance_uri(ACQUIRIUM_NS.Medium4) == str(ACQUIRIUM_NS.Medium4)
    assert query._normalize_instance_uri(None) is None

    # Neither a URI nor a bound CURIE -> error.
    with pytest.raises(ValueError):
        query._normalize_instance_uri("notaprefix:thing")


##### Per-frame value casting tests #####

def test_dataframe_cast_float_is_per_frame(acquirium_client_csv):
    """cast_value='float' casts each point's frame independently.

    Points 1-8 are numeric and points 9-10 are text. The cast now runs per
    point frame before the value column is split, so the numeric points land
    in ``value_numeric`` while the un-castable text points fall back to
    ``value_text`` instead of one bad frame derailing the whole cast.
    """
    acq = acquirium_client_csv
    df = acq.find_all_data().dataframe(shape="narrow", cast_value="float", limit=5)

    numeric = df.filter(pl.col("point_id") == "ex1:point_1")
    assert len(numeric) > 0
    # numeric point populated value_numeric (cast succeeded) and not value_text
    assert numeric["value_numeric"].null_count() < len(numeric)
    assert numeric["value_text"].null_count() == len(numeric)

    text = df.filter(pl.col("point_id") == "ex1:point_9")
    assert len(text) > 0
    # un-castable text point gracefully stayed in value_text
    assert text["value_numeric"].null_count() == len(text)
    assert text["value_text"].null_count() < len(text)


def test_dataframe_cast_int_per_frame_does_not_raise(acquirium_client_csv):
    """cast_value='int' on mixed numeric/text data returns data for both kinds.

    A frame whose values cannot be cast to int is skipped (logged), not fatal.
    """
    acq = acquirium_client_csv
    df = acq.find_all_data().dataframe(shape="narrow", cast_value="int", limit=5)

    point_ids = set(df["point_id"].to_list())
    assert "ex1:point_1" in point_ids   # numeric point present
    assert "ex1:point_9" in point_ids   # text point still present, not dropped


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
