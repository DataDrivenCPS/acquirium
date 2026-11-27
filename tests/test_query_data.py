import pytest
import polars as pl
from acquirium import Acquirium
from acquirium.Client.data_object import DataObject
from acquirium.internals.internals_namespaces import *
from datetime import datetime
from zoneinfo import ZoneInfo
from conftest import _CSV_SOURCE_ID

##### Find all data tests #####

def test_find_all_data_reads_and_shapes_registered_streams(acquirium_client_csv):
    query = acquirium_client_csv.find_all_data()

    assert len(query.query_graph.data_nodes) == 1
    assert len(query.query_graph.nodes) == 1

    # Discover the ten streams, then read their year of hourly data in both shapes.
    data = query.data()
    assert isinstance(data, DataObject)
    metadata = data.metadata()
    assert metadata.height == 10
    assert {"data_alias", "property_uri"} <= set(metadata.columns)
    assert "stream_uri" not in metadata.columns
    assert {"stream_uri", "source_id", "stream_name"} <= set(
        data.metadata(include_stream_uris=True).columns
    )
    narrow = data.dataframe(shape="narrow")
    assert narrow.shape == (10 * 365 * 24, 5)
    assert "property_id" in narrow.columns
    assert "property_uri" not in narrow.columns
    full = data.dataframe(shape="narrow", compact=False)
    assert {"data_alias", "property_uri", "stream_uri", "time", "value_numeric", "value_text"} <= set(full.columns)
    wide = data.dataframe(shape="wide")
    assert wide.shape == (365 * 24, 11)
    assert wide.columns[0] == "time"

    # Query.dataframe applies date bounds, ordering, and limits before returning a frame.
    assert query.dataframe(shape="narrow", start="2023-01-01", end="2023-06-30 23:00:00").shape == (10 * 24 * 181, 5)
    assert query.dataframe(shape="wide", start="2023-01-01", end="2023-03-31 23:00:00").shape == (24 * 90, 11)
    newest = query.dataframe(shape="wide", order="desc", limit=1)
    assert newest.shape == (1, 11)
    assert newest["time"][0] == datetime(2023, 12, 31, 23, tzinfo=ZoneInfo("UTC"))

    # Alias access and latest() expose the same readings.
    alias, = data.aliases
    assert {"time", "value"} <= set(data[alias].columns)
    assert not data[alias].is_empty()
    assert data.latest(alias).height == 10
    latest = data.latest()
    assert latest.columns == ["data_alias", "property_uri", "time", "value"]
    assert latest.height == 10
    assert latest["time"].unique().to_list() == newest["time"].to_list()
    with pytest.raises(KeyError):
        data["nonexistent_alias"]


def test_select_streams_by_number_and_uri(acquirium_client_csv):
    data = acquirium_client_csv.find_all_data().data()
    info = data.stream_info()
    assert info.columns == ["stream", "alias", "property_uri", "stream_uri", "source_id", "stream_name"]
    assert info["stream"].to_list() == list(range(10))
    assert set(info["source_id"]) == {_CSV_SOURCE_ID}
    assert set(info["stream_name"]) == {f"point_{i}" for i in range(1, 11)}
    assert data.stream_info(data.aliases[0]).equals(info)
    with pytest.raises(KeyError):
        data.stream_info("nonexistent_alias")

    # Use the same identity to narrow the tall, wide, and latest views.
    first, second = info["stream_uri"][:2]
    selected = data.dataframe(shape="narrow", compact=False, streams=[0])
    assert selected["stream_uri"].unique().to_list() == [first]
    assert data.dataframe(shape="narrow", compact=False, streams=first).equals(selected)
    mixed = data.dataframe(shape="narrow", compact=False, streams=[0, second])
    assert set(mixed["stream_uri"]) == {first, second}
    assert data.dataframe(streams=[0]).width == 2
    assert data.latest(streams=[0]).height == 1
    with pytest.raises(KeyError):
        data.dataframe(streams=[info.height])


def test_data_groups_follow_the_entity_query(acquirium_client_csv):
    entity = acquirium_client_csv.query().entity(ACQUIRIUM_NS.B, alias="b")

    # The entity can match without selecting any readings.
    empty = entity.data()
    assert empty.is_empty()
    assert empty.aliases == []
    assert empty.entity_aliases == ["b"]
    assert list(empty.by("b")) == []
    assert empty.dataframe().is_empty()

    # A measurement attached to B retains the B alias for grouping.
    data = entity.measurement(alias="b_data").data(limit=5)
    assert data.aliases == ["b_data"]
    assert data.entity_aliases == ["b"]
    groups = list(data.by("b"))
    assert groups
    for uri, group in groups:
        assert isinstance(uri, str)
        assert isinstance(group, DataObject)
        assert not group.is_empty()
    with pytest.raises(KeyError):
        list(data.by("nonexistent_entity"))


def test_find_all_data_2(acquirium_client_csv):
    acq = acquirium_client_csv
    entity_BE = acq.query().entity(ACQUIRIUM_NS.B, alias="b").entity(ACQUIRIUM_NS.E, alias="e")
    all_data_query = entity_BE.measurement(frm="*")

    assert len(all_data_query.query_graph.data_nodes) == 2
    assert len(all_data_query.query_graph.nodes) == 4

    meta = all_data_query.metadata()

    # two measurement nodes are UNION branches, not a cross product: each row
    # carries either b_data or e_data
    assert len(meta) == 12
    assert len(meta.columns) == 4
    assert meta["b_data"].null_count() + meta["e_data"].null_count() == 12

    meta_full = all_data_query.metadata(include_internals=True)
    # per data node: ref, unit, ref unit, source id, stream name
    assert len(meta_full.columns) == 4 + 2 * 5

    df_long = all_data_query.dataframe(shape="narrow")
    assert df_long.shape == (6*365*24, 5)
    df_wide = all_data_query.dataframe(shape="wide")
    assert df_wide.shape == (365*24, 7)

#### Test Filters
def test_data_filters(acquirium_client_csv):
    acq = acquirium_client_csv
    query = acq.find_all_data()
    point = lambda n: acq.client.compact_uri(f"urn:ex/point_{n}")   # prefix is environment-dependent

    meta_1 = query.where(medium=ACQUIRIUM_NS.Medium4).metadata()
    assert len(meta_1) == 2
    assert point(9) in meta_1["data"].to_list()

    meta_2 = query.where(medium=ACQUIRIUM_NS.Medium1).metadata()
    assert len(meta_2) == 2
    assert point(2) in meta_2["data"].to_list()

    meta_3 = query.where(substance=ACQUIRIUM_NS.Substance2).metadata()
    assert len(meta_3) == 3
    assert point(3) in meta_3["data"].to_list()

    meta_4 = query.where(unit=ACQUIRIUM_NS.Unit0).metadata()
    assert len(meta_4) == 3
    assert point(3) in meta_4["data"].to_list()

    meta_5 = query.where(quantity_kind=ACQUIRIUM_NS.QuantityKind2).metadata()
    assert len(meta_5) == 3
    assert point(8) in meta_5["data"].to_list()

    meta_6 = query.where(enumeration_kind=ACQUIRIUM_NS.EnumerationKind1).metadata()
    assert len(meta_6) == 2
    assert point(10) in meta_6["data"].to_list()

    meta_7 = acq.find_all_data(uri="urn:ex/point_10").metadata()
    assert len(meta_7) == 1
    assert meta_7["data"].to_list() == [point(10)]


##### Namespace transfer + CURIE helper tests #####
# These exercise the rdflib-backed namespace helpers on the client
# (acq.client.compact_uri / expand_uri / namespace_manager). Prefix names are
# environment-dependent (rdflib de-duplicates colliding prefixes), so the tests
# assert round-trip behaviour and namespace URIs rather than fixed prefixes.

def _bound_namespace_uris(acq: Acquirium) -> set[str]:
    """Set of namespace URIs currently bound on the client."""
    return {str(uri) for _, uri in acq.client.namespace_manager().namespaces()}


def test_namespaces_transferred_on_insert(acquirium_client_csv):
    """insert_graph propagates @prefix bindings from the TTL into /namespace/list."""
    acq = acquirium_client_csv
    bound = _bound_namespace_uris(acq)

    # Namespaces declared in tests/test_model_csv.ttl must reach the query store.
    assert "urn:acquirium#" in bound
    assert "urn:ex/" in bound
    assert "http://qudt.org/schema/qudt/" in bound
    assert "http://data.ashrae.org/standard223#" in bound


def test_compact_uri_roundtrip(acquirium_client_csv):
    """compact_uri returns prefix:local for a URI and round-trips through expand_uri."""
    c = acquirium_client_csv.client

    compact = c.compact_uri("urn:ex/point_1")
    assert ":" in compact and compact.endswith(":point_1")
    assert c.expand_uri(compact) == "urn:ex/point_1"

    # urn:acquirium# round-trips too (the exact prefix is environment-dependent).
    med = str(ACQUIRIUM_NS.Medium4)
    compact_med = c.compact_uri(med)
    assert compact_med.endswith(":Medium4")
    assert c.expand_uri(compact_med) == med


def test_compact_uri_rejects_non_uri(acquirium_client_csv):
    """compact_uri raises on strings that are not splittable URIs."""
    c = acquirium_client_csv.client
    with pytest.raises(ValueError):
        c.compact_uri("just text")
    with pytest.raises(ValueError):
        c.compact_uri("point_9")


def test_expand_uri_requires_bound_curie(acquirium_client_csv):
    """expand_uri only accepts a CURIE whose prefix is bound; full URIs/unbound raise."""
    c = acquirium_client_csv.client
    with pytest.raises(ValueError):
        c.expand_uri("urn:ex/point_1")   # a full URI is not a CURIE
    with pytest.raises(ValueError):
        c.expand_uri("nope:thing")       # prefix not bound


def test_metadata_renders_compact_curie(acquirium_client_csv):
    """metadata() renders point ids as CURIEs (prefix:local), not bare local names."""
    acq = acquirium_client_csv
    ids = acq.find_all_data().metadata()["data"].to_list()

    assert all(":" in pid for pid in ids)
    assert any(pid.endswith(":point_1") for pid in ids)


def test_normalize_instance_uri(acquirium_client_csv):
    """uri= accepts full URIs and bound CURIEs, rejects everything else."""
    acq = acquirium_client_csv
    query = acq.find_all_data()

    # Full URI string and URIRef pass through unchanged; None stays None.
    assert query._normalize_instance_uri("urn:ex/point_1") == "urn:ex/point_1"
    assert query._normalize_instance_uri(ACQUIRIUM_NS.Medium4) == str(ACQUIRIUM_NS.Medium4)
    assert query._normalize_instance_uri(None) is None

    # A CURIE with a bound prefix expands back to its full URI.
    point_curie = acq.client.compact_uri("urn:ex/point_1")
    assert query._normalize_instance_uri(point_curie) == "urn:ex/point_1"

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

    # point ids render as CURIEs whose prefix is environment-dependent; derive
    # them the same way dataframe() does so the test is prefix-agnostic.
    p1 = acq.client.compact_uri("urn:ex/point_1")   # numeric
    p9 = acq.client.compact_uri("urn:ex/point_9")   # text

    numeric = df.filter(pl.col("property_id") == p1)
    assert len(numeric) > 0
    # numeric point populated value_numeric (cast succeeded) and not value_text
    assert numeric["value_numeric"].null_count() < len(numeric)
    assert numeric["value_text"].null_count() == len(numeric)

    text = df.filter(pl.col("property_id") == p9)
    assert len(text) > 0
    # un-castable text point gracefully stayed in value_text
    assert text["value_numeric"].null_count() == len(text)
    assert text["value_text"].null_count() < len(text)


def test_dataframe_cast_int_warns_when_truncating(acquirium_client_csv):
    """cast_value='int' truncates the fractional values of points 5-8 and says so."""
    acq = acquirium_client_csv
    with pytest.warns(UserWarning, match="cast_value='int' truncates"):
        df = acq.find_all_data().dataframe(shape="narrow", cast_value="int", limit=5)
    assert df["value_numeric"].dtype == pl.Int64
    assert acq.client.compact_uri("urn:ex/point_9") in set(df["property_id"].to_list())   # text point still present


def test_dataframe_cast_value_is_validated(acquirium_client_csv):
    acq = acquirium_client_csv
    with pytest.raises(ValueError, match="cast_value must be"):
        acq.find_all_data().data(cast_value="double")


##### Find data tests #####

def test_find_data_1(acquirium_client_csv):
    acq = acquirium_client_csv
    entity_BE = acq.query().entity(ACQUIRIUM_NS.B, alias="b").entity(ACQUIRIUM_NS.E, alias="e")

    entity_BE_data1 = entity_BE.measurement()

    assert len(entity_BE_data1.query_graph.data_nodes) == 1
    assert len(entity_BE_data1.query_graph.nodes) == 3

    meta = entity_BE_data1.metadata()
    assert len(meta) == 4
    assert "e_data" in meta.columns

    entity_BE_data2 = entity_BE.measurement(frm="b", alias="data_of_b")

    assert len(entity_BE_data2.query_graph.data_nodes) == 1
    assert len(entity_BE_data2.query_graph.nodes) == 3

    meta = entity_BE_data2.metadata()
    assert len(meta) == 8
    assert "data_of_b" in meta.columns
