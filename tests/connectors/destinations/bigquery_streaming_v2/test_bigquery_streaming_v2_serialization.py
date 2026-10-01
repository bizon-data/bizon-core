"""Per-row serialization and batching of the Storage Write API path (CI-safe, no live BigQuery)."""

import pytest
from google.cloud.bigquery import SchemaField
from google.protobuf.json_format import ParseDict, ParseError

from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import (
    BigQueryStreamingV2Destination,
)
from bizon.connectors.destinations.bigquery_streaming_v2.src.proto_utils import (
    get_proto_schema_and_class,
)

SCHEMA = [
    SchemaField(name="name", field_type="STRING", mode="REQUIRED"),
    SchemaField(name="age", field_type="INTEGER", mode="NULLABLE"),
    SchemaField(name="active", field_type="BOOLEAN", mode="NULLABLE"),
    SchemaField(name="score", field_type="FLOAT", mode="NULLABLE"),
    SchemaField(name="payload", field_type="STRING", mode="NULLABLE"),
]


@pytest.fixture
def table_row_class():
    _, cls = get_proto_schema_and_class(SCHEMA)
    return cls


def reference(table_row_class, row: dict) -> bytes:
    """What ParseDict produces for the same row -- the contract the fast path must keep."""
    return ParseDict(row, table_row_class()).SerializeToString()


@pytest.mark.parametrize(
    "row",
    [
        {"name": "John", "age": 30, "active": True, "score": 1.5, "payload": "x"},
        {"name": "John", "age": None, "active": None, "score": None, "payload": None},
        {"name": "Zoë ✓", "age": -1},
        {"name": "John"},
    ],
    ids=["all-scalars", "nones-are-unset", "unicode-and-negative", "missing-optionals"],
)
def test_serialization_matches_parsedict_bytes(table_row_class, row):
    assert BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, dict(row)) == reference(
        table_row_class, {k: v for k, v in row.items() if v is not None}
    )


def test_serialization_json_encodes_nested_values(table_row_class):
    row = {"name": "John", "payload": {"a": [1, 2], "b": {"c": None}}}
    expected = reference(table_row_class, {"name": "John", "payload": '{"a":[1,2],"b":{"c":null}}'})
    assert BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, row) == expected


def test_serialization_coerces_numeric_strings_like_parsedict(table_row_class):
    row = {"name": "John", "age": "30", "score": "2.5"}
    assert BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, dict(row)) == reference(
        table_row_class, row
    )


def test_serialization_rejects_unknown_fields_with_parse_error(table_row_class):
    with pytest.raises(ParseError):
        BigQueryStreamingV2Destination.to_protobuf_serialization(table_row_class, {"name": "John", "nope": 1})


MB = 1024 * 1024


def test_batch_sizes_rows_by_serialized_length(build_bq_destination):
    # Nine 1 MB rows fit under the 9.5 MB request limit. `str(bytes)` would inflate every
    # 0x00 byte to four characters and split them across four requests.
    rows = [b"\x00" * MB] * 9
    with build_bq_destination("streaming_v2") as destination:
        batches = list(destination.batch(rows))
    assert [len(b["stream_batch"]) for b in batches] == [9]
    assert all(b["json_batch"] == [] for b in batches)


def test_batch_classifies_large_rows_by_serialized_length(build_bq_destination):
    small = b"\x00" * (3 * MB)
    large = b"\x00" * (9 * MB)
    with build_bq_destination("streaming_v2") as destination:
        batches = list(destination.batch([small, large]))
    assert len(batches) == 1
    assert batches[0]["stream_batch"] == [small]
    assert batches[0]["json_batch"] == [large]


def test_batch_respects_max_rows_per_request(build_bq_destination):
    with build_bq_destination("streaming_v2", bq_max_rows_per_request=2) as destination:
        batches = list(destination.batch([b"a", b"b", b"c"]))
    assert [len(b["stream_batch"]) for b in batches] == [2, 1]
