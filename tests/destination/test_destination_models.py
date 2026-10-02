from datetime import datetime

import polars as pl
from pytz import UTC

from bizon.destination.models import destination_record_schema, transform_to_df_destination_records
from bizon.source.models import source_record_schema

df_source_records = pl.DataFrame(
    {
        "id": ["record_1", "record_2"],
        "data": ['{"key": "value1"}', '{"key": "value2"}'],
        "timestamp": [datetime(2024, 12, 5, 11, 30, tzinfo=UTC), datetime(2024, 12, 5, 12, 30, tzinfo=UTC)],
        "destination_id": ["test", "test"],
    },
    schema=source_record_schema,
)


def test_destination_record_from_source_record():
    destination_source_records = transform_to_df_destination_records(
        df_source_records=df_source_records,
        extracted_at=datetime(2024, 12, 5, 12, 0),
    )
    assert destination_source_records["source_record_id"].to_list() == ["record_1", "record_2"]
    assert destination_source_records["source_data"].to_list() == ['{"key": "value1"}', '{"key": "value2"}']


def test_destination_records_have_the_destination_schema():
    df = transform_to_df_destination_records(
        df_source_records=df_source_records, extracted_at=datetime(2024, 12, 5, 12, 0, tzinfo=UTC)
    )
    assert df.schema == destination_record_schema
    assert df["bizon_extracted_at"].to_list() == [datetime(2024, 12, 5, 12, 0, tzinfo=UTC)] * 2
    assert df["source_timestamp"].to_list() == df_source_records["timestamp"].to_list()


def test_destination_records_ids_are_unique_32_char_hex():
    df = transform_to_df_destination_records(
        df_source_records=df_source_records, extracted_at=datetime(2024, 12, 5, 12, 0, tzinfo=UTC)
    )
    ids = df["bizon_id"].to_list()
    assert len(set(ids)) == 2
    assert all(len(i) == 32 and int(i, 16) >= 0 for i in ids)


def test_empty_source_frame_keeps_the_destination_schema():
    df = transform_to_df_destination_records(
        df_source_records=df_source_records.clear(), extracted_at=datetime(2024, 12, 5, 12, 0, tzinfo=UTC)
    )
    assert df.height == 0
    assert df.schema == destination_record_schema
