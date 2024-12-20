from datetime import datetime

import polars as pl
from pytz import UTC

from bizon.destination.models import transform_to_df_destination_records
from bizon.source.models import source_record_schema

df_source_records = pl.DataFrame(
    {
        "id": ["record_1", "record_2"],
        "data": ['{"key": "value1"}', '{"key": "value2"}'],
        "timestamp": [datetime(2024, 12, 5, 11, 30, tzinfo=UTC), datetime(2024, 12, 5, 12, 30, tzinfo=UTC)],
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
