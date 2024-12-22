from datetime import datetime

import polars as pl
import pytest

from bizon.connectors.destinations.bigquery.src.config import BigQueryColumn
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destination.models import destination_record_schema


def test_unnest_records_to_bigquery():
    df_destination_records = pl.DataFrame(
        data={
            "source_record_id": ["1"],
            "source_timestamp": [datetime.now()],
            "source_data": ['{"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00"}'],
            "bizon_extracted_at": [datetime.now()],
            "bizon_loaded_at": [datetime.now()],
            "bizon_id": ["1"],
        },
        schema=destination_record_schema,
    )

    assert df_destination_records.height > 0

    res = BigQueryDestination.unnest_data(
        df_destination_records=df_destination_records,
        record_schema=[
            BigQueryColumn(name="id", type="INTEGER", mode="REQUIRED"),
            BigQueryColumn(name="name", type="STRING", mode="REQUIRED"),
            BigQueryColumn(name="created_at", type="DATETIME", mode="REQUIRED"),
        ],
    )

    assert res.height == 1


def test_unnest_records_to_bigquery_with_invalid_data():
    df_destination_records = pl.DataFrame(
        data={
            "source_record_id": ["1"],
            "source_timestamp": [datetime.now()],
            "source_data": ['{"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00", "cookies": "chocolate"}'],
            "bizon_extracted_at": [datetime.now()],
            "bizon_loaded_at": [datetime.now()],
            "bizon_id": ["1"],
        },
        schema=destination_record_schema,
    )

    # We raise exception as the data has an extra column compared to BigQuery schema

    with pytest.raises(AssertionError) as e:
        res = BigQueryDestination.unnest_data(
            df_destination_records=df_destination_records,
            record_schema=[
                BigQueryColumn(name="id", type="INTEGER", mode="REQUIRED"),
                BigQueryColumn(name="name", type="STRING", mode="REQUIRED"),
                BigQueryColumn(name="created_at", type="DATETIME", mode="REQUIRED"),
            ],
        )
