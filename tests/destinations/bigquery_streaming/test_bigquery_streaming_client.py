import json
import os
from datetime import datetime

import polars as pl
import pytest
from dotenv import load_dotenv
from faker import Faker
from google.protobuf.json_format import ParseError
from google.protobuf.message import EncodeError

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery_streaming.src.config import (
    BigQueryStreamingConfig,
    BigQueryStreamingConfigDetails,
)
from bizon.destination.config import DestinationTypes
from bizon.destination.destination import DestinationFactory
from bizon.destination.models import destination_record_schema

load_dotenv()


fake = Faker("en_US")


TEST_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "test_project")
TEST_TABLE_ID = "test_fake_records_streaming"
TEST_DATASET_ID = "bizon_test"
TEST_BUFFER_BUCKET = "bizon-buffer"


df_destination_records = pl.DataFrame(
    {
        "bizon_id": ["id_1", "id_2"],
        "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
        "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
        "source_record_id": ["record_1", "record_2"],
        "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
        "source_data": ["cookies", "cream"],
    },
    schema=destination_record_schema,
)


@pytest.fixture(scope="function")
def sync_metadata_stream() -> SyncMetadata:
    return SyncMetadata(
        name="streaming_api",
        job_id="rfou98C9DJH",
        source_name="cookie_test",
        stream_name="test_stream",
        destination_name="bigquery",
        sync_mode="stream",
    )


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_streaming_records_to_bigquery(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream, config=bigquery_config, backend=my_backend_config
    )

    # Import here to not throw auth errors when running tests
    from bizon.connectors.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

    success, error_msg = bq_destination.write_records(df_destination_records=df_destination_records)

    assert success is True
    assert error_msg == ""


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_streaming_unnested_records(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                },
                {
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED",
                },
                {
                    "name": "created_at",
                    "type": "DATETIME",
                    "mode": "REQUIRED",
                },
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream, config=bigquery_config, backend=my_backend_config
    )

    # Import here to not throw auth errors when running tests
    from bizon.connectors.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

    records = [
        {"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00"},
        {"id": 2, "name": "Bob", "created_at": "2021-01-01 00:00:00"},
    ]

    df_unnested_records = pl.DataFrame(
        {
            "bizon_id": ["id_1", "id_2"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
            "source_record_id": ["record_1", "record_2"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
            "source_data": [json.dumps(record) for record in records],
        },
        schema=destination_record_schema,
    )

    success, error_msg = bq_destination.write_records(df_destination_records=df_unnested_records)

    assert success is True
    assert error_msg == ""


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_error_on_added_column(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                },
                {
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED",
                },
                {
                    "name": "created_at",
                    "type": "DATETIME",
                    "mode": "REQUIRED",
                },
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream, config=bigquery_config, backend=my_backend_config
    )

    # Insert proper records
    records = [
        {"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00"},
        {"id": 2, "name": "Bob", "created_at": "2021-01-01 00:00:00"},
    ]
    df_unnested_records = pl.DataFrame(
        {
            "bizon_id": ["id_1", "id_2"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
            "source_record_id": ["record_1", "record_2"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
            "source_data": [json.dumps(record) for record in records],
        },
        schema=destination_record_schema,
    )

    bq_destination.write_records(df_destination_records=df_unnested_records)

    # Try to insert a new record with an added column

    new_column_in_record = {"id": 3, "name": "Charlie", "last_name": "Chaplin", "created_at": "2021-01-01 00:00:00"}

    df_new = pl.DataFrame(
        {
            "bizon_id": ["id_3"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30)],
            "source_record_id": ["record_3"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30)],
            "source_data": [json.dumps(new_column_in_record)],
        },
        schema=destination_record_schema,
    )

    # The call should raise an error because the new record has an extra column
    with pytest.raises(ParseError):
        bq_destination.write_records(df_destination_records=df_new)


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_error_on_deleted_column(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                },
                {
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED",
                },
                {
                    "name": "created_at",
                    "type": "DATETIME",
                    "mode": "REQUIRED",
                },
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream, config=bigquery_config, backend=my_backend_config
    )

    # Insert proper records
    records = [
        {"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00"},
        {"id": 2, "name": "Bob", "created_at": "2021-01-01 00:00:00"},
    ]
    df_unnested_records = pl.DataFrame(
        {
            "bizon_id": ["id_1", "id_2"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
            "source_record_id": ["record_1", "record_2"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
            "source_data": [json.dumps(record) for record in records],
        },
        schema=destination_record_schema,
    )

    bq_destination.write_records(df_destination_records=df_unnested_records)

    # Try to insert a new record with a deleted column
    new_column_in_record = {"id": 3, "created_at": "2021-01-01 00:00:00"}

    df_new = pl.DataFrame(
        {
            "bizon_id": ["id_3"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30)],
            "source_record_id": ["record_3"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30)],
            "source_data": [json.dumps(new_column_in_record)],
        },
        schema=destination_record_schema,
    )

    # The call should raise an error because the new record has a missing column
    with pytest.raises(EncodeError):
        bq_destination.write_records(df_destination_records=df_new)
