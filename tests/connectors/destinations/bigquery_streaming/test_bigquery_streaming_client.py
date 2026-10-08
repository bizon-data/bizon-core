import json
import os
from datetime import datetime

import polars as pl
import pytest
from dotenv import load_dotenv

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery_streaming.src.config import (
    BigQueryStreamingConfig,
    BigQueryStreamingConfigDetails,
)
from bizon.destination.config import DestinationTypes
from bizon.destination.destination import DestinationFactory
from bizon.destination.models import destination_record_schema
from bizon.monitoring.noop.monitor import NoOpMonitor

# Talks to real GCP; CI deselects it with -m 'not live'.
pytestmark = pytest.mark.live

load_dotenv()


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
        "source_data": ['{"cookies": 2, "price": 3, "cream": 1}', '{"cookies": 1, "price": 2, "cream": 2}'],
    },
    schema=destination_record_schema,
)


@pytest.fixture(scope="function")
def sync_metadata_stream() -> SyncMetadata:
    return SyncMetadata(
        name="streaming_api",
        job_id="rfou98C9DJH",
        source_name="cookie_test",
        stream_name="test_stream_2",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode="stream",
    )


def test_streaming_records_to_bigquery(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
    )

    # Import here to not throw auth errors when running tests
    from bizon.connectors.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

    success, error_msg = bq_destination.write_records(df_destination_records=df_destination_records)

    assert success is True
    assert error_msg == ""


def test_override_destination_id_streaming_records_to_bigquery(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            destination_id=f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_override",
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
    )

    # Import here to not throw auth errors when running tests
    from bizon.connectors.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

    success, error_msg = bq_destination.write_records(df_destination_records=df_destination_records)

    assert success is True
    assert error_msg == ""


def test_streaming_large_records_to_bigquery(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
    )

    # Import here to not throw auth errors when running tests
    from bizon.connectors.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

    df_destination_records = pl.DataFrame(
        {
            "bizon_id": ["id_1", "id_2"],
            "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
            "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
            "source_record_id": ["record_1", "record_2"],
            "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
            "source_data": ["the_next_record_is_large", json.dumps({"data": "y" * 2_000_000})],
        },
        schema=destination_record_schema,
    )

    success, error_msg = bq_destination.write_records(df_destination_records=df_destination_records)

    assert success is True
    assert error_msg == ""


def test_streaming_unnested_records(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schemas=[
                {
                    "destination_id": f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_unnested",
                    "record_schema": [
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
                }
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
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


def test_enforce_record_schema_columns(my_backend_config, sync_metadata_stream):
    """
    Test that the record schema is enforced on the destination.
    Any extra columns not in the record schema will be ignored.
    Any columns in the record schema that are not present in the record will be set to NULL.
    """
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schemas=[
                {
                    "destination_id": f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_enforce_schema",
                    "record_schema": [
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
                }
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
    )

    # Insert proper records
    records = [
        {"id": 1, "name": "Alice", "created_at": "2021-01-01 00:00:00", "column_not_in_schema": "value"},
        {"id": 2, "name": "Bob", "created_at": "2021-01-01 00:00:00", "column_not_in_schema": "value"},
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

    # Try to insert a new record with a new schema

    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schemas=[
                {
                    "destination_id": f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_propagate",
                    "record_schema": [
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
                        {
                            "name": "last_name",
                            "type": "STRING",
                            "mode": "NULLABLE",
                        },
                    ],
                }
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
    )
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

    success, error_msg = bq_destination.write_records(df_destination_records=df_new)

    assert success is True
    assert error_msg == ""


def test_streaming_unnested_records_legacy(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schemas=[
                {
                    "destination_id": f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_legacy",
                    "record_schema": [
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
                }
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
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


def test_streaming_unnested_records_clustering_keys(my_backend_config, sync_metadata_stream):
    bigquery_config = BigQueryStreamingConfig(
        name=DestinationTypes.BIGQUERY_STREAMING,
        config=BigQueryStreamingConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            unnest=True,
            time_partitioning={"type": "DAY", "field": "created_at"},
            record_schemas=[
                {
                    "destination_id": f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}_clustering_keys",
                    "clustering_keys": ["id"],
                    "record_schema": [
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
                }
            ],
        ),
    )

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream,
        config=bigquery_config,
        backend=my_backend_config,
        source_callback=None,
        monitor=NoOpMonitor(sync_metadata=sync_metadata_stream, monitoring_config=None),
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
