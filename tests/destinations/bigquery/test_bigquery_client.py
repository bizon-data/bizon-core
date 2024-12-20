import logging
import os
import random
from datetime import datetime
from random import randint

import polars as pl
import pytest
from faker import Faker
from google.cloud import bigquery

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfig,
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destination.config import DestinationTypes
from bizon.destination.destination import DestinationFactory
from bizon.destination.models import destination_record_schema

logger = logging.getLogger(__name__)

fake = Faker("en_US")

TEST_PROJECT_ID = "my_project"
TEST_TABLE_ID = "test_fake_records"
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
def sync_metadata() -> SyncMetadata:
    return SyncMetadata(
        name="gcs_loading",
        job_id="rfou98C9DJH",
        source_name="cookie",
        stream_name="test",
        destination_name="bigquery",
        sync_mode="full_refresh",
    )


@pytest.fixture(scope="function")
def test_table():
    table_id = f"{TEST_PROJECT_ID}.{TEST_DATASET_ID}.{TEST_TABLE_ID}"
    client = bigquery.Client()
    table = bigquery.Table(table_id)
    table = client.create_table(table)
    yield table
    client.delete_table(table_id)
    client.delete_table(f"{table_id}_temp")


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_load_records_to_bigquery(my_backend_config, test_table, sync_metadata):
    bigquery_config = BigQueryConfig(
        name=DestinationTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            gcs_buffer_bucket=TEST_BUFFER_BUCKET,
            gcs_buffer_format=GCSBufferFormat.PARQUET,
        ),
    )
    fake_records = [
        {"foo": randint(0, 100), "bar": {"baz": fake.name(), "poo": float(random.randrange(155, 389)) / 100}}
        for _ in range(100)
    ]

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata, config=bigquery_config, backend=my_backend_config
    )

    assert isinstance(bq_destination, BigQueryDestination)

    success, error_msg = bq_destination.write_records(df_destination_records=df_destination_records)

    assert success is True
    assert error_msg == ""
