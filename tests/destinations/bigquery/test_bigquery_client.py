import logging
import os
import random
import uuid
from datetime import datetime
from random import randint

import pytest
from faker import Faker
from google.cloud import bigquery

from bizon.common.models import SyncMetadata
from bizon.destinations.bigquery.src.config import (
    BigQueryConfig,
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destinations.config import DestinationTypes
from bizon.destinations.destination import DestinationFactory
from bizon.destinations.models import DestinationRecord

logger = logging.getLogger(__name__)

fake = Faker("en_US")

TEST_PROJECT_ID = "my_project"
TEST_TABLE_ID = "test_fake_records"
TEST_DATASET_ID = "bizon_test"
TEST_BUFFER_BUCKET = "bizon-buffer"


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

    destination_records = [
        DestinationRecord(
            source_data=record,
            bizon_id=str(uuid.uuid4()),
            bizon_extracted_at=datetime.utcnow(),
            bizon_loaded_at=datetime.utcnow(),
            source_record_id=str(uuid.uuid4()),
            source_timestamp=datetime.utcnow(),
        )
        for record in fake_records
    ]

    success, error_msg = bq_destination.write_records(destination_records)

    assert success is True
    assert error_msg == ""
