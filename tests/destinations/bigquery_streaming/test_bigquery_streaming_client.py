import os
import uuid
from datetime import datetime
from random import randint, randrange

import pytest
from faker import Faker

from bizon.common.models import SyncMetadata
from bizon.destinations.bigquery_streaming.src.config import (
    BigQueryStreamingConfig,
    BigQueryStreamingConfigDetails,
)
from bizon.destinations.config import DestinationTypes
from bizon.destinations.destination import DestinationFactory
from bizon.destinations.models import DestinationRecord

fake = Faker("en_US")


TEST_PROJECT_ID = "my_project"
TEST_TABLE_ID = "test_fake_records"
TEST_DATASET_ID = "bizon_test"
TEST_BUFFER_BUCKET = "bizon-buffer"


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
    fake_records = [
        {"foo": randint(0, 100), "bar": {"baz": fake.name(), "poo": float(randrange(155, 389)) / 100}}
        for _ in range(100)
    ]

    bq_destination = DestinationFactory().get_destination(
        sync_metadata=sync_metadata_stream, config=bigquery_config, backend=my_backend_config
    )

    # Import here to not throw auth errors when running tests
    from bizon.destinations.bigquery_streaming.src.destination import (
        BigQueryStreamingDestination,
    )

    assert isinstance(bq_destination, BigQueryStreamingDestination)

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
