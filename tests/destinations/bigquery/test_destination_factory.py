import os

import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfig,
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destination.config import DestinationTypes
from bizon.destination.destination import DestinationFactory


@pytest.fixture(scope="function")
def sync_metadata() -> SyncMetadata:
    return SyncMetadata(
        job_id="rfou98C9DJH",
        source_name="cookie",
        stream_name="test",
        destination_name="bigquery",
        sync_mode="full_refresh",
    )


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_bigquery_factory(sync_metadata, my_backend):
    config = BigQueryConfig(
        name=DestinationTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            project_id="project_id",
            dataset_id="dataset_id",
            table_id="table_id",
            credentials_path="credentials_path",
            gcs_buffer_bucket="gcs_buffer_bucket",
            gcs_buffer_format=GCSBufferFormat.PARQUET,
            authentication={"service_account_key": ""},
        ),
    )

    destination = DestinationFactory().get_destination(sync_metadata=sync_metadata, config=config, backend=my_backend)
    assert isinstance(destination, BigQueryDestination)
    assert destination.config.authentication.service_account_key == ""
    assert destination.config.project_id == "project_id"
    assert destination.config.dataset_id == "dataset_id"


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_bigquery_factory_empty_service_account(sync_metadata, my_backend):
    config = BigQueryConfig(
        name=DestinationTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            project_id="project_id",
            dataset_id="dataset_id",
            table_id="table_id",
            credentials_path="credentials_path",
            gcs_buffer_bucket="gcs_buffer_bucket",
            gcs_buffer_format=GCSBufferFormat.PARQUET,
        ),
    )

    destination = DestinationFactory().get_destination(sync_metadata=sync_metadata, config=config, backend=my_backend)
    assert isinstance(destination, BigQueryDestination)
    assert destination.config.authentication is None
