from unittest.mock import MagicMock, patch

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

MODULE = "bizon.connectors.destinations.bigquery.src.destination"


@pytest.fixture(scope="function")
def sync_metadata() -> SyncMetadata:
    return SyncMetadata(
        name="factory_test",
        job_id="rfou98C9DJH",
        source_name="cookie",
        stream_name="test",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode="full_refresh",
    )


def _get_destination(sync_metadata: SyncMetadata, **config_overrides) -> BigQueryDestination:
    config = BigQueryConfig(
        name=DestinationTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            project_id="project_id",
            dataset_id="dataset_id",
            gcs_buffer_bucket="gcs_buffer_bucket",
            gcs_buffer_format=GCSBufferFormat.PARQUET,
            **config_overrides,
        ),
    )
    with patch(f"{MODULE}.bigquery.Client"), patch(f"{MODULE}.storage.Client"):
        return DestinationFactory().get_destination(
            sync_metadata=sync_metadata,
            config=config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )


def test_bigquery_factory(sync_metadata):
    destination = _get_destination(sync_metadata, authentication={"service_account_key": ""})

    assert isinstance(destination, BigQueryDestination)
    assert destination.config.authentication.service_account_key == ""
    assert destination.config.project_id == "project_id"
    assert destination.config.dataset_id == "dataset_id"


def test_bigquery_factory_without_authentication(sync_metadata):
    destination = _get_destination(sync_metadata)

    assert isinstance(destination, BigQueryDestination)
    assert destination.config.authentication is None
