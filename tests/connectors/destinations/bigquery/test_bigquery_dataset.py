"""Tests for the create_dataset flag on the BigQuery destination."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.source.config import SourceSyncModes


@pytest.fixture
def mock_bq_client():
    with patch("bizon.connectors.destinations.bigquery.src.destination.bigquery.Client") as mock:
        yield mock


@pytest.fixture
def mock_gcs_client():
    with patch("bizon.connectors.destinations.bigquery.src.destination.storage.Client") as mock:
        yield mock


def make_destination(mock_bq_client, mock_gcs_client, **config_overrides) -> BigQueryDestination:
    config = BigQueryConfigDetails(
        project_id="test-project",
        dataset_id="test_dataset",
        gcs_buffer_bucket="test-bucket",
        gcs_buffer_format=GCSBufferFormat.PARQUET,
        **config_overrides,
    )
    sync_metadata = SyncMetadata(
        name="p",
        job_id="j",
        source_name="s",
        stream_name="st",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=SourceSyncModes.FULL_REFRESH.value,
    )
    return BigQueryDestination(
        sync_metadata=sync_metadata,
        config=config,
        backend=MagicMock(),
        source_callback=MagicMock(),
        monitor=MagicMock(),
    )


def test_create_dataset_defaults_to_false():
    config = BigQueryConfigDetails(
        project_id="p", dataset_id="d", gcs_buffer_bucket="b", gcs_buffer_format=GCSBufferFormat.PARQUET
    )
    assert config.create_dataset is False


def test_ensure_dataset_creates_when_missing_and_flag_true(mock_bq_client, mock_gcs_client):
    dest = make_destination(mock_bq_client, mock_gcs_client, create_dataset=True)
    dest.bq_client.get_dataset = MagicMock(side_effect=NotFound("missing"))
    dest.bq_client.create_dataset = MagicMock()

    dest._ensure_dataset()

    dest.bq_client.create_dataset.assert_called_once()


def test_ensure_dataset_raises_when_missing_and_flag_false(mock_bq_client, mock_gcs_client):
    dest = make_destination(mock_bq_client, mock_gcs_client)  # create_dataset defaults False
    dest.bq_client.get_dataset = MagicMock(side_effect=NotFound("missing"))
    dest.bq_client.create_dataset = MagicMock()

    with pytest.raises(RuntimeError):
        dest._ensure_dataset()

    dest.bq_client.create_dataset.assert_not_called()


def test_ensure_dataset_noop_when_exists(mock_bq_client, mock_gcs_client):
    dest = make_destination(mock_bq_client, mock_gcs_client, create_dataset=True)
    dest.bq_client.get_dataset = MagicMock()  # exists
    dest.bq_client.create_dataset = MagicMock()

    dest._ensure_dataset()

    dest.bq_client.create_dataset.assert_not_called()


def test_ensure_dataset_only_checks_once(mock_bq_client, mock_gcs_client):
    dest = make_destination(mock_bq_client, mock_gcs_client, create_dataset=True)
    dest.bq_client.get_dataset = MagicMock()

    dest._ensure_dataset()
    dest._ensure_dataset()

    dest.bq_client.get_dataset.assert_called_once()
