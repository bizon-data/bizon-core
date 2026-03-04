"""Tests for BigQuery destination incremental sync mode."""

from unittest.mock import MagicMock, patch

import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.source.config import SourceSyncModes


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    with patch("bizon.connectors.destinations.bigquery.src.destination.bigquery.Client") as mock:
        yield mock


@pytest.fixture
def mock_gcs_client():
    """Mock GCS client."""
    with patch("bizon.connectors.destinations.bigquery.src.destination.storage.Client") as mock:
        yield mock


@pytest.fixture
def bigquery_config():
    """Create a BigQuery config for testing."""
    return BigQueryConfigDetails(
        project_id="test-project",
        dataset_id="test_dataset",
        gcs_buffer_bucket="test-bucket",
        gcs_buffer_format=GCSBufferFormat.PARQUET,
    )


def create_sync_metadata(sync_mode: SourceSyncModes) -> SyncMetadata:
    """Create SyncMetadata with specified sync mode."""
    return SyncMetadata(
        name="test_pipeline",
        job_id="test_job_123",
        source_name="test_source",
        stream_name="test_stream",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=sync_mode.value,
    )


class TestBigQueryTempTableId:
    """Test cases for temp_table_id property."""

    def test_temp_table_id_full_refresh(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test that temp_table_id returns _temp suffix for FULL_REFRESH mode."""
        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        expected_table_id = f"{destination.table_id}_temp"
        assert destination.temp_table_id == expected_table_id
        assert destination.temp_table_id.endswith("_temp")

    def test_temp_table_id_incremental(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test that temp_table_id returns _incremental suffix for INCREMENTAL mode."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        expected_table_id = f"{destination.table_id}_incremental"
        assert destination.temp_table_id == expected_table_id
        assert destination.temp_table_id.endswith("_incremental")

    def test_temp_table_id_stream(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test that temp_table_id returns main table_id for STREAM mode (no temp table)."""
        sync_metadata = create_sync_metadata(SourceSyncModes.STREAM)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # STREAM mode writes directly to main table
        assert destination.temp_table_id == destination.table_id
        assert not destination.temp_table_id.endswith("_temp")
        assert not destination.temp_table_id.endswith("_incremental")


class TestBigQueryFinalize:
    """Test cases for finalize() method."""

    def test_finalize_full_refresh(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test finalize() for FULL_REFRESH mode creates/replaces table."""
        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        destination.bq_client.query = mock_query
        destination.bq_client.delete_table = MagicMock()

        result = destination.finalize()

        assert result is True
        # Check that CREATE OR REPLACE was called
        mock_query.assert_called_once()
        query_call = mock_query.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in query_call

    def test_finalize_incremental(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test finalize() for INCREMENTAL mode appends data when table exists."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        mock_query.return_value.result = MagicMock()
        destination.bq_client.query = mock_query
        destination.bq_client.delete_table = MagicMock()
        # Mock get_table to simulate table exists
        destination.bq_client.get_table = MagicMock()

        result = destination.finalize()

        assert result is True
        # Check that INSERT INTO was called
        mock_query.assert_called_once()
        query_call = mock_query.call_args[0][0]
        assert "INSERT INTO" in query_call

    def test_finalize_incremental_first_run(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test finalize() for INCREMENTAL mode creates table on first run."""
        from google.api_core.exceptions import NotFound

        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        mock_query.return_value.result = MagicMock()
        destination.bq_client.query = mock_query
        destination.bq_client.delete_table = MagicMock()
        # Mock get_table to simulate table does NOT exist
        destination.bq_client.get_table = MagicMock(side_effect=NotFound("Table not found"))

        result = destination.finalize()

        assert result is True
        # Check that CREATE TABLE was called instead of INSERT INTO
        mock_query.assert_called_once()
        query_call = mock_query.call_args[0][0]
        assert "CREATE TABLE" in query_call
        assert "INSERT INTO" not in query_call

    def test_finalize_stream(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test finalize() for STREAM mode does nothing."""
        sync_metadata = create_sync_metadata(SourceSyncModes.STREAM)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        destination.bq_client.query = mock_query

        result = destination.finalize()

        assert result is True
        # STREAM mode should not call query
        mock_query.assert_not_called()
