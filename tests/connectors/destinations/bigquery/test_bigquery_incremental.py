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
        """Test finalize() for FULL_REFRESH replaces the table with a free copy job (WRITE_TRUNCATE)."""
        from google.cloud import bigquery

        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        mock_copy = MagicMock()
        destination.bq_client.copy_table = mock_copy
        destination.bq_client.query = MagicMock()
        destination.bq_client.get_table = MagicMock()
        destination.bq_client.delete_table = MagicMock()

        result = destination.finalize()

        assert result is True
        # Finalize must use a copy job, not query DML (no bytes billed)
        destination.bq_client.query.assert_not_called()
        mock_copy.assert_called_once()
        args, kwargs = mock_copy.call_args
        assert args[0] == destination.temp_table_id
        assert args[1] == destination.table_id
        assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
        # Temp table is cleaned up
        destination.bq_client.delete_table.assert_called_once()

    def test_finalize_incremental(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Test finalize() for INCREMENTAL appends via a free copy job (WRITE_APPEND)."""
        from google.cloud import bigquery

        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        mock_copy = MagicMock()
        destination.bq_client.copy_table = mock_copy
        destination.bq_client.query = MagicMock()
        destination.bq_client.delete_table = MagicMock()

        result = destination.finalize()

        assert result is True
        # No query DML — append is done with a copy job
        destination.bq_client.query.assert_not_called()
        mock_copy.assert_called_once()
        args, kwargs = mock_copy.call_args
        assert args[0] == destination.temp_table_id
        assert args[1] == destination.table_id
        assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND
        destination.bq_client.delete_table.assert_called_once()

    def test_finalize_incremental_first_run(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """INCREMENTAL first run (no main table): a WRITE_APPEND copy job creates it, no DML needed."""
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        mock_copy = MagicMock()
        destination.bq_client.copy_table = mock_copy
        destination.bq_client.query = MagicMock()
        destination.bq_client.delete_table = MagicMock()
        # Main table does not exist yet on the first incremental run
        destination.bq_client.get_table = MagicMock(side_effect=NotFound("Table not found"))

        result = destination.finalize()

        assert result is True
        destination.bq_client.query.assert_not_called()
        mock_copy.assert_called_once()
        _, kwargs = mock_copy.call_args
        assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND

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

        mock_query = MagicMock()
        destination.bq_client.query = mock_query
        mock_copy = MagicMock()
        destination.bq_client.copy_table = mock_copy

        result = destination.finalize()

        assert result is True
        # STREAM mode writes directly to the final table: no query, no copy
        mock_query.assert_not_called()
        mock_copy.assert_not_called()
