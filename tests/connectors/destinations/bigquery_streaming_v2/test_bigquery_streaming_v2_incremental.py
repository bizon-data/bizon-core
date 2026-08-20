"""Tests for BigQuery Streaming V2 destination incremental sync mode."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery_streaming_v2.src.config import (
    BigQueryStreamingV2ConfigDetails,
)
from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import (
    BigQueryStreamingV2Destination,
)
from bizon.source.config import SourceSyncModes


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    with patch("bizon.connectors.destinations.bigquery_streaming_v2.src.destination.bigquery.Client") as mock:
        yield mock


@pytest.fixture
def streaming_v2_config():
    """Create a BigQuery Streaming V2 config for testing."""
    return BigQueryStreamingV2ConfigDetails(
        project_id="test-project",
        dataset_id="test_dataset",
    )


def create_sync_metadata(sync_mode: SourceSyncModes) -> SyncMetadata:
    """Create SyncMetadata with specified sync mode."""
    return SyncMetadata(
        name="test_pipeline",
        job_id="test_job_123",
        source_name="test_source",
        stream_name="test_stream",
        destination_name="bigquery_streaming_v2",
        destination_alias="bigquery",
        sync_mode=sync_mode.value,
    )


class TestBigQueryStreamingV2TempTableId:
    """Test cases for temp_table_id property."""

    def test_temp_table_id_full_refresh(self, streaming_v2_config, mock_bq_client):
        """Test that temp_table_id returns _temp suffix for FULL_REFRESH mode."""
        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        expected_table_id = f"{destination.table_id}_temp"
        assert destination.temp_table_id == expected_table_id
        assert destination.temp_table_id.endswith("_temp")

    def test_temp_table_id_incremental(self, streaming_v2_config, mock_bq_client):
        """Test that temp_table_id returns _incremental suffix for INCREMENTAL mode."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        expected_table_id = f"{destination.table_id}_incremental"
        assert destination.temp_table_id == expected_table_id
        assert destination.temp_table_id.endswith("_incremental")

    def test_temp_table_id_stream(self, streaming_v2_config, mock_bq_client):
        """Test that temp_table_id returns main table_id for STREAM mode (no temp table)."""
        sync_metadata = create_sync_metadata(SourceSyncModes.STREAM)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # STREAM mode writes directly to main table
        assert destination.temp_table_id == destination.table_id
        assert not destination.temp_table_id.endswith("_temp")
        assert not destination.temp_table_id.endswith("_incremental")


class TestBigQueryStreamingV2Finalize:
    """Test cases for finalize() method."""

    def test_finalize_full_refresh(self, streaming_v2_config, mock_bq_client):
        """Test finalize() for FULL_REFRESH mode creates/replaces table."""
        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        mock_query.return_value.result = MagicMock()
        destination.bq_client.query = mock_query
        destination.bq_client.delete_table = MagicMock()
        # Destination table does not exist yet, so the DDL may carry the layout clauses.
        destination.bq_client.get_table = MagicMock(side_effect=NotFound("missing"))

        result = destination.finalize()

        assert result is True
        # Check that CREATE OR REPLACE was called, carrying the configured partitioning: a plain
        # CTAS produces an unpartitioned table that BigQuery then refuses to ever repartition.
        mock_query.assert_called_once()
        query_call = mock_query.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in query_call
        assert "PARTITION BY TIMESTAMP_TRUNC(`_bizon_loaded_at`, DAY)" in query_call

    def test_finalize_incremental(self, streaming_v2_config, mock_bq_client):
        """Test finalize() for INCREMENTAL mode appends data."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        # Mock the query method
        mock_query = MagicMock()
        mock_query.return_value.result = MagicMock()
        destination.bq_client.query = mock_query
        destination.bq_client.delete_table = MagicMock()
        destination.bq_client.create_table = MagicMock()
        destination.bq_client.get_table = MagicMock(side_effect=NotFound("missing"))

        result = destination.finalize()

        assert result is True
        # The main table is created first: INSERT INTO 404s against a missing table.
        destination.bq_client.create_table.assert_called_once()
        # Check that INSERT INTO was called
        mock_query.assert_called_once()
        query_call = mock_query.call_args[0][0]
        assert "INSERT INTO" in query_call

    def test_finalize_stream(self, streaming_v2_config, mock_bq_client):
        """Test finalize() for STREAM mode does nothing."""
        sync_metadata = create_sync_metadata(SourceSyncModes.STREAM)

        destination = BigQueryStreamingV2Destination(
            sync_metadata=sync_metadata,
            config=streaming_v2_config,
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
