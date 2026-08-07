"""Tests for BigQuery destination incremental sync mode."""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from bizon.common.models import BizonConfig, SyncMetadata
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


def create_sync_metadata(sync_mode: SourceSyncModes, reset: bool = False) -> SyncMetadata:
    """Create SyncMetadata with specified sync mode."""
    return SyncMetadata(
        name="test_pipeline",
        job_id="test_job_123",
        source_name="test_source",
        stream_name="test_stream",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=sync_mode.value,
        reset=reset,
    )


RESET_CONFIG = """
name: test_pipeline
source:
  name: dummy
  stream: creatures
  sync_mode: incremental
  cursor_field: updated_at
  reset: {reset}
  authentication: {{type: api_key, params: {{token: t}}}}
destination:
  name: bigquery
  config: {{project_id: test-project, dataset_id: test_dataset, gcs_buffer_bucket: test-bucket}}
"""


def build_reset_config(reset: bool = True) -> BizonConfig:
    """An incremental config with `reset` set, as the runner would hand it to the destination."""
    return BizonConfig.model_validate(obj=yaml.safe_load(RESET_CONFIG.format(reset=str(reset).lower())))


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


class TestBigQueryStreamReset:
    """Test cases for stream reset: an incremental job that replaces the table for one run."""

    def _destination(self, bigquery_config, backend=None, reset=True):
        # Built from a real config through from_bizon_config, so these exercise the actual wiring:
        # an `incremental` + `reset` config is what has to reach the destination as a full refresh.
        return BigQueryDestination(
            sync_metadata=SyncMetadata.from_bizon_config(job_id="test_job_123", config=build_reset_config(reset=reset)),
            config=bigquery_config,
            backend=backend or MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

    def test_temp_table_id_uses_full_refresh_staging(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """A reset stages into the full-refresh temp table, since it publishes with WRITE_TRUNCATE."""
        destination = self._destination(bigquery_config)

        assert destination.temp_table_id == f"{destination.table_id}_temp"

    def test_finalize_replaces_table(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """A reset must replace the table (WRITE_TRUNCATE), not append to it like a normal incremental."""
        from google.cloud import bigquery

        destination = self._destination(bigquery_config)

        mock_copy = MagicMock()
        destination.bq_client.copy_table = mock_copy
        destination.bq_client.query = MagicMock()
        destination.bq_client.get_table = MagicMock()
        destination.bq_client.delete_table = MagicMock()

        assert destination.finalize() is True

        destination.bq_client.query.assert_not_called()
        _, kwargs = mock_copy.call_args
        assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

    def test_stale_temp_table_is_dropped_on_a_fresh_reset(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Rows left by an earlier crashed run must not survive into the replaced table."""
        backend = MagicMock()
        backend.get_last_cursor_by_job_id.return_value = None
        destination = self._destination(bigquery_config, backend=backend)
        destination.bq_client.delete_table = MagicMock()

        destination._ensure_clean_temp_table()

        destination.bq_client.delete_table.assert_called_once_with(destination.temp_table_id, not_found_ok=True)

    def test_temp_table_is_kept_when_resuming_a_crashed_reset(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """The producer resumes from the last cursor, so already-written iterations must be kept."""
        backend = MagicMock()
        backend.get_last_cursor_by_job_id.return_value = MagicMock(to_source_iteration=4)
        destination = self._destination(bigquery_config, backend=backend)
        destination.bq_client.delete_table = MagicMock()

        destination._ensure_clean_temp_table()

        destination.bq_client.delete_table.assert_not_called()

    def test_temp_table_is_only_dropped_once(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """Both write paths call the guard on every flush; only the first may drop the table."""
        backend = MagicMock()
        backend.get_last_cursor_by_job_id.return_value = None
        destination = self._destination(bigquery_config, backend=backend)
        destination.bq_client.delete_table = MagicMock()

        destination._ensure_clean_temp_table()
        destination._ensure_clean_temp_table()

        destination.bq_client.delete_table.assert_called_once()

    def test_non_reset_run_never_drops_its_temp_table(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """A plain incremental appends into `_incremental` across runs and must leave it alone."""
        destination = self._destination(bigquery_config, reset=False)
        destination.bq_client.delete_table = MagicMock()

        destination._ensure_clean_temp_table()

        assert destination.temp_table_id == f"{destination.table_id}_incremental"
        destination.bq_client.delete_table.assert_not_called()

    def test_plain_full_refresh_drops_its_stale_temp_table(self, bigquery_config, mock_bq_client, mock_gcs_client):
        """A full refresh publishes with WRITE_TRUNCATE too, so it needs the same guard as a reset.

        Loads WRITE_APPEND into `_temp`, so rows left by a killed run would otherwise be published
        alongside the new ones - the duplicate accumulation seen in production (132k rows piled into
        `_temp` across seven killed attempts).
        """
        backend = MagicMock()
        backend.get_last_cursor_by_job_id.return_value = None
        destination = BigQueryDestination(
            sync_metadata=create_sync_metadata(SourceSyncModes.FULL_REFRESH),
            config=bigquery_config,
            backend=backend,
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )
        destination.bq_client.delete_table = MagicMock()

        destination._ensure_clean_temp_table()

        destination.bq_client.delete_table.assert_called_once_with(destination.temp_table_id, not_found_ok=True)
