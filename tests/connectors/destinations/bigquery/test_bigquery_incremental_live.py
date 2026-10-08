"""Live integration tests for BigQuery destination incremental sync mode.

These tests hit real BigQuery APIs and require valid GCP credentials.
Run with: uv run pytest tests/connectors/destinations/bigquery/test_bigquery_incremental_live.py -v
"""

import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pytz import UTC

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.source.config import SourceSyncModes

# Talks to real GCP; CI deselects it with -m 'not live'.
pytestmark = pytest.mark.live

# Test configuration - update these values for your GCP project
PROJECT_ID = "my-gcp-project"
DATASET_ID = "bizon_test"
GCS_BUCKET = "my-gcs-buffer-bucket"


@pytest.fixture
def test_run_id():
    """Generate unique ID for this test run to avoid conflicts."""
    return str(uuid.uuid4())[:8]


@pytest.fixture
def bigquery_config():
    """Create a BigQuery config for live testing."""
    return BigQueryConfigDetails(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        gcs_buffer_bucket=GCS_BUCKET,
        gcs_buffer_format=GCSBufferFormat.PARQUET,
    )


@pytest.fixture
def bq_client():
    """Create a real BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def create_sync_metadata(sync_mode: SourceSyncModes, test_run_id: str) -> SyncMetadata:
    """Create SyncMetadata with specified sync mode."""
    return SyncMetadata(
        name="test_pipeline",
        job_id=f"test_job_{test_run_id}",
        source_name="test_source",
        stream_name=f"test_stream_{test_run_id}",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=sync_mode.value,
    )


def cleanup_tables(bq_client: bigquery.Client, table_ids: list[str]):
    """Clean up test tables."""
    for table_id in table_ids:
        try:
            bq_client.delete_table(table_id, not_found_ok=True)
        except Exception as e:
            print(f"Warning: Could not delete table {table_id}: {e}")


class TestBigQueryIncrementalLive:
    """Live integration tests for incremental sync."""

    def test_temp_table_id_incremental_live(self, bigquery_config, test_run_id):
        """Test temp_table_id property with real destination."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL, test_run_id)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        assert destination.temp_table_id.endswith("_incremental")
        assert PROJECT_ID in destination.temp_table_id
        assert DATASET_ID in destination.temp_table_id

    def test_finalize_incremental_live(self, bigquery_config, bq_client, test_run_id):
        """Test finalize() for INCREMENTAL mode with real BigQuery tables."""
        sync_metadata = create_sync_metadata(SourceSyncModes.INCREMENTAL, test_run_id)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        main_table_id = destination.table_id
        temp_table_id = destination.temp_table_id

        try:
            # Create schema
            schema = [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

            # Create main table with initial data
            main_table = bigquery.Table(main_table_id, schema=schema)
            bq_client.create_table(main_table, exists_ok=True)

            # Insert initial row into main table
            initial_rows = [
                {
                    "_source_record_id": "initial_1",
                    "_source_timestamp": datetime.now(tz=UTC).isoformat(),
                    "_source_data": '{"key": "initial"}',
                    "_bizon_extracted_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_loaded_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_id": "bizon_initial_1",
                }
            ]
            bq_client.insert_rows_json(main_table_id, initial_rows)

            # Create temp incremental table with new data
            temp_table = bigquery.Table(temp_table_id, schema=schema)
            bq_client.create_table(temp_table, exists_ok=True)

            # Insert incremental row into temp table
            incremental_rows = [
                {
                    "_source_record_id": "incremental_1",
                    "_source_timestamp": datetime.now(tz=UTC).isoformat(),
                    "_source_data": '{"key": "incremental"}',
                    "_bizon_extracted_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_loaded_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_id": "bizon_incremental_1",
                }
            ]
            bq_client.insert_rows_json(temp_table_id, incremental_rows)

            # Wait for streaming buffer to be available
            import time

            time.sleep(2)

            # Run finalize
            result = destination.finalize()

            assert result is True

            # Verify temp table was deleted
            with pytest.raises(NotFound):
                bq_client.get_table(temp_table_id)

            # Verify main table has both rows (initial + incremental)
            query = f"SELECT COUNT(*) as count FROM `{main_table_id}`"
            query_job = bq_client.query(query)
            results = list(query_job.result())
            assert results[0].count == 2

        finally:
            cleanup_tables(bq_client, [main_table_id, temp_table_id])

    def test_finalize_full_refresh_live(self, bigquery_config, bq_client, test_run_id):
        """Test finalize() for FULL_REFRESH mode with real BigQuery tables."""
        sync_metadata = create_sync_metadata(SourceSyncModes.FULL_REFRESH, test_run_id)

        destination = BigQueryDestination(
            sync_metadata=sync_metadata,
            config=bigquery_config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        main_table_id = destination.table_id
        temp_table_id = destination.temp_table_id

        try:
            # Create schema
            schema = [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

            # Create temp table with fresh data
            temp_table = bigquery.Table(temp_table_id, schema=schema)
            bq_client.create_table(temp_table, exists_ok=True)

            # Insert row into temp table
            rows = [
                {
                    "_source_record_id": "fresh_1",
                    "_source_timestamp": datetime.now(tz=UTC).isoformat(),
                    "_source_data": '{"key": "fresh"}',
                    "_bizon_extracted_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_loaded_at": datetime.now(tz=UTC).isoformat(),
                    "_bizon_id": "bizon_fresh_1",
                }
            ]
            bq_client.insert_rows_json(temp_table_id, rows)

            # Wait for streaming buffer
            import time

            time.sleep(2)

            # Run finalize
            result = destination.finalize()

            assert result is True

            # Verify temp table was deleted
            with pytest.raises(NotFound):
                bq_client.get_table(temp_table_id)

            # Verify main table exists with data
            query = f"SELECT COUNT(*) as count FROM `{main_table_id}`"
            query_job = bq_client.query(query)
            results = list(query_job.result())
            assert results[0].count == 1

        finally:
            cleanup_tables(bq_client, [main_table_id, temp_table_id])
