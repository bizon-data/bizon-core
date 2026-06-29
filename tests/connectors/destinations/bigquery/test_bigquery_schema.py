"""Regression tests for the batch BigQuery destination schema modes.

Metadata columns must be NULLABLE: BigQuery forbids promoting an existing NULLABLE column to
REQUIRED, so a REQUIRED load schema is rejected against tables whose metadata columns are already
NULLABLE (the production state that broke all pipelines). See destination.get_bigquery_schema().
"""

from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.source.config import SourceSyncModes

METADATA_COLUMNS = {
    "_source_record_id",
    "_source_timestamp",
    "_source_data",
    "_bizon_extracted_at",
    "_bizon_loaded_at",
    "_bizon_id",
}


def make_destination() -> BigQueryDestination:
    with (
        patch("bizon.connectors.destinations.bigquery.src.destination.bigquery.Client"),
        patch("bizon.connectors.destinations.bigquery.src.destination.storage.Client"),
    ):
        config = BigQueryConfigDetails(
            project_id="test-project",
            dataset_id="test_dataset",
            gcs_buffer_bucket="test-bucket",
            gcs_buffer_format=GCSBufferFormat.PARQUET,
        )
        sync_metadata = SyncMetadata(
            name="test_pipeline",
            job_id="job_123",
            source_name="test_source",
            stream_name="test_stream",
            destination_name="bigquery",
            destination_alias="bigquery",
            sync_mode=SourceSyncModes.STREAM.value,
        )
        return BigQueryDestination(
            sync_metadata=sync_metadata,
            config=config,
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )


def test_metadata_columns_are_nullable():
    """All metadata columns must be NULLABLE so loads succeed against existing NULLABLE tables."""
    schema = make_destination().get_bigquery_schema()

    found = {field.name for field in schema}
    assert METADATA_COLUMNS.issubset(found), f"Missing metadata columns: {METADATA_COLUMNS - found}"

    for field in schema:
        if field.name in METADATA_COLUMNS:
            assert field.mode == "NULLABLE", f"Column {field.name} must be NULLABLE, got {field.mode}"


def test_load_job_config_allows_field_relaxation():
    """Load jobs must allow relaxing pre-existing REQUIRED columns so old tables self-heal."""
    job_config = make_destination()._build_load_job_config()

    assert bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION in job_config.schema_update_options
    assert bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION in job_config.schema_update_options
