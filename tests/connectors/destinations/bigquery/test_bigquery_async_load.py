"""Tests for the async / batched BigQuery load-job path (config.async_load)."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from pytz import UTC

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfigDetails,
    GCSBufferFormat,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.destination.models import destination_record_schema
from bizon.source.config import SourceSyncModes


@pytest.fixture
def mock_bq_client():
    with patch("bizon.connectors.destinations.bigquery.src.destination.bigquery.Client") as mock:
        yield mock


@pytest.fixture
def mock_gcs_client():
    with patch("bizon.connectors.destinations.bigquery.src.destination.storage.Client") as mock:
        yield mock


def make_config(**overrides) -> BigQueryConfigDetails:
    params = dict(
        project_id="test-project",
        dataset_id="test_dataset",
        gcs_buffer_bucket="test-bucket",
        gcs_buffer_format=GCSBufferFormat.PARQUET,
        async_load=True,
        load_files_per_job=10,
        load_max_in_flight_jobs=3,
    )
    params.update(overrides)
    return BigQueryConfigDetails(**params)


def make_sync_metadata(sync_mode=SourceSyncModes.FULL_REFRESH) -> SyncMetadata:
    return SyncMetadata(
        name="test_pipeline",
        job_id="job_123",
        source_name="test_source",
        stream_name="test_stream",
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=sync_mode.value,
    )


def make_destination(config, sync_mode=SourceSyncModes.FULL_REFRESH) -> BigQueryDestination:
    dest = BigQueryDestination(
        sync_metadata=make_sync_metadata(sync_mode),
        config=config,
        backend=MagicMock(),
        source_callback=MagicMock(),
        monitor=MagicMock(),
    )
    # Make GCS upload a no-op that returns a unique file name per call
    counter = {"n": 0}

    def fake_upload(*args, **kwargs):
        counter["n"] += 1
        return f"file_{counter['n']}.parquet"

    dest.convert_and_upload_to_buffer = MagicMock(side_effect=fake_upload)
    dest.cleanup = MagicMock()
    return dest


def one_record_df() -> pl.DataFrame:
    now = datetime.now(tz=UTC)
    return pl.DataFrame(
        {
            "bizon_id": ["a"],
            "bizon_extracted_at": [now],
            "bizon_loaded_at": [now],
            "source_record_id": ["r1"],
            "source_timestamp": [now],
            "source_data": ['{"k": "v"}'],
        },
        schema=destination_record_schema,
    )


def make_load_job(done=True) -> MagicMock:
    job = MagicMock()
    job.done.return_value = done
    job.result.return_value = MagicMock(state="DONE", output_rows=1)
    job.error_result = None
    return job


def flush(dest: BigQueryDestination, iteration: int):
    """Drive a single buffer flush the way write_or_buffer_records would."""
    dest.buffer.add_source_iteration_records_to_buffer(
        iteration=iteration, df_destination_records=one_record_df(), pagination={"cursor": iteration}
    )
    di = dest.buffer_flush_handler()
    dest.buffer.flush()
    return di


class TestAsyncDisabled:
    def test_sync_path_loads_and_creates_cursor_per_flush(self, mock_bq_client, mock_gcs_client):
        """async_load=False keeps the synchronous one-job-per-flush behavior."""
        dest = make_destination(make_config(async_load=False))
        dest.bq_client.load_table_from_uri.return_value = make_load_job(done=True)

        flush(dest, iteration=0)

        # Synchronous: one load job submitted and blocked on, one cursor created now.
        dest.bq_client.load_table_from_uri.assert_called_once()
        assert dest.backend.create_destination_cursor.call_count == 1


class TestAsyncBatching:
    def test_files_are_batched_into_a_single_load_job(self, mock_bq_client, mock_gcs_client):
        """load_files_per_job files produce exactly one load job over a list of URIs."""
        dest = make_destination(make_config(load_files_per_job=3, load_max_in_flight_jobs=10))
        dest.bq_client.load_table_from_uri.return_value = make_load_job(done=False)

        for i in range(3):
            flush(dest, iteration=i)

        dest.bq_client.load_table_from_uri.assert_called_once()
        uris = dest.bq_client.load_table_from_uri.call_args[0][0]
        assert isinstance(uris, list) and len(uris) == 3

    def test_no_load_job_until_batch_is_full(self, mock_bq_client, mock_gcs_client):
        dest = make_destination(make_config(load_files_per_job=5))
        dest.bq_client.load_table_from_uri.return_value = make_load_job(done=False)

        for i in range(4):  # below the batch threshold
            flush(dest, iteration=i)

        dest.bq_client.load_table_from_uri.assert_not_called()


class TestAsyncCursorTiming:
    def test_cursor_created_only_after_load_lands(self, mock_bq_client, mock_gcs_client):
        """No cursor is created while the load job is still in flight."""
        dest = make_destination(make_config(load_files_per_job=1, load_max_in_flight_jobs=10))
        pending_job = make_load_job(done=False)
        dest.bq_client.load_table_from_uri.return_value = pending_job

        flush(dest, iteration=0)  # submits a job that is not done yet

        assert dest.backend.create_destination_cursor.call_count == 0

        # Job lands; next flush reaps the completed prefix and creates its cursor.
        pending_job.done.return_value = True
        flush(dest, iteration=1)

        # iteration 0's cursor is created once its load landed; success=True.
        assert dest.backend.create_destination_cursor.call_count >= 1
        first_call = dest.backend.create_destination_cursor.call_args_list[0]
        assert first_call.kwargs["success"] is True
        assert first_call.kwargs["from_source_iteration"] == 0
        # Cleanup happens after the load lands, not at upload time.
        assert dest.cleanup.call_count >= 1


class TestAsyncBackpressure:
    def test_blocks_when_max_in_flight_exceeded(self, mock_bq_client, mock_gcs_client):
        """Submitting beyond load_max_in_flight_jobs blocks on the oldest job."""
        dest = make_destination(make_config(load_files_per_job=1, load_max_in_flight_jobs=2))
        jobs = [make_load_job(done=False) for _ in range(3)]
        dest.bq_client.load_table_from_uri.side_effect = jobs

        flush(dest, iteration=0)
        flush(dest, iteration=1)
        # Third submission exceeds max in-flight -> oldest job must be awaited.
        flush(dest, iteration=2)

        jobs[0].result.assert_called()


class TestAsyncFailure:
    def test_failed_load_records_failed_cursor_then_aborts(self, mock_bq_client, mock_gcs_client):
        """A failed load records a success=False cursor for its range, then raises to abort the run.

        Aborting is what keeps the successful cursors a contiguous prefix: recovery resumes from
        the last contiguous success and re-fetches the failed range (at-least-once), never skipping it.
        """
        dest = make_destination(make_config(load_files_per_job=1, load_max_in_flight_jobs=10))
        failing_job = make_load_job(done=False)
        failing_job.result.side_effect = RuntimeError("load boom")
        dest.bq_client.load_table_from_uri.return_value = failing_job

        flush(dest, iteration=0)  # submits the failing job (not reaped yet)
        assert dest.backend.create_destination_cursor.call_count == 0

        failing_job.done.return_value = True
        with pytest.raises(RuntimeError):
            flush(dest, iteration=1)  # reaping the failed job must abort

        failed_cursor = dest.backend.create_destination_cursor.call_args_list[0]
        assert failed_cursor.kwargs["success"] is False
        assert failed_cursor.kwargs["from_source_iteration"] == 0

    def test_failure_never_writes_a_later_success_cursor(self, mock_bq_client, mock_gcs_client):
        """The gap guard: an earlier failed batch must prevent any later batch's success cursor
        (which recovery would otherwise treat as the high-water mark and skip the failed range)."""
        dest = make_destination(make_config(load_files_per_job=1, load_max_in_flight_jobs=10))
        job_fail = make_load_job(done=False)
        job_fail.result.side_effect = RuntimeError("boom")
        job_ok = make_load_job(done=False)
        dest.bq_client.load_table_from_uri.side_effect = [job_fail, job_ok]
        dest.bq_client.copy_table = MagicMock()

        flush(dest, iteration=0)  # batch A -> job_fail (in flight, not reaped)
        flush(dest, iteration=1)  # batch B -> job_ok (in flight, not reaped)

        with pytest.raises(RuntimeError):
            dest.finalize()  # drains A first: A fails -> abort before B is reaped

        # No success cursor exists, so recovery resumes from before A and re-fetches A and B.
        successes = [c for c in dest.backend.create_destination_cursor.call_args_list if c.kwargs["success"]]
        assert successes == []
        # And we never publish partial data to the main table on failure.
        dest.bq_client.copy_table.assert_not_called()


class TestAsyncFinalize:
    def test_finalize_drains_pending_then_copies(self, mock_bq_client, mock_gcs_client):
        """finalize() flushes still-pending files, awaits all loads, then runs the copy job."""
        from google.cloud import bigquery

        dest = make_destination(make_config(load_files_per_job=10))  # batch never fills
        dest.bq_client.load_table_from_uri.return_value = make_load_job(done=True)
        dest.bq_client.copy_table = MagicMock()
        dest.bq_client.get_table = MagicMock()
        dest.bq_client.delete_table = MagicMock()

        flush(dest, iteration=0)
        flush(dest, iteration=1)
        # Nothing submitted yet (below threshold)
        dest.bq_client.load_table_from_uri.assert_not_called()

        assert dest.finalize() is True

        # The two pending files are loaded in a final job, both cursors created.
        dest.bq_client.load_table_from_uri.assert_called_once()
        assert dest.backend.create_destination_cursor.call_count == 2
        # Then the temp->main copy job runs (Improvement #1).
        dest.bq_client.copy_table.assert_called_once()
        _, kwargs = dest.bq_client.copy_table.call_args
        assert kwargs["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
