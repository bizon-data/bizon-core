import os
from datetime import datetime
from queue import Queue
from threading import Event
from unittest.mock import MagicMock

import pytest
from pytz import UTC

from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.producer import Producer
from bizon.source.config import SourceSyncModes
from bizon.source.models import SourceIncrementalState, SourceIteration


@pytest.fixture(scope="function")
def incremental_producer(my_sqlite_backend: AbstractBackend):
    """Create a producer with incremental sync mode."""
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    # Override sync_mode to INCREMENTAL
    runner.bizon_config.source.sync_mode = SourceSyncModes.INCREMENTAL
    runner.bizon_config.source.cursor_field = "updated_at"

    source = runner.get_source(bizon_config=runner.bizon_config, config=runner.config)
    my_sqlite_backend.create_all_tables()
    queue = runner.get_queue(bizon_config=runner.bizon_config, queue=Queue())
    return Producer(bizon_config=runner.bizon_config, queue=queue, source=source, backend=my_sqlite_backend)


@pytest.fixture(scope="function")
def previous_successful_job(incremental_producer: Producer, sqlite_db_session) -> StreamJob:
    """Create a previous successful job for incremental testing."""
    job = incremental_producer.backend.create_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
        sync_mode=SourceSyncModes.INCREMENTAL.value,
        job_status=JobStatus.SUCCEEDED,
        session=sqlite_db_session,
    )
    return job


def test_incremental_sync_mode_detection(incremental_producer: Producer):
    """Test that the producer correctly detects incremental sync mode."""
    assert incremental_producer.bizon_config.source.sync_mode == SourceSyncModes.INCREMENTAL


def test_incremental_cursor_field_set(incremental_producer: Producer):
    """Test that cursor_field is correctly set in bizon config."""
    # cursor_field is set in bizon_config.source, not the source's own config
    assert incremental_producer.bizon_config.source.cursor_field == "updated_at"


def test_incremental_first_run_fallback(incremental_producer: Producer, sqlite_db_session):
    """Test that first incremental run falls back to full refresh behavior when no previous job exists."""
    # Create a new job (not a previous successful one)
    job = incremental_producer.backend.create_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
        sync_mode=SourceSyncModes.INCREMENTAL.value,
        job_status=JobStatus.STARTED,
        session=sqlite_db_session,
    )

    # Verify no previous successful job exists
    last_successful = incremental_producer.backend.get_last_successful_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
    )
    assert last_successful is None


def test_incremental_with_previous_job(incremental_producer: Producer, previous_successful_job: StreamJob):
    """Test that incremental mode finds the previous successful job."""
    last_successful = incremental_producer.backend.get_last_successful_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
    )

    assert last_successful is not None
    assert last_successful.id == previous_successful_job.id
    assert last_successful.status == JobStatus.SUCCEEDED


def test_source_incremental_state_creation(incremental_producer: Producer, previous_successful_job: StreamJob):
    """Test that SourceIncrementalState is created correctly."""
    last_successful = incremental_producer.backend.get_last_successful_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
    )

    # Create the incremental state as the producer would (using bizon_config.source.cursor_field)
    source_incremental_state = SourceIncrementalState(
        last_run=last_successful.created_at,
        state={},
        cursor_field=incremental_producer.bizon_config.source.cursor_field,
    )

    assert source_incremental_state.last_run == last_successful.created_at
    assert source_incremental_state.state == {}
    assert source_incremental_state.cursor_field == "updated_at"


def test_source_incremental_state_model():
    """Test SourceIncrementalState model validation."""
    now = datetime.now(tz=UTC)

    state = SourceIncrementalState(
        last_run=now,
        state={"custom_key": "custom_value"},
        cursor_field="modified_at",
    )

    assert state.last_run == now
    assert state.state == {"custom_key": "custom_value"}
    assert state.cursor_field == "modified_at"


def test_source_incremental_state_default_values():
    """Test SourceIncrementalState default values."""
    now = datetime.now(tz=UTC)

    state = SourceIncrementalState(last_run=now)

    assert state.last_run == now
    assert state.state == {}
    assert state.cursor_field is None


@pytest.fixture(scope="function")
def started_job(incremental_producer: Producer, sqlite_db_session) -> StreamJob:
    """Create the job the producer under test is running."""
    return incremental_producer.backend.create_stream_job(
        name=incremental_producer.bizon_config.name,
        source_name=incremental_producer.source.config.name,
        stream_name=incremental_producer.source.config.stream,
        sync_mode=SourceSyncModes.INCREMENTAL.value,
        job_status=JobStatus.STARTED,
        session=sqlite_db_session,
    )


def _stub_source_fetches(producer: Producer):
    """Stub both fetch methods with a single terminal iteration, so run() exits after one loop."""
    terminal = SourceIteration(records=[], next_pagination={})
    producer.source.get = MagicMock(return_value=terminal)
    producer.source.get_records_after = MagicMock(return_value=terminal)


def test_incremental_uses_watermark_without_reset(
    incremental_producer: Producer, previous_successful_job: StreamJob, started_job: StreamJob
):
    """Baseline: with a previous successful job and no reset, the producer fetches incrementally."""
    _stub_source_fetches(incremental_producer)

    incremental_producer.run(job_id=started_job.id, stop_event=Event())

    incremental_producer.source.get_records_after.assert_called_once()
    incremental_producer.source.get.assert_not_called()


def test_reset_ignores_the_watermark(
    incremental_producer: Producer, previous_successful_job: StreamJob, started_job: StreamJob
):
    """A reset re-fetches the whole stream, even though a watermark is available."""
    incremental_producer.bizon_config.source.reset = True
    _stub_source_fetches(incremental_producer)

    incremental_producer.run(job_id=started_job.id, stop_event=Event())

    incremental_producer.source.get.assert_called_once()
    incremental_producer.source.get_records_after.assert_not_called()
