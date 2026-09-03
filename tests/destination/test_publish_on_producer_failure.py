"""A failed producer must not publish.

Before this, the producer terminated the queue the same way whether it had finished or
died. The consumer read that as "last iteration", flushed, marked the job SUCCEEDED and
called finalize() -- which for BigQuery copies the staging table over the destination with
WRITE_TRUNCATE. A source error therefore replaced a good table with a partial one and
reported success, and only the pipeline's own exit code said otherwise.
"""

from datetime import datetime

import polars as pl
import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.logger.src.config import LoggerDestinationConfig
from bizon.connectors.destinations.logger.src.destination import LoggerDestination
from bizon.destination.destination import DestinationBufferStatus
from bizon.destination.models import destination_record_schema
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import (
    QUEUE_TERMINATION,
    QUEUE_TERMINATION_ERROR,
    QueueMessage,
)
from bizon.monitoring.noop.monitor import NoOpMonitor
from bizon.source.callback import NoOpSourceCallback
from bizon.source.models import source_record_schema


@pytest.fixture(scope="function")
def logger_destination(my_sqlite_backend: SQLAlchemyBackend, sqlite_db_session):
    my_sqlite_backend.create_all_tables()

    job = my_sqlite_backend.create_stream_job(
        name="job_test",
        source_name="dummy",
        stream_name="test",
        sync_mode="full_refresh",
        job_status=JobStatus.RUNNING,
        session=sqlite_db_session,
    )

    sync_metadata = SyncMetadata(
        job_id=job.id,
        name="job_test",
        source_name="dummy",
        stream_name="test",
        destination_name="logger",
        destination_alias="logger",
        sync_mode="full_refresh",
    )

    return LoggerDestination(
        sync_metadata=sync_metadata,
        config=LoggerDestinationConfig(dummy="bizon"),
        backend=my_sqlite_backend,
        source_callback=NoOpSourceCallback(config={}),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )


df_destination_records = pl.DataFrame(
    {
        "bizon_id": ["id_1", "id_2"],
        "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
        "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
        "source_record_id": ["record_1", "record_2"],
        "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
        "source_data": ["cookies", "cream"],
    },
    schema=destination_record_schema,
)


def _buffer_then_terminate(destination, session, publish: bool):
    destination.write_or_buffer_records(
        df_destination_records=df_destination_records,
        iteration=0,
        session=session,
    )
    return destination.write_or_buffer_records(
        df_destination_records=pl.DataFrame(schema=destination_record_schema),
        iteration=1,
        last_iteration=True,
        session=session,
        publish=publish,
    )


# ── destination ────────────────────────────────────────────────────────────────


def test_publish_false_skips_finalize_and_leaves_job_running(logger_destination, sqlite_db_session, monkeypatch):
    finalized = []
    monkeypatch.setattr(logger_destination, "finalize", lambda: finalized.append(True))

    status = _buffer_then_terminate(logger_destination, sqlite_db_session, publish=False)

    assert status == DestinationBufferStatus.RECORDS_WRITTEN
    assert finalized == [], "finalize() publishes the staging table; a failed run must not reach it"

    job: StreamJob = logger_destination.backend.get_stream_job_by_id(
        job_id=logger_destination.sync_metadata.job_id, session=sqlite_db_session
    )
    assert job.status == JobStatus.RUNNING, "a failed run must not be recorded as SUCCEEDED"

    # The staged rows are still flushed: they are the resume state for the next attempt.
    assert logger_destination.buffer.df_destination_records.height == 0


def test_publish_true_is_unchanged(logger_destination, sqlite_db_session, monkeypatch):
    finalized = []
    monkeypatch.setattr(logger_destination, "finalize", lambda: finalized.append(True))

    status = _buffer_then_terminate(logger_destination, sqlite_db_session, publish=True)

    assert status == DestinationBufferStatus.RECORDS_WRITTEN
    assert finalized == [True]

    job: StreamJob = logger_destination.backend.get_stream_job_by_id(
        job_id=logger_destination.sync_metadata.job_id, session=sqlite_db_session
    )
    assert job.status == JobStatus.SUCCEEDED


def test_publish_defaults_to_true(logger_destination, sqlite_db_session, monkeypatch):
    """Every existing caller omits `publish`; none of them may stop publishing."""
    finalized = []
    monkeypatch.setattr(logger_destination, "finalize", lambda: finalized.append(True))

    logger_destination.write_or_buffer_records(
        df_destination_records=df_destination_records, iteration=0, session=sqlite_db_session
    )
    logger_destination.write_or_buffer_records(
        df_destination_records=pl.DataFrame(schema=destination_record_schema),
        iteration=1,
        last_iteration=True,
        session=sqlite_db_session,
    )
    assert finalized == [True]


# ── consumer ───────────────────────────────────────────────────────────────────


class _RecordingDestination:
    """Captures how the consumer terminated the sync."""

    def __init__(self):
        self.calls = []

    def write_records_and_update_cursor(self, **kwargs):
        self.calls.append(kwargs)
        return True


def _consumer(destination):
    from bizon.engine.queue.adapters.python_queue.consumer import PythonQueueConsumer
    from bizon.transform.transform import Transform

    sync_metadata = SyncMetadata(
        job_id="1",
        name="job_test",
        source_name="dummy",
        stream_name="test",
        destination_name="logger",
        destination_alias="logger",
        sync_mode="full_refresh",
    )
    return PythonQueueConsumer(
        config=None,
        queue=None,
        destination=destination,
        transform=Transform(transforms=[]),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )


def _termination_message(signal: str) -> QueueMessage:
    return QueueMessage(
        iteration=7,
        df_source_records=pl.DataFrame(schema=source_record_schema),
        extracted_at=datetime(2024, 12, 5, 12, 0),
        pagination={},
        signal=signal,
    )


@pytest.mark.parametrize(
    "signal, expected_status, expected_publish",
    [
        (QUEUE_TERMINATION, PipelineReturnStatus.SUCCESS, True),
        (QUEUE_TERMINATION_ERROR, PipelineReturnStatus.SOURCE_ERROR, False),
    ],
)
def test_consumer_publishes_only_on_a_clean_termination(signal, expected_status, expected_publish):
    destination = _RecordingDestination()
    consumer = _consumer(destination)

    status = consumer.process_queue_message(_termination_message(signal))

    assert status == expected_status
    assert len(destination.calls) == 1
    assert destination.calls[0]["last_iteration"] is True
    assert destination.calls[0]["publish"] is expected_publish
