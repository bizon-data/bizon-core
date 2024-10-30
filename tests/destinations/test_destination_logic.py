import sys

import pytest

from bizon.common.models import SyncMetadata
from bizon.destinations.destination import DestinationBufferStatus
from bizon.destinations.logger.src.config import LoggerDestinationConfig
from bizon.destinations.logger.src.destination import LoggerDestination
from bizon.destinations.models import DestinationRecord
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.source.models import SourceRecord


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

    return LoggerDestination(
        sync_metadata=SyncMetadata(
            job_id=job.id,
            name="job_test",
            source_name="dummy",
            stream_name="test",
            destination_name="logger",
            sync_mode="full_refresh",
        ),
        config=LoggerDestinationConfig(dummy="bizon"),
        backend=my_sqlite_backend,
    )


def test_logger_destination(logger_destination: LoggerDestination):
    assert logger_destination.buffer.records == []


def test_buffer_records(logger_destination: LoggerDestination):
    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    logger_destination.buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    assert logger_destination.buffer.records == sources_records


def test_write_or_buffer_records(logger_destination: LoggerDestination):

    TEST_RECORDS = [
        DestinationRecord(
            bizon_id=str(i),
            bizon_extracted_at="2021-07-01T00:00:00Z",
            bizon_loaded_at="2021-07-01T00:00:00Z",
            source_record_id="3",
            source_data={"name": "Alice", "age": 35},
        )
        for i in range(1000)
    ]

    # Overriding buffer size to bytes instead of mbs
    logger_destination.buffer.buffer_size = sys.getsizeof(TEST_RECORDS)

    buffer_status = logger_destination.write_or_buffer_records(
        destination_records=[TEST_RECORDS[0]],
        iteration=0,
    )
    assert buffer_status == DestinationBufferStatus.RECORDS_BUFFERED

    # Reset buffer
    logger_destination.buffer.buffer_size = sys.getsizeof(TEST_RECORDS)

    # Write twice
    buffer_status = logger_destination.write_or_buffer_records(
        destination_records=TEST_RECORDS + TEST_RECORDS, iteration=1
    )

    assert buffer_status == DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED


def test_write_last_iteration(logger_destination: LoggerDestination, sqlite_db_session):

    buffer_status = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdfj",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="fjdj9787j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=0,
        session=sqlite_db_session,
    )

    assert buffer_status == DestinationBufferStatus.RECORDS_BUFFERED

    running_job: StreamJob = logger_destination.backend.get_stream_job_by_id(
        job_id=logger_destination.sync_metadata.job_id, session=sqlite_db_session
    )
    assert running_job.status == JobStatus.RUNNING

    # Write the last iteration
    buffer_status = logger_destination.write_or_buffer_records(
        destination_records=[],
        iteration=1,
        last_iteration=True,
        session=sqlite_db_session,
    )

    assert buffer_status == DestinationBufferStatus.RECORDS_WRITTEN

    finished_job: StreamJob = logger_destination.backend.get_stream_job_by_id(
        job_id=logger_destination.sync_metadata.job_id, session=sqlite_db_session
    )
    assert finished_job.status == JobStatus.SUCCEEDED


def test_no_records_written(logger_destination: LoggerDestination, sqlite_db_session):

    buffer_status = logger_destination.write_or_buffer_records(
        destination_records=[],
        iteration=0,
        session=sqlite_db_session,
    )

    assert buffer_status == DestinationBufferStatus.NO_RECORDS
