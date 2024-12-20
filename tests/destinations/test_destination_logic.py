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


def test_logger_destination(logger_destination: LoggerDestination):
    assert logger_destination.buffer.df_destination_records.height == 0


def test_buffer_records(logger_destination: LoggerDestination):
    logger_destination.buffer.add_source_iteration_records_to_buffer(
        iteration=0, df_destination_records=df_destination_records
    )
    assert logger_destination.buffer.df_destination_records.equals(df_destination_records)


def test_write_or_buffer_records(logger_destination: LoggerDestination):

    df_big_size = pl.DataFrame(schema=destination_record_schema)

    for _ in range(100):
        df_big_size = df_big_size.vstack(df_destination_records, in_place=True)

    # Overriding buffer size to bytes instead of mbs
    logger_destination.buffer.buffer_size = df_big_size.estimated_size(unit="b")

    buffer_status = logger_destination.write_or_buffer_records(
        df_destination_records=df_destination_records,
        iteration=0,
    )

    assert buffer_status == DestinationBufferStatus.RECORDS_BUFFERED

    # Reset buffer
    logger_destination.buffer.buffer_size = df_big_size.estimated_size(unit="b")

    # Write twice
    buffer_status = logger_destination.write_or_buffer_records(
        df_destination_records=df_big_size.vstack(df_destination_records), iteration=1
    )

    assert buffer_status == DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED


def test_write_last_iteration(logger_destination: LoggerDestination, sqlite_db_session):

    buffer_status = logger_destination.write_or_buffer_records(
        df_destination_records=df_destination_records,
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
        df_destination_records=pl.DataFrame(schema=destination_record_schema),
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
        df_destination_records=pl.DataFrame(schema=destination_record_schema),
        iteration=0,
        session=sqlite_db_session,
    )

    assert buffer_status == DestinationBufferStatus.NO_RECORDS
