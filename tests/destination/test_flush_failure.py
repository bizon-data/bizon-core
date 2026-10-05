from datetime import datetime
from typing import Tuple

import polars as pl
import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.logger.src.config import LoggerDestinationConfig
from bizon.connectors.destinations.logger.src.destination import LoggerDestination
from bizon.destination.destination import DestinationWriteError
from bizon.destination.models import destination_record_schema
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import DestinationCursor, JobStatus, StreamJob
from bizon.monitoring.noop.monitor import NoOpMonitor
from bizon.source.callback import NoOpSourceCallback
from bizon.source.models import SourceRecord


class FailingDestination(LoggerDestination):
    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        return False, "load job failed"


def _make(cls, backend: SQLAlchemyBackend, session):
    backend.create_all_tables()
    job = backend.create_stream_job(
        name="job_test",
        source_name="dummy",
        stream_name="test",
        sync_mode="full_refresh",
        job_status=JobStatus.RUNNING,
        session=session,
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
    return cls(
        sync_metadata=sync_metadata,
        config=LoggerDestinationConfig(dummy="bizon"),
        backend=backend,
        source_callback=NoOpSourceCallback(config={}),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )


@pytest.fixture
def failing_destination(my_sqlite_backend, sqlite_db_session):
    return _make(FailingDestination, my_sqlite_backend, sqlite_db_session)


@pytest.fixture
def logger_destination(my_sqlite_backend, sqlite_db_session):
    return _make(LoggerDestination, my_sqlite_backend, sqlite_db_session)


records = pl.DataFrame(
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
empty = pl.DataFrame(schema=destination_record_schema)


def _job_status(destination, session) -> JobStatus:
    job: StreamJob = destination.backend.get_stream_job_by_id(job_id=destination.sync_metadata.job_id, session=session)
    return job.status


def _cursors(destination, session):
    return session.query(DestinationCursor).filter(DestinationCursor.job_id == destination.sync_metadata.job_id).all()


def test_failed_intermediate_flush_raises(failing_destination, sqlite_db_session):
    failing_destination.buffer.buffer_size = 0

    with pytest.raises(DestinationWriteError, match="load job failed"):
        failing_destination.write_or_buffer_records(df_destination_records=records, iteration=0)

    cursors = _cursors(failing_destination, sqlite_db_session)
    assert [c.success for c in cursors] == [False]
    assert (
        failing_destination.backend.get_last_cursor_by_job_id(job_id=failing_destination.sync_metadata.job_id) is None
    )


def test_failed_partial_flush_raises(failing_destination):
    failing_destination.write_or_buffer_records(df_destination_records=records, iteration=0)
    failing_destination.buffer.buffer_size = failing_destination.buffer.current_size + 1

    with pytest.raises(DestinationWriteError):
        failing_destination.write_or_buffer_records(df_destination_records=records, iteration=1)


def test_failed_last_flush_does_not_publish(failing_destination, sqlite_db_session, monkeypatch):
    finalized = []
    monkeypatch.setattr(failing_destination, "finalize", lambda: finalized.append(True))

    failing_destination.write_or_buffer_records(df_destination_records=records, iteration=0)
    with pytest.raises(DestinationWriteError):
        failing_destination.write_or_buffer_records(
            df_destination_records=empty, iteration=1, last_iteration=True, session=sqlite_db_session
        )

    assert finalized == []
    assert _job_status(failing_destination, sqlite_db_session) == JobStatus.RUNNING


def test_source_record_timestamp_defaults_to_creation_time():
    before = datetime.now().astimezone()
    record = SourceRecord(id="1", data={})
    assert record.timestamp >= before
