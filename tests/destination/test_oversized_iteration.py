from datetime import datetime
from typing import List, Tuple

import polars as pl
import pytest

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.logger.src.config import LoggerDestinationConfig
from bizon.connectors.destinations.logger.src.destination import LoggerDestination
from bizon.destination.destination import DestinationBufferStatus, DestinationWriteError
from bizon.destination.models import destination_record_schema
from bizon.engine.backend.models import DestinationCursor, JobStatus
from bizon.monitoring.noop.monitor import NoOpMonitor
from bizon.source.callback import NoOpSourceCallback


class RecordingDestination(LoggerDestination):
    fail_on_write: int = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.writes: List[pl.DataFrame] = []

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        self.writes.append(df_destination_records)
        if len(self.writes) == self.fail_on_write:
            return False, "load job failed"
        return True, None


def _records(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bizon_id": [f"id_{i}" for i in range(n)],
            "bizon_extracted_at": [datetime(2026, 1, 1)] * n,
            "bizon_loaded_at": [datetime(2026, 1, 1)] * n,
            "source_record_id": [f"r_{i}" for i in range(n)],
            "source_timestamp": [datetime(2026, 1, 1)] * n,
            "source_data": ["x" * 200] * n,
        },
        schema=destination_record_schema,
    )


@pytest.fixture
def destination(my_sqlite_backend, sqlite_db_session):
    my_sqlite_backend.create_all_tables()
    job = my_sqlite_backend.create_stream_job(
        name="job_test",
        source_name="dummy",
        stream_name="test",
        sync_mode="incremental",
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
        sync_mode="incremental",
    )
    destination = RecordingDestination(
        sync_metadata=sync_metadata,
        config=LoggerDestinationConfig(dummy="bizon"),
        backend=my_sqlite_backend,
        source_callback=NoOpSourceCallback(config={}),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )
    # Ten small iterations fit, a thousand-record one does not.
    destination.buffer.buffer_size = _records(10).estimated_size(unit="b") * 10
    return destination


def _cursors(destination, session) -> List[DestinationCursor]:
    return (
        session.query(DestinationCursor)
        .filter(DestinationCursor.job_id == destination.sync_metadata.job_id)
        .order_by(DestinationCursor.to_source_iteration)
        .all()
    )


def test_oversized_iteration_is_written_in_buffer_sized_chunks(destination, sqlite_db_session):
    destination.write_or_buffer_records(df_destination_records=_records(10), iteration=0, pagination={"page": 1})

    status = destination.write_or_buffer_records(
        df_destination_records=_records(1000), iteration=1, pagination={"page": 2}, session=sqlite_db_session
    )

    assert status == DestinationBufferStatus.RECORDS_WRITTEN
    assert destination.buffer.is_empty
    # The buffered iteration is flushed on its own first, then the oversized one in chunks.
    assert destination.writes[0].height == 10
    chunks = destination.writes[1:]
    assert len(chunks) > 1
    assert sum(chunk.height for chunk in chunks) == 1000
    assert all(chunk.estimated_size(unit="b") <= destination.buffer.buffer_size for chunk in chunks)

    cursors = _cursors(destination, sqlite_db_session)
    assert [(c.from_source_iteration, c.to_source_iteration, c.rows_written) for c in cursors] == [
        (0, 0, 10),
        (1, 1, 1000),
    ]
    assert all(c.success for c in cursors)


def test_crash_mid_iteration_resumes_from_the_previous_iteration(destination, sqlite_db_session):
    destination.write_or_buffer_records(df_destination_records=_records(10), iteration=0, pagination={"page": 1})
    destination.fail_on_write = 3  # the buffered flush, one chunk, then the failure

    with pytest.raises(DestinationWriteError, match="iteration 1"):
        destination.write_or_buffer_records(
            df_destination_records=_records(1000), iteration=1, pagination={"page": 2}, session=sqlite_db_session
        )

    resume_from = destination.backend.get_last_cursor_by_job_id(job_id=destination.sync_metadata.job_id)
    assert resume_from.to_source_iteration == 0
    assert resume_from.pagination == '{"page": 1}'
