"""Regression tests: a producer that aborts on an error must not publish a partial extract.

Before this fix the producer sent the SAME clean `QUEUE_TERMINATION` signal whether it
finished the stream or died halfway through it. The consumer therefore treated an aborted
run as the last iteration, which set the stream job to SUCCEEDED and called the
destination's `finalize()` -- for BigQuery, a WRITE_TRUNCATE swap of the temp table into
the production table.

Observed in production on a full_refresh HubSpot stream: the source died on a 401 at
iteration 3517 of 4400, and the run still replaced the production table with the ~80%
extract and recorded the job as `succeeded`. Only the process exit code indicated
anything had gone wrong.
"""

from datetime import datetime
from tempfile import NamedTemporaryFile

import polars as pl
import pytest
from pytz import UTC

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.file.src.config import (
    FileDestinationDetailsConfig,
    FileFormat,
)
from bizon.connectors.destinations.file.src.destination import FileDestination
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import (
    QUEUE_TERMINATION,
    QUEUE_TERMINATION_ERROR,
    QueueMessage,
)
from bizon.monitoring.noop.monitor import NoOpMonitor
from bizon.source.callback import NoOpSourceCallback
from bizon.source.models import source_record_schema
from bizon.transform.transform import Transform

temporary_file = NamedTemporaryFile()


class RecordingConsumer(AbstractQueueConsumer):
    """Minimal concrete consumer -- we only exercise `process_queue_message`."""

    def run(self, stop_event):  # pragma: no cover - not used by these tests
        raise NotImplementedError


@pytest.fixture(scope="function")
def consumer_and_destination(my_sqlite_backend: SQLAlchemyBackend, sqlite_db_session):
    my_sqlite_backend.create_all_tables()

    job = my_sqlite_backend.create_stream_job(
        name="job_test",
        source_name="dummy",
        stream_name="test",
        sync_mode="full_refresh",
        job_status=JobStatus.STARTED,
        session=sqlite_db_session,
    )

    sync_metadata = SyncMetadata(
        job_id=job.id,
        name="job_test",
        source_name="dummy",
        stream_name="test",
        destination_name="file",
        destination_alias="file",
        sync_mode="full_refresh",
    )

    destination = FileDestination(
        sync_metadata=sync_metadata,
        config=FileDestinationDetailsConfig(
            format=FileFormat.JSON,
            destination_id=temporary_file.name,
            # Non-zero buffer, as production defs use. With buffer_size=0 records are
            # flushed on every iteration and the last-iteration path short-circuits on
            # an empty buffer before ever reaching finalize().
            buffer_size=50,
            buffer_flush_timeout=3600,
        ),
        backend=my_sqlite_backend,
        source_callback=NoOpSourceCallback(config={}),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )

    # Record whether the destination was finalized -- that is the operation that
    # publishes the extract (WRITE_TRUNCATE swap on BigQuery).
    finalize_calls = []
    original_finalize = destination.finalize
    destination.finalize = lambda *a, **kw: (finalize_calls.append(True), original_finalize(*a, **kw))[1]

    consumer = RecordingConsumer(
        config=None,
        destination=destination,
        transform=Transform(transforms=[]),
        monitor=NoOpMonitor(sync_metadata=sync_metadata, monitoring_config=None),
    )
    return consumer, destination, finalize_calls, job.id


def _records_message(iteration: int) -> QueueMessage:
    """A normal batch of records, as the producer emits mid-stream."""
    return QueueMessage(
        iteration=iteration,
        df_source_records=pl.DataFrame(
            {
                "id": ["record_1", "record_2"],
                "data": ['{"name": "cookies"}', '{"name": "cream"}'],
                "timestamp": [
                    datetime(2026, 8, 17, 5, 0, tzinfo=UTC),
                    datetime(2026, 8, 17, 5, 1, tzinfo=UTC),
                ],
                "destination_id": [None, None],
            },
            schema=source_record_schema,
        ),
        extracted_at=datetime(2026, 8, 17, 5, 30, tzinfo=UTC),
        pagination={},
        signal=None,
    )


def _termination_message(signal: str, iteration: int) -> QueueMessage:
    """The empty message the producer sends to close the stream."""
    return QueueMessage(
        iteration=iteration,
        df_source_records=pl.DataFrame(
            {"id": [], "data": [], "timestamp": [], "destination_id": []},
            schema=source_record_schema,
        ),
        extracted_at=datetime(2026, 8, 17, 5, 30, tzinfo=UTC),
        pagination={},
        signal=signal,
    )


def test_error_termination_does_not_finalize_or_mark_succeeded(
    consumer_and_destination, my_sqlite_backend, sqlite_db_session
):
    """The production scenario: records land, then the producer dies mid-stream.

    The partial extract must not be published and must not be recorded as succeeded.
    """
    consumer, _destination, finalize_calls, job_id = consumer_and_destination

    # Producer emitted a partial extract before failing.
    assert consumer.process_queue_message(_records_message(iteration=0)) == PipelineReturnStatus.RUNNING

    status = consumer.process_queue_message(_termination_message(QUEUE_TERMINATION_ERROR, iteration=1))

    assert status == PipelineReturnStatus.SOURCE_ERROR, "an aborted producer must not report SUCCESS"
    assert finalize_calls == [], "finalize() must NOT run -- it would publish the partial extract"

    job = my_sqlite_backend.get_stream_job_by_id(job_id=job_id, session=sqlite_db_session)
    assert job.status != JobStatus.SUCCEEDED, "a partial extract must never be recorded as succeeded"


def test_normal_termination_still_finalizes_and_marks_succeeded(
    consumer_and_destination, my_sqlite_backend, sqlite_db_session
):
    """The happy path is unchanged: a clean end-of-stream still publishes."""
    consumer, _destination, finalize_calls, job_id = consumer_and_destination

    assert consumer.process_queue_message(_records_message(iteration=0)) == PipelineReturnStatus.RUNNING

    status = consumer.process_queue_message(_termination_message(QUEUE_TERMINATION, iteration=1))

    assert status == PipelineReturnStatus.SUCCESS
    assert finalize_calls == [True], "a completed stream must still finalize"

    job = my_sqlite_backend.get_stream_job_by_id(job_id=job_id, session=sqlite_db_session)
    assert job.status == JobStatus.SUCCEEDED
