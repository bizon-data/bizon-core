import os
import threading
from datetime import datetime
from queue import Queue

import pytest

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.producer import Producer
from bizon.source.models import SourceIteration, SourceRecord


@pytest.fixture(scope="function")
def my_producer(my_sqlite_backend: AbstractBackend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    source = runner.get_source(bizon_config=runner.bizon_config, config=runner.config)
    my_sqlite_backend.create_all_tables()
    queue = runner.get_queue(bizon_config=runner.bizon_config, queue=Queue())
    return Producer(bizon_config=runner.bizon_config, queue=queue, source=source, backend=my_sqlite_backend)


@pytest.fixture(scope="function")
def my_job(my_producer: Producer, sqlite_db_session):
    job = my_producer.backend.create_stream_job(
        name=my_producer.bizon_config.name,
        source_name=my_producer.source.config.name,
        stream_name=my_producer.source.config.stream,
        sync_mode="full_refresh",
        job_status=JobStatus.STARTED,
        session=sqlite_db_session,
    )
    return job


def test_cursor_recovery_after_iteration(my_producer: Producer, sqlite_db_session, my_job: StreamJob):
    cursor = my_producer.get_or_create_cursor(job_id=my_job.id, session=sqlite_db_session)
    assert cursor is not None

    stop_event = threading.Event()
    my_producer.run(job_id=my_job.id, stop_event=stop_event)

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=my_job.id, session=sqlite_db_session)
    assert cursor_from_db is None


def test_cursor_recovery_not_running(my_producer: Producer, sqlite_db_session, my_job: StreamJob):

    cursor = my_producer.get_or_create_cursor(job_id=my_job.id, session=sqlite_db_session)
    assert cursor is not None

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=my_job.id, session=sqlite_db_session)
    assert cursor_from_db is None


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.name == "dummy"


def test_queue_is_full(my_producer: Producer, sqlite_db_session, my_job: StreamJob):

    assert my_producer.queue.config.max_nb_messages == 1_000_000

    my_producer.queue.connect()

    cursor = my_producer.get_or_create_cursor(job_id=my_job.id, session=sqlite_db_session)

    is_queue_full, queue_size, approximate_nb_records_in_queue = my_producer.is_queue_full(cursor)
    assert is_queue_full is False
    assert queue_size == 0
    assert approximate_nb_records_in_queue == 0

    source_iteration = SourceIteration(
        records=[
            SourceRecord(
                id=str(i),
                data={"test": "test"},
                timestamp="2021-01-01T00:00:00",
            )
            for i in range(1000)
        ],
        next_pagination={"test": "test"},
    )

    my_producer.queue.put(
        source_iteration=source_iteration,
        iteration=cursor.iteration,
        extracted_at=datetime.now(),
    )

    cursor.update_state(
        pagination_dict=source_iteration.next_pagination, nb_records_fetched=len(source_iteration.records)
    )

    is_queue_full, queue_size, approximate_nb_records_in_queue = my_producer.is_queue_full(cursor)
    assert is_queue_full is False
    assert queue_size == 1
    assert approximate_nb_records_in_queue == 1000

    for i in range(1000):
        my_producer.queue.put(
            source_iteration=source_iteration,
            iteration=cursor.iteration,
            extracted_at=datetime.now(),
        )
        cursor.update_state(
            pagination_dict=source_iteration.next_pagination, nb_records_fetched=len(source_iteration.records)
        )

    is_queue_full, queue_size, approximate_nb_records_in_queue = my_producer.is_queue_full(cursor)
    assert is_queue_full is True
    assert queue_size == 1001
    assert approximate_nb_records_in_queue == 1_001_000
