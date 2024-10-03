import os
from queue import Queue

import pytest

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.producer import Producer


@pytest.fixture(scope="function")
def my_producer(my_backend: AbstractBackend, db_session: SQLAlchemyBackend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    source = runner.get_source(bizon_config=runner.bizon_config, config=runner.config)
    my_backend.create_all_tables()
    queue = runner.get_queue(bizon_config=runner.bizon_config, queue=Queue)
    return Producer(queue=queue, source=source, backend=my_backend)


@pytest.fixture(scope="function")
def my_job(my_producer: Producer, db_session):
    job = my_producer.backend.create_stream_job(
        source_name=my_producer.source.config.source_name,
        stream_name=my_producer.source.config.stream_name,
        job_status=JobStatus.STARTED,
        session=db_session,
    )
    return job


def test_cursor_recovery_after_iteration(my_producer: Producer, db_session: SQLAlchemyBackend, my_job: StreamJob):
    cursor = my_producer.get_or_create_cursor(job_id=my_job.id, session=db_session)
    assert cursor is not None

    my_producer.run(job_id=my_job.id)

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=my_job.id, session=db_session)
    assert cursor_from_db is None


def test_cursor_recovery_not_running(my_producer: Producer, db_session: SQLAlchemyBackend, my_job: StreamJob):

    cursor = my_producer.get_or_create_cursor(job_id=my_job.id, session=db_session)
    assert cursor is not None

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=my_job.id, session=db_session)
    assert cursor_from_db is None


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.source_name == "dummy"
