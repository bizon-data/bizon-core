import os

import pytest

from bizon.common.models import JobStatus
from bizon.engine.backend.backend import Backend
from bizon.engine.producer import Producer
from bizon.engine.runner import RunnerFactory


@pytest.fixture(scope="function")
def my_producer(my_backend_client: Backend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline.yml"))
    source = runner.get_source()
    my_backend_client._create_all_tables()
    source_backend = my_backend_client
    return Producer(queue=runner.queue, source=source, backend=source_backend)


def test_create_job(my_producer: Producer, db_session):
    job = my_producer.get_or_create_job(session=db_session)
    assert job is not None

    my_job = my_producer.backend.get_stream_job_by_id(job_id=job.id, session=db_session)
    assert my_job.id == job.id


def test_create_job_and_recover(my_producer: Producer, db_session):
    job = my_producer.get_or_create_job(session=db_session)
    assert job is not None

    my_producer.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING, session=db_session)

    recover_job = my_producer.get_or_create_job(session=db_session)
    assert recover_job.id == job.id


def test_cursor_recovery_after_iteration(my_producer: Producer, db_session):
    job = my_producer.get_or_create_job(session=db_session)
    assert job is not None

    cursor = my_producer.get_or_create_cursor(job_id=job.id, session=db_session)
    assert cursor is not None

    my_producer.run()

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=job.id, session=db_session)

    assert cursor_from_db is None


def test_cursor_recovery_not_running(my_producer: Producer, db_session):
    job = my_producer.get_or_create_job(session=db_session)
    assert job is not None

    cursor = my_producer.get_or_create_cursor(job_id=job.id, session=db_session)
    assert cursor is not None

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_cursor_by_job_id(job_id=job.id, session=db_session)
    assert cursor_from_db is None


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.source_name == "dummy"
