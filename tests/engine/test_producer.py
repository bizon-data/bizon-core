import os

import pytest

from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.producer import Producer
from bizon.engine.runner import RunnerFactory


@pytest.fixture(scope="function")
def my_producer(my_backend: AbstractBackend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline.yml"))
    source = runner.get_source()
    my_backend.create_all_tables()
    source_backend = my_backend
    return Producer(queue=runner.queue, source=source, backend=source_backend)


def test_cursor_recovery_after_iteration(my_producer: Producer, db_session):

    job_id = "vfbkgbjkgbj"

    cursor = my_producer.get_or_create_cursor(job_id=job_id, session=db_session)
    assert cursor is not None

    my_producer.run(job_id=job_id)

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_source_cursor_by_job_id(job_id=job_id, session=db_session)
    assert cursor_from_db is None


def test_cursor_recovery_not_running(my_producer: Producer, db_session):

    job_id = "vfbkgbjkgbj"

    cursor = my_producer.get_or_create_cursor(job_id=job_id, session=db_session)
    assert cursor is not None

    # Here we did not run the job, so the cursor should be None
    cursor_from_db = my_producer.backend.get_last_source_cursor_by_job_id(job_id=job_id, session=db_session)
    assert cursor_from_db is None


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.source_name == "dummy"
