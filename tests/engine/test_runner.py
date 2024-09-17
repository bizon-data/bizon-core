import os

import pytest

from bizon.cli.parser import parse_from_yaml
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.producer import Producer
from bizon.engine.runner import RunnerFactory
from bizon.engine.runners.thread import ThreadRunner


def test_load_from_config():
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    assert runner.is_running is False


@pytest.fixture(scope="function")
def my_producer(my_backend: AbstractBackend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    source = runner.get_source()
    my_backend.create_all_tables()
    source_backend = my_backend
    return Producer(queue=runner.queue, source=source, backend=source_backend)


@pytest.fixture(scope="function")
def my_runner(my_producer: Producer) -> ThreadRunner:
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    runner.backend = my_producer.backend
    runner.backend.create_all_tables()
    return runner


def test_run(my_runner: ThreadRunner):
    assert my_runner.is_running is False


def test_get_source(my_runner: ThreadRunner):
    source = my_runner.get_source()
    assert source.config.source_name == "dummy"


def test_get_backend(my_runner: ThreadRunner):
    backend: SQLAlchemyBackend = my_runner.get_backend()
    assert backend.type == "sqlite_in_memory"


def test_create_job(my_runner: ThreadRunner, db_session):
    job = my_runner.get_or_create_job(session=db_session)
    assert job is not None

    my_job = my_runner.backend.get_stream_job_by_id(job_id=job.id, session=db_session)
    assert my_job.id == job.id


def test_create_job_and_recover(my_runner: ThreadRunner, db_session):
    job = my_runner.get_or_create_job(session=db_session)
    assert job is not None

    my_runner.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING, session=db_session)

    recover_job = my_runner.get_or_create_job(session=db_session)
    assert recover_job.id == job.id


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.source_name == "dummy"
