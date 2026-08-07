import os
from queue import Queue

import pytest

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.engine import RunnerFactory, replace_env_variables_in_config
from bizon.engine.pipeline.producer import Producer
from bizon.engine.runner.adapters.thread import ThreadRunner
from bizon.source.config import SourceSyncModes


def test_load_from_config():
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    assert runner.is_running is False


def test_replace_env_variables_in_config():
    os.environ["BIZON_ENV_SOURCE_URL"] = "https://dummy.com"
    config = {
        "source": {
            "name": "dummy",
            "url": "BIZON_ENV_SOURCE_URL",
        }
    }
    config = replace_env_variables_in_config(config=config)
    assert config["source"]["url"] == "https://dummy.com"


def test_replace_env_variables_in_yaml_config():
    os.environ["BIZON_ENV_POSTGRES_HOST"] = "localhost"
    os.environ["BIZON_ENV_POSTGRES_USERNAME"] = "postgres"
    os.environ["BIZON_ENV_POSTGRES_PASSWORD"] = "bizon"
    os.environ["BIZON_ENV_MAX_ITERATIONS"] = "10"

    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_postgres_env_variables.yml"))

    assert runner.bizon_config.engine.backend.config.host == "localhost"
    assert runner.bizon_config.engine.backend.config.username == "postgres"
    assert runner.bizon_config.engine.backend.config.password == "bizon"
    assert runner.bizon_config.source.max_iterations == 10


@pytest.fixture(scope="function")
def my_producer(my_sqlite_backend: AbstractBackend):
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    source = runner.get_source(bizon_config=runner.bizon_config, config=runner.config)
    my_sqlite_backend.create_all_tables()
    source_backend = my_sqlite_backend
    queue = runner.get_queue(bizon_config=runner.bizon_config, queue=Queue())
    return Producer(bizon_config=runner.bizon_config, queue=queue, source=source, backend=source_backend)


@pytest.fixture(scope="function")
def my_runner(my_sqlite_backend) -> ThreadRunner:
    runner = RunnerFactory.create_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    runner.backend = my_sqlite_backend
    runner.backend.create_all_tables()
    return runner


def test_run(my_runner: ThreadRunner):
    assert my_runner.is_running is False


def test_get_source(my_runner: ThreadRunner):
    source = my_runner.get_source(bizon_config=my_runner.bizon_config, config=my_runner.config)
    assert source.config.name == "dummy"


def test_get_backend(my_runner: ThreadRunner):
    backend: SQLAlchemyBackend = my_runner.get_backend(bizon_config=my_runner.bizon_config)
    assert backend.type == "sqlite_in_memory"


def test_create_job(my_runner: ThreadRunner, sqlite_db_session):
    job = my_runner.get_or_create_job(
        bizon_config=my_runner.bizon_config,
        backend=my_runner.backend,
        source=my_runner.get_source(bizon_config=my_runner.bizon_config, config=my_runner.config),
        session=sqlite_db_session,
    )
    assert job is not None

    my_job = my_runner.backend.get_stream_job_by_id(job_id=job.id, session=sqlite_db_session)
    assert my_job.id == job.id


def test_incremental_job_is_recovered(my_runner: ThreadRunner, sqlite_db_session):
    """An incremental run resumes a `running` job: it appends, so the work already done still counts."""
    my_runner.bizon_config.source.sync_mode = SourceSyncModes.INCREMENTAL
    source = my_runner.get_source(bizon_config=my_runner.bizon_config, config=my_runner.config)

    job = my_runner.get_or_create_job(
        bizon_config=my_runner.bizon_config, backend=my_runner.backend, source=source, session=sqlite_db_session
    )
    assert job is not None

    my_runner.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING, session=sqlite_db_session)

    recover_job = my_runner.get_or_create_job(
        bizon_config=my_runner.bizon_config, backend=my_runner.backend, source=source, session=sqlite_db_session
    )
    assert recover_job.id == job.id


def test_full_refresh_job_is_not_recovered(my_runner: ThreadRunner, sqlite_db_session):
    """A full refresh must start fresh.

    A job left in `running` by a killed process has no resumable value: resuming it continues a stale
    item list, so it never reaches finalize() and the table is never republished - while the run keeps
    the job `running`, making the next run resume it too. Regression test for a table that served
    week-old data through seven consecutive "successful" runs.
    """
    assert my_runner.bizon_config.source.sync_mode == SourceSyncModes.FULL_REFRESH
    source = my_runner.get_source(bizon_config=my_runner.bizon_config, config=my_runner.config)

    job = my_runner.get_or_create_job(
        bizon_config=my_runner.bizon_config, backend=my_runner.backend, source=source, session=sqlite_db_session
    )
    my_runner.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING, session=sqlite_db_session)

    new_job = my_runner.get_or_create_job(
        bizon_config=my_runner.bizon_config, backend=my_runner.backend, source=source, session=sqlite_db_session
    )

    assert new_job.id != job.id, "a killed full-refresh job must not be resumed"

    # The stale job is retired, so it can never be picked up as `running` again.
    stale_job = my_runner.backend.get_stream_job_by_id(job_id=job.id, session=sqlite_db_session)
    assert stale_job.status == JobStatus.CANCELED


def test_source_thread(my_producer: Producer):
    assert my_producer.source.config.name == "dummy"
