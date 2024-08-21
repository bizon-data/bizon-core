from os import environ

import pytest
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
    BigQueryConfigDetails,
    BigQuerySQLAlchemyConfig,
    PostgresSQLAlchemyConfig,
)
from bizon.engine.backend.config import BackendTypes
from bizon.engine.backend.models import CursorStatus, JobStatus, StreamCursor


@pytest.fixture(scope="function")
def my_bq_backend_config():
    return BigQuerySQLAlchemyConfig(
        type=BackendTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            database=environ["BIGQUERY_PROJECT_ID"],
            schema=environ["BIGQUERY_DATASET_ID"],
            syncCursorInDBEvery=2,
        ),
    )


@pytest.fixture(scope="function")
def my_bq_backend(my_bq_backend_config: PostgresSQLAlchemyConfig) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(config=my_bq_backend_config.config, type=my_bq_backend_config.type)


@pytest.fixture(scope="function")
def bq_session(my_bq_backend: SQLAlchemyBackend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""
    session_ = scoped_session(sessionmaker(bind=my_bq_backend.engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()


def test_backend_client_instance(my_bq_backend: SQLAlchemyBackend):
    assert my_bq_backend.config.schema == "bizon_test"
    assert my_bq_backend.config.syncCursorInDBEvery == 2


@pytest.mark.skip(reason="Super slow test")
def test_check_prerequisites(my_bq_backend: SQLAlchemyBackend):
    my_bq_backend.drop_all_tables()

    all_entities_exist = my_bq_backend.check_prerequisites()
    assert all_entities_exist == False


@pytest.mark.skip(reason="Super slow test")
def test_create_all_then_check_prerequisites(my_bq_backend: SQLAlchemyBackend):
    my_bq_backend.create_all_tables()
    all_entities_exist = my_bq_backend.check_prerequisites()
    assert all_entities_exist == True


@pytest.mark.skip(reason="Super slow test")
def test_create_stream_job(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job_id = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    assert new_job_id


@pytest.mark.skip(reason="Super slow test")
def test_update_stream_job_status(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    # If no job is running, get_running_job_id should return None
    job_running_not_found = my_bq_backend.get_running_stream_job(
        session=bq_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert job_running_not_found == None

    my_bq_backend.update_stream_job_status(session=bq_session, job_id=new_job.id, job_status=JobStatus.RUNNING)

    job_running = my_bq_backend.get_running_stream_job(
        session=bq_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert new_job.id == job_running.id


@pytest.mark.skip(reason="Super slow test")
def test_stream_job_create_and_get(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    job = my_bq_backend.get_stream_job_by_id(session=bq_session, job_id=new_job.id)
    assert job.id == new_job.id


@pytest.mark.skip(reason="Super slow test")
def test_create_stream_cursor(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    new_cursor_id = my_bq_backend.create_stream_cursor(
        session=bq_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    assert new_cursor_id


@pytest.mark.skip(reason="Super slow test")
def test_create_failed_cursor(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_bq_backend.create_stream_cursor(
        session=bq_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.FAILED,
        error_message="This is a test error message",
    )

    assert new_cursor_id


@pytest.mark.skip(reason="Super slow test")
def test_update_stream_cursor(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_bq_backend.create_stream_cursor(
        session=bq_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    my_bq_backend.update_stream_cursor_status(
        session=bq_session,
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
    )

    cursor = bq_session.query(StreamCursor).filter_by(id=new_cursor_id).first()
    assert cursor.status == CursorStatus.WRITTEN_IN_DESTINATION


@pytest.mark.skip(reason="Super slow test")
def test_stream_cursor_get_last_after_destination_iteration(my_bq_backend: SQLAlchemyBackend, bq_session: Session):
    my_bq_backend.create_all_tables()

    new_job = my_bq_backend.create_stream_job(
        session=bq_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_bq_backend.create_stream_cursor(
        session=bq_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    cursor = my_bq_backend.get_last_cursor_by_job_id(job_id=new_job.id, session=bq_session)
    assert cursor is None

    # We recover from the cursor only if data has been written in destination
    my_bq_backend.update_stream_cursor_status(
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
        session=bq_session,
    )

    cursor = my_bq_backend.get_last_cursor_by_job_id(job_id=new_job.id, session=bq_session)

    assert cursor.id == new_cursor_id
    assert cursor.job_id == new_job.id
