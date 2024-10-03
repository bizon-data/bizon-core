import os

import pytest
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
    PostgresConfigDetails,
    PostgresSQLAlchemyConfig,
)
from bizon.engine.backend.config import BackendTypes
from bizon.engine.backend.models import CursorStatus, JobStatus, SourceCursor


@pytest.fixture(scope="function")
def my_pg_backend_config():
    return PostgresSQLAlchemyConfig(
        type=BackendTypes.POSTGRES,
        config=PostgresConfigDetails(
            database="bizon_test",
            schema="public",
            syncCursorInDBEvery=2,
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=5432,
            username="postgres",
            password="bizon",
        ),
    )


@pytest.fixture(scope="function")
def my_pg_backend(my_pg_backend_config: PostgresSQLAlchemyConfig) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(config=my_pg_backend_config.config, type=my_pg_backend_config.type)


@pytest.fixture(scope="function")
def pg_db_session(my_pg_backend: SQLAlchemyBackend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""

    engine = my_pg_backend.get_engine()

    session_ = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()


def test_backend_client_instance(my_pg_backend: SQLAlchemyBackend):
    assert my_pg_backend.config.database == "bizon_test"
    assert my_pg_backend.config.schema_name == "public"
    assert my_pg_backend.config.syncCursorInDBEvery == 2


def test_create_all_then_check_prerequisites(my_pg_backend: SQLAlchemyBackend):
    my_pg_backend.create_all_tables()
    all_entities_exist = my_pg_backend.check_prerequisites()
    assert all_entities_exist == True


def test_create_stream_job(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job_id = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    assert new_job_id


def test_update_stream_job_status(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    # If no job is running, get_running_job_id should return None
    job_running_not_found = my_pg_backend.get_running_stream_job(
        session=pg_db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert job_running_not_found == None

    my_pg_backend.update_stream_job_status(session=pg_db_session, job_id=new_job.id, job_status=JobStatus.RUNNING)

    job_running = my_pg_backend.get_running_stream_job(
        session=pg_db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert new_job.id == job_running.id


def test_stream_job_create_and_get(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    job = my_pg_backend.get_stream_job_by_id(session=pg_db_session, job_id=new_job.id)
    assert job.id == new_job.id


def test_create_source_cursor(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    new_cursor_id = my_pg_backend.create_source_cursor(
        session=pg_db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.STARTED,
    )

    assert new_cursor_id


def test_create_failed_cursor(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_pg_backend.create_source_cursor(
        session=pg_db_session,
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


def test_update_source_cursor(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_pg_backend.create_source_cursor(
        session=pg_db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.STARTED,
    )

    my_pg_backend.update_source_cursor_status(
        session=pg_db_session,
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
    )

    cursor = pg_db_session.query(SourceCursor).filter_by(id=new_cursor_id).first()
    assert cursor.status == CursorStatus.WRITTEN_IN_DESTINATION


def test_get_last_iteration_cursor(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    cursor = my_pg_backend.get_last_cursor_by_job_id(job_id=new_job.id, session=pg_db_session)
    assert cursor is None

    my_pg_backend.create_destination_cursor(
        session=pg_db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
        success=True,
    )

    cursor = my_pg_backend.get_last_cursor_by_job_id(job_id=new_job.id, session=pg_db_session)

    assert cursor.job_id == new_job.id


def test_create_destination_cursor(my_pg_backend: SQLAlchemyBackend, pg_db_session: Session):
    my_pg_backend.create_all_tables()

    new_job = my_pg_backend.create_stream_job(
        session=pg_db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_destination_cursor = my_pg_backend.create_destination_cursor(
        session=pg_db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
        success=True,
    )

    assert new_destination_cursor.rows_written == 0
