import os

import pytest
from sqlalchemy.orm import Session

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import CursorStatus, JobStatus, SourceCursor


@pytest.mark.parametrize("backend", [pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("my_sqlite_backend")])
def test_backend_client_instance(backend: SQLAlchemyBackend):
    assert backend.config.syncCursorInDBEvery == 2


@pytest.mark.parametrize("backend", [pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("my_sqlite_backend")])
def test_create_all_then_check_prerequisites(backend: SQLAlchemyBackend):
    backend.create_all_tables()
    all_entities_exist = backend.check_prerequisites()
    assert all_entities_exist == True


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_create_stream_job(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job_id = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    assert new_job_id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_update_stream_job_status(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="cookie",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    # If no job is running, get_running_job_id should return None
    job_running_not_found = backend.get_running_stream_job(
        session=session, name="testjob", source_name="sourcetest", stream_name="cookie"
    )
    assert job_running_not_found == None

    backend.update_stream_job_status(session=session, job_id=new_job.id, job_status=JobStatus.RUNNING)

    job_running = backend.get_running_stream_job(
        session=session, name="testjob", source_name="sourcetest", stream_name="cookie"
    )
    assert new_job.id == job_running.id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_stream_job_create_and_get(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    job = backend.get_stream_job_by_id(session=session, job_id=new_job.id)
    assert job.id == new_job.id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_create_source_cursor(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.STARTED,
    )

    new_cursor_id = backend.create_source_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.STARTED,
    )

    assert new_cursor_id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_create_failed_cursor(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = backend.create_source_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.FAILED,
        error_message="This is a test error message",
    )

    assert new_cursor_id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_update_source_cursor(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = backend.create_source_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.STARTED,
    )

    backend.update_source_cursor_status(
        session=session,
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
    )

    cursor = session.query(SourceCursor).filter_by(id=new_cursor_id).first()
    assert cursor.status == CursorStatus.WRITTEN_IN_DESTINATION


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_get_last_iteration_cursor(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    cursor = backend.get_last_cursor_by_job_id(job_id=new_job.id, session=session)
    assert cursor is None

    backend.create_destination_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
        success=True,
    )

    cursor = backend.get_last_cursor_by_job_id(job_id=new_job.id, session=session)

    assert cursor.job_id == new_job.id


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_create_destination_cursor(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()

    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_destination_cursor = backend.create_destination_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
        success=True,
    )

    assert new_destination_cursor.rows_written == 0


@pytest.mark.parametrize(
    "backend,session",
    [
        (pytest.lazy_fixture("my_pg_backend"), pytest.lazy_fixture("pg_db_session")),
        (pytest.lazy_fixture("my_sqlite_backend"), pytest.lazy_fixture("sqlite_db_session")),
    ],
)
def test_number_of_rows_written(backend: SQLAlchemyBackend, session: Session):
    backend.create_all_tables()
    new_job = backend.create_stream_job(
        session=session,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        sync_mode="full_refresh",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    backend.create_destination_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=5,
        success=True,
    )
    backend.create_destination_cursor(
        session=session,
        job_id=new_job.id,
        name="testjob",
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=10,
        success=True,
    )
    assert backend.get_number_of_written_rows_for_job(job_id=new_job.id, session=session) == 15
