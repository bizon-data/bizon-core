from sqlalchemy.orm import Session

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import CursorStatus, JobStatus, SourceCursor


def test_backend_client_instance(my_backend: SQLAlchemyBackend):
    assert my_backend.config.database == "NOT_DATABASE_WITH_SQLITE"
    assert my_backend.config.schema == "NOT_SCHEMA_IN_SQLITE"
    assert my_backend.config.syncCursorInDBEvery == 2


def test_check_prerequisites(my_backend: SQLAlchemyBackend):
    my_backend.drop_all_tables()

    all_entities_exist = my_backend.check_prerequisites()
    assert all_entities_exist == False


def test_create_all_then_check_prerequisites(my_backend: SQLAlchemyBackend):
    my_backend.create_all_tables()
    all_entities_exist = my_backend.check_prerequisites()
    assert all_entities_exist == True


def test_create_stream_job(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job_id = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    assert new_job_id


def test_update_stream_job_status(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    # If no job is running, get_running_job_id should return None
    job_running_not_found = my_backend.get_running_stream_job(
        session=db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert job_running_not_found == None

    my_backend.update_stream_job_status(session=db_session, job_id=new_job.id, job_status=JobStatus.RUNNING)

    job_running = my_backend.get_running_stream_job(
        session=db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert new_job.id == job_running.id


def test_stream_job_create_and_get(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    job = my_backend.get_stream_job_by_id(session=db_session, job_id=new_job.id)
    assert job.id == new_job.id


def test_create_source_cursor(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    new_cursor_id = my_backend.create_source_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    assert new_cursor_id


def test_create_failed_cursor(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend.create_source_cursor(
        session=db_session,
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


def test_update_source_cursor(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend.create_source_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    my_backend.update_source_cursor_status(
        session=db_session,
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
    )

    cursor = db_session.query(SourceCursor).filter_by(id=new_cursor_id).first()
    assert cursor.status == CursorStatus.WRITTEN_IN_DESTINATION


def test_source_cursor_get_last_after_destination_iteration(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend.create_source_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    cursor = my_backend.get_last_source_cursor_by_job_id(job_id=new_job.id, session=db_session)
    assert cursor is None

    # We recover from the cursor only if data has been written in destination
    my_backend.update_source_cursor_status(
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
        session=db_session,
    )

    cursor = my_backend.get_last_source_cursor_by_job_id(job_id=new_job.id, session=db_session)

    assert cursor.id == new_cursor_id
    assert cursor.job_id == new_job.id


def test_create_destination_cursor(my_backend: SQLAlchemyBackend, db_session: Session):
    my_backend.create_all_tables()

    new_job = my_backend.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_destination_cursor = my_backend.create_destination_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
    )

    assert new_destination_cursor.rows_written == 0


def test_update_destination_cursor(my_backend: SQLAlchemyBackend, db_session: Session):

    my_backend.create_all_tables()

    new_destination_cursor = my_backend.create_destination_cursor(
        session=db_session,
        job_id="crfrifjpduehdpziuh",
        source_name="sourcetest",
        stream_name="streamtest",
        destination_name="destinationtest",
        from_source_iteration=0,
        to_source_iteration=0,
        rows_written=0,
    )

    assert new_destination_cursor.rows_written == 0

    my_backend.update_destination_cursor(
        session=db_session,
        cursor_id=new_destination_cursor.id,
        status=CursorStatus.WRITTEN_IN_DESTINATION,
        rows_written=10,
    )

    cursor = my_backend.get_destination_cursor_by_id(cursor_id=new_destination_cursor.id, session=db_session)
    assert cursor.rows_written == 10
