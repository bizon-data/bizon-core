from sqlalchemy.orm import Session

from bizon.backend.client import BackendClient
from bizon.backend.config import SQLAlchemyTypes
from bizon.backend.models import StreamCursor
from bizon.common.models import CursorStatus, JobStatus


def test_backend_client_instance(my_backend_client: BackendClient):
    assert my_backend_client.config.type == SQLAlchemyTypes.SQLITE
    assert my_backend_client.config.database == "NOT_DATABASE_WITH_SQLITE"
    assert my_backend_client.config.schema == "NOT_SCHEMA_IN_SQLITE"
    assert my_backend_client.config.echoEngine == False
    assert my_backend_client.engine.dialect.name == "sqlite"
    assert my_backend_client.config.syncCursorInDBEvery == 2


def test_check_prerequisites(my_backend_client: BackendClient):
    my_backend_client._drop_all_tables()

    all_entities_exist = my_backend_client.check_prerequisites()
    assert all_entities_exist == False


def test_create_all_then_check_prerequisites(my_backend_client: BackendClient):
    my_backend_client._create_all_tables()
    all_entities_exist = my_backend_client.check_prerequisites()
    assert all_entities_exist == True


def test_create_stream_job(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job_id = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    assert new_job_id


def test_update_stream_job_status(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    # If no job is running, get_running_job_id should return None
    job_running_not_found = my_backend_client.get_running_stream_job(
        session=db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert job_running_not_found == None

    my_backend_client.update_stream_job_status(session=db_session, job_id=new_job.id, job_status=JobStatus.RUNNING)

    job_running = my_backend_client.get_running_stream_job(
        session=db_session, source_name="sourcetest", stream_name="streamtest"
    )
    assert new_job.id == job_running.id


def test_stream_job_create_and_get(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    job = my_backend_client.get_stream_job_by_id(session=db_session, job_id=new_job.id)
    assert job.id == new_job.id


def test_create_stream_cursor(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.NOT_STARTED,
    )

    new_cursor_id = my_backend_client.create_stream_cursor(
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


def test_create_failed_cursor(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend_client.create_stream_cursor(
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


def test_update_stream_cursor(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend_client.create_stream_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    my_backend_client.update_stream_cursor_status(
        session=db_session,
        cursor_id=new_cursor_id,
        cursor_status=CursorStatus.SUCCESS,
    )

    cursor = db_session.query(StreamCursor).filter_by(id=new_cursor_id).first()
    assert cursor.status == CursorStatus.SUCCESS


def test_stream_cursor_get_last(my_backend_client: BackendClient, db_session: Session):
    my_backend_client._create_all_tables()

    new_job = my_backend_client.create_stream_job(
        session=db_session,
        source_name="sourcetest",
        stream_name="streamtest",
        total_records_to_fetch=200,
        job_status=JobStatus.RUNNING,
    )

    new_cursor_id = my_backend_client.create_stream_cursor(
        session=db_session,
        job_id=new_job.id,
        source_name="sourcetest",
        stream_name="streamtest",
        iteration=0,
        rows_fetched=0,
        next_pagination=None,
        cursor_status=CursorStatus.NOT_STARTED,
    )

    cursor = my_backend_client.get_last_cursor_by_job_id(job_id=new_job.id, session=db_session)
    assert cursor.id == new_cursor_id
    assert cursor.job_id == new_job.id
