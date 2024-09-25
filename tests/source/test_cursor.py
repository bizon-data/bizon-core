import pytest

from bizon.engine.backend.models import JobStatus
from bizon.source.cursor import Cursor


@pytest.fixture
def my_cursor():
    return Cursor(
        source_name="dummy",
        stream_name="test",
        job_id="jfd87djkj",
        total_records=100,
    )


def test_cursor_instance(my_cursor: Cursor):
    assert my_cursor.source_name == "dummy"
    assert my_cursor.stream_name == "test"
    assert my_cursor.source_full_name == "dummy.test"
    assert my_cursor.total_records == 100
    assert my_cursor.iteration == 0
    assert my_cursor.job_status == JobStatus.STARTED
    assert my_cursor.is_finished == False


def test_cursor_percentage(my_cursor: Cursor):
    my_cursor.update_state(
        pagination_dict={"after": "abc"},
        nb_records_fetched=20,
    )
    assert my_cursor.percentage_fetched == 0.2


def test_update_state_pagination_empty_after_first_iteration(my_cursor: Cursor):
    my_cursor.update_state(
        pagination_dict={},
        nb_records_fetched=20,
    )
    assert my_cursor.job_status == JobStatus.SUCCEEDED
    assert my_cursor.is_finished == True


def test_update_state_pagination_empty_after_two_iterations(my_cursor: Cursor):
    my_cursor.update_state(
        pagination_dict={"after": "abc"},
        nb_records_fetched=20,
    )

    assert my_cursor.job_status == JobStatus.RUNNING
    assert my_cursor.iteration == 1
    assert my_cursor.is_finished == False

    my_cursor.update_state(
        pagination_dict={},
        nb_records_fetched=20,
    )

    assert my_cursor.job_status == JobStatus.SUCCEEDED
    assert my_cursor.is_finished == True
    assert my_cursor.iteration == 2
