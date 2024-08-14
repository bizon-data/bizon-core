import pytest

from bizon.api.cursor import Cursor, FetchStatus


@pytest.fixture
def my_cursor():
    return Cursor(
        stream_name="test",
        total_records=100,
    )


def test_cursor_instance(my_cursor: Cursor):
    assert my_cursor.stream_name == "test"
    assert my_cursor.total_records == 100
    assert my_cursor.iteration == 0
    assert my_cursor.fetch_status == FetchStatus.NOT_STARTED
    assert my_cursor.is_finished == False


def test_cursor_percentage(my_cursor: Cursor):
    my_cursor.update_state(
        pagination_dict={"after": "abc"},
        nb_records_fetched=20,
    )
    assert my_cursor.percentage_fetched == 0.2
