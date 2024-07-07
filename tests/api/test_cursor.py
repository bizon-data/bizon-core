import pytest
from pydantic import BaseModel, Field

from bizon.api.cursor import Cursor
from bizon.api.models.state import AbstractState, FetchStatus


class ExamplePagingModel(BaseModel):
    after: str = Field(None, description="Cursor for the next page")
    before: str = Field(None, description="Cursor for the previous page")


class ConcreteState(AbstractState):
    paging: ExamplePagingModel


@pytest.fixture
def my_cursor():
    return Cursor(
        state=ConcreteState(
            stream_name="test",
            fetch_status=FetchStatus.SUCCESS,
            records_fetched=10,
            is_finished=False,
            paging=ExamplePagingModel(after="abc", before="def"),
        ),
        total_records=100,
    )


def test_cursor_instance(my_cursor):
    assert my_cursor.state.stream_name == "test"
    assert my_cursor.state.fetch_status == FetchStatus.SUCCESS
    assert my_cursor.state.records_fetched == 10
    assert my_cursor.is_finished == False


def test_cursor_percentage(my_cursor):
    my_cursor.update_state(
        new_state=ConcreteState(
            stream_name="test",
            fetch_status=FetchStatus.SUCCESS,
            records_fetched=20,
            is_finished=False,
            paging=ExamplePagingModel(after="abc", before="def"),
        )
    )
    assert my_cursor.percentage_fetched == 0.2
