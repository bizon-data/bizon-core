import pytest

from bizon.destinations.logger.src.config import LoggerDestinationConfig
from bizon.destinations.logger.src.destination import LoggerDestination
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.source.models import SourceRecord


@pytest.fixture(scope="function")
def logger_destination(my_backend: SQLAlchemyBackend):
    return LoggerDestination(sync_metadata=None, config=LoggerDestinationConfig(dummy="bizon"), backend=my_backend)


def test_logger_destination(logger_destination: LoggerDestination):
    assert logger_destination.buffer.current_size == 0
    assert logger_destination.buffer.buffer_free_space == 2000


def test_buffer_records(logger_destination: LoggerDestination):
    assert logger_destination.buffer.current_size == 0

    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    logger_destination.buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    assert logger_destination.buffer.current_size == 2
    assert logger_destination.buffer.buffer_free_space == 1998


def test_write_or_buffer_records(logger_destination: LoggerDestination):

    # Set buffer size to 3
    logger_destination.buffer.buffer_size = 3
    assert logger_destination.buffer.current_size == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        source_records=[
            SourceRecord(id=1, data={"name": "John", "age": 30}),
            SourceRecord(id=2, data={"name": "Tom", "age": 25}),
        ],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 2
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        source_records=[SourceRecord(id=3, data={"name": "Alice", "age": 35})],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 3
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        source_records=[SourceRecord(id=3, data={"name": "Bob", "age": 35})],
        iteration=1,
    )
    assert logger_destination.buffer.current_size == 1
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 3


def test_write_last_iteration(logger_destination: LoggerDestination):

    # Set buffer size to 3
    logger_destination.buffer.buffer_size = 3
    assert logger_destination.buffer.current_size == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        source_records=[
            SourceRecord(id=1, data={"name": "John", "age": 30}),
            SourceRecord(id=2, data={"name": "Tom", "age": 25}),
        ],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 2
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        source_records=[],
        iteration=1,
        last_iteration=True,
    )

    assert logger_destination.buffer.current_size == 0
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 2
