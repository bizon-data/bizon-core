import pytest

from bizon.common.models import SyncMetadata
from bizon.destinations.logger.src.config import LoggerDestinationConfig
from bizon.destinations.logger.src.destination import LoggerDestination
from bizon.destinations.models import DestinationRecord
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus
from bizon.source.models import SourceRecord


@pytest.fixture(scope="function")
def logger_destination(my_backend: SQLAlchemyBackend, db_session):

    my_backend.create_all_tables()

    job = my_backend.create_stream_job(
        source_name="dummy",
        stream_name="test",
        job_status=JobStatus.STARTED,
        session=db_session,
    )

    return LoggerDestination(
        sync_metadata=SyncMetadata(
            job_id=job.id,
            source_name="dummy",
            stream_name="test",
            destination_name="logger",
            sync_mode="full_refresh",
        ),
        config=LoggerDestinationConfig(dummy="bizon"),
        backend=my_backend,
    )


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
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdfj",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "Alice", "age": 35},
            ),
            DestinationRecord(
                bizon_id="fjdj9787j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "Tom", "age": 29},
            ),
        ],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 2
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdf9999j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="9",
                source_data={"name": "Coco", "age": 35},
            )
        ],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 3
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdf9999j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="9",
                source_data={"name": "Coco", "age": 35},
            )
        ],
        iteration=1,
    )
    assert logger_destination.buffer.current_size == 1
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 3


def test_write_last_iteration(logger_destination: LoggerDestination, db_session):

    # Set buffer size to 3
    logger_destination.buffer.buffer_size = 3
    assert logger_destination.buffer.current_size == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdfj",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="fjdj9787j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=1,
    )

    assert logger_destination.buffer.current_size == 2
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 0

    destination_iteration = logger_destination.write_or_buffer_records(
        destination_records=[],
        iteration=1,
        last_iteration=True,
        session=db_session,
    )

    assert logger_destination.buffer.current_size == 0
    assert destination_iteration.success == True
    assert destination_iteration.records_written == 2


def test_from_to_iteration(logger_destination: LoggerDestination, db_session):

    # Set buffer size to 15
    logger_destination.buffer.buffer_size = 15
    assert logger_destination.buffer.current_size == 0

    destination_iteration_0 = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdfj",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="fjdj9787j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=0,
    )

    destination_iteration_1 = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="hhhu",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="iiiio",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=1,
    )

    assert logger_destination.buffer.from_iteration == 0
    assert logger_destination.buffer.to_iteration == 1

    destination_iteration_2 = logger_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="hhhu",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="iiiio",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=2,
    )

    assert logger_destination.buffer.from_iteration == 0
    assert logger_destination.buffer.to_iteration == 2
