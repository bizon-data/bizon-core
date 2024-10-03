import time

from bizon.destinations.buffer import DestinationBuffer
from bizon.source.models import SourceRecord

TEST_BUFFER_SIZE = 1  # Mb
TEST_BUFFER_TIMEOUT = 1


def test_buffer_destination():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)
    assert buffer.buffer_size == TEST_BUFFER_SIZE * 1024 * 1024
    assert buffer._iterations == []


def test_buffer_records():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)
    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    assert buffer.records == sources_records
    assert buffer._iterations == [0]
    assert buffer.from_iteration == 0 and buffer.to_iteration == 0


def test_buffer_flush():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)

    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)

    assert buffer.records == sources_records

    buffer.flush()

    assert buffer.records == []


def test_buffer_iterations():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=TEST_BUFFER_TIMEOUT)
    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    buffer.add_source_iteration_records_to_buffer(iteration=1, records=sources_records)

    assert len(buffer._iterations) == 2

    buffer.add_source_iteration_records_to_buffer(iteration=2, records=sources_records)

    assert len(buffer._iterations) == 3
    assert buffer.from_iteration == 0 and buffer.to_iteration == 2
    buffer.flush()


def test_buffer_ripeness():
    buffer = DestinationBuffer(buffer_size=2000, buffer_flush_timeout=TEST_BUFFER_TIMEOUT)
    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    time.sleep(1)
    buffer.add_source_iteration_records_to_buffer(iteration=1, records=sources_records)
    assert buffer.ripeness <= TEST_BUFFER_TIMEOUT
    assert buffer.is_ripe
