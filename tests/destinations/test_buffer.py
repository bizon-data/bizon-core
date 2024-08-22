from bizon.destinations.buffer import DestinationBuffer
from bizon.source.models import SourceRecord


def test_buffer_destination():
    buffer = DestinationBuffer(buffer_size=2000)
    assert buffer.buffer_size == 2000
    assert buffer.buffer_free_space == 2000
    assert buffer.current_size == 0
    assert buffer._iterations == []


def test_buffer_records():
    buffer = DestinationBuffer(buffer_size=2000)
    assert buffer.current_size == 0

    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)
    assert buffer.current_size == 2
    assert buffer.buffer_free_space == 1998
    assert len(buffer.records) == 2


def test_buffer_flush():
    buffer = DestinationBuffer(buffer_size=2000)
    assert buffer.current_size == 0

    sources_records = [
        SourceRecord(id=1, data={"name": "John", "age": 30}),
        SourceRecord(id=2, data={"name": "Tom", "age": 25}),
    ]
    buffer.add_source_iteration_records_to_buffer(iteration=0, records=sources_records)

    assert buffer.current_size == 2

    buffer.flush()

    assert buffer.current_size == 0
    assert buffer._iterations == []
    assert buffer.buffer_free_space == 2000
    assert len(buffer.records) == 0
