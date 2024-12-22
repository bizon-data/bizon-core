import time
from datetime import datetime

import polars as pl

from bizon.destination.buffer import DestinationBuffer
from bizon.destination.models import destination_record_schema

TEST_BUFFER_SIZE = 1  # Mb
TEST_BUFFER_TIMEOUT = 1


def test_buffer_destination():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)
    assert buffer.buffer_size == TEST_BUFFER_SIZE * 1024 * 1024
    assert buffer._iterations == []


df_destination_records = pl.DataFrame(
    {
        "bizon_id": ["id_1", "id_2"],
        "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
        "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
        "source_record_id": ["record_1", "record_2"],
        "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
        "source_data": ["cookies", "cream"],
    },
    schema=destination_record_schema,
)


def test_buffer_records():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)
    buffer.add_source_iteration_records_to_buffer(iteration=0, df_destination_records=df_destination_records)
    assert buffer.df_destination_records.equals(df_destination_records)
    assert buffer._iterations == [0]
    assert buffer.from_iteration == 0 and buffer.to_iteration == 0


def test_buffer_flush():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=60)

    buffer.add_source_iteration_records_to_buffer(iteration=0, df_destination_records=df_destination_records)

    assert buffer.df_destination_records.equals(df_destination_records)

    buffer.flush()

    assert buffer.df_destination_records.height == 0


def test_buffer_iterations():
    buffer = DestinationBuffer(buffer_size=TEST_BUFFER_SIZE, buffer_flush_timeout=TEST_BUFFER_TIMEOUT)
    buffer.add_source_iteration_records_to_buffer(iteration=0, df_destination_records=df_destination_records)
    buffer.add_source_iteration_records_to_buffer(iteration=1, df_destination_records=df_destination_records)

    assert len(buffer._iterations) == 2

    buffer.add_source_iteration_records_to_buffer(iteration=2, df_destination_records=df_destination_records)

    assert len(buffer._iterations) == 3
    assert buffer.from_iteration == 0 and buffer.to_iteration == 2
    buffer.flush()


def test_buffer_ripeness():
    buffer = DestinationBuffer(buffer_size=2000, buffer_flush_timeout=TEST_BUFFER_TIMEOUT)
    buffer.add_source_iteration_records_to_buffer(iteration=0, df_destination_records=df_destination_records)
    time.sleep(1)
    buffer.add_source_iteration_records_to_buffer(iteration=1, df_destination_records=df_destination_records)
    assert buffer.ripeness <= TEST_BUFFER_TIMEOUT
    assert buffer.is_ripe
