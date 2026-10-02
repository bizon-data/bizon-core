from datetime import datetime
from decimal import Decimal
from queue import Queue

import orjson
import pytest
from pytz import UTC

from bizon.engine.queue.adapters.python_queue.config import PythonQueueConfigDetails
from bizon.engine.queue.adapters.python_queue.queue import PythonQueue
from bizon.engine.runner.adapters.streaming import StreamingRunner
from bizon.source.models import SourceIteration, SourceRecord, source_record_schema

TS = datetime(2024, 12, 5, 11, 30, tzinfo=UTC)


def put_and_get(records):
    stdlib_queue = Queue()
    queue = PythonQueue(config=PythonQueueConfigDetails(), queue=stdlib_queue)
    queue.put(source_iteration=SourceIteration(next_pagination={"page": 2}, records=records), iteration=3)
    return stdlib_queue.get_nowait()


@pytest.mark.parametrize("convert", ["queue", "streaming_runner"])
def test_data_round_trips_as_json(convert):
    data = {"nested": {"list": [1, 2.5, None, True]}, "unicode": "Zoë ✓", 1: "non-string key"}
    records = [SourceRecord(id="r1", data=data, timestamp=TS, destination_id="d")]
    if convert == "queue":
        df = put_and_get(records).df_source_records
    else:
        df = StreamingRunner.convert_source_records(records)
    assert orjson.loads(df["data"][0]) == {
        "nested": {"list": [1, 2.5, None, True]},
        "unicode": "Zoë ✓",
        "1": "non-string key",
    }
    assert df.schema == source_record_schema
    assert df["id"].to_list() == ["r1"]
    assert df["timestamp"].to_list() == [TS]
    assert df["destination_id"].to_list() == ["d"]


@pytest.mark.parametrize("convert", ["queue", "streaming_runner"])
def test_datetime_values_are_serialized_as_iso8601(convert):
    records = [SourceRecord(id="r1", data={"at": TS}, timestamp=TS)]
    if convert == "queue":
        df = put_and_get(records).df_source_records
    else:
        df = StreamingRunner.convert_source_records(records)
    assert orjson.loads(df["data"][0]) == {"at": "2024-12-05T11:30:00+00:00"}


@pytest.mark.parametrize("convert", ["queue", "streaming_runner"])
def test_decimal_values_are_serialized_as_exact_numbers(convert):
    records = [
        SourceRecord(id="r1", data={"amt": Decimal("12.30"), "big": Decimal("123456789012345678901.5")}, timestamp=TS)
    ]
    if convert == "queue":
        df = put_and_get(records).df_source_records
    else:
        df = StreamingRunner.convert_source_records(records)
    assert df["data"][0] == '{"amt":12.30,"big":123456789012345678901.5}'


@pytest.mark.parametrize("convert", ["queue", "streaming_runner"])
def test_bytes_values_are_decoded_as_utf8(convert):
    records = [SourceRecord(id="r1", data={"v": b"\t", "b": bytearray(b"ok")}, timestamp=TS)]
    if convert == "queue":
        df = put_and_get(records).df_source_records
    else:
        df = StreamingRunner.convert_source_records(records)
    assert orjson.loads(df["data"][0]) == {"v": "\t", "b": "ok"}


def test_queue_message_carries_iteration_and_pagination():
    message = put_and_get([SourceRecord(id="r1", data={}, timestamp=TS)])
    assert message.iteration == 3
    assert message.pagination == {"page": 2}
    assert message.df_source_records.height == 1


def test_empty_iteration_keeps_schema():
    df = put_and_get([]).df_source_records
    assert df.height == 0
    assert df.schema == source_record_schema
