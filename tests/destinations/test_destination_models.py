from bizon.destinations.models import DestinationRecord
from bizon.source.models import SourceRecord


def test_destination_record_from_source_record():
    source_record = SourceRecord(
        id="id",
        data={"key": "value"},
    )

    destination_record = DestinationRecord.from_source_record(source_record)

    assert destination_record.bizon_id
    assert destination_record.bizon_loaded_at
    assert destination_record.source_data == '{"key": "value"}'
    assert destination_record.source_record_id == source_record.id
