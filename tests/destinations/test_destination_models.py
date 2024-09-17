from bizon.destinations.models import DestinationRecord
from bizon.source.models import SourceRecord


def test_destination_record_from_source_record():
    source_record = SourceRecord(
        id="id",
        data={"key": "value"},
    )

    destination_record = DestinationRecord.from_source_record(
        source_record,
        extracted_at="2021-07-01T00:00:00Z",
    )

    assert destination_record.bizon_id
    assert destination_record.bizon_loaded_at
    assert destination_record.source_data == {"key": "value"}
    assert destination_record.source_record_id == source_record.id


def test_destination_record_to_raw_json():
    source_record = SourceRecord(
        id="id",
        data={"key": "value"},
    )

    destination_record = DestinationRecord.from_source_record(
        source_record,
        extracted_at="2021-07-01T00:00:00Z",
    )

    raw_record = destination_record.to_dict_raw_json_data()

    expected_keys = [
        "_source_record_id",
        # "_bizon_extracted_at",
        "_bizon_loaded_at",
        "_source_data",
        "_bizon_id",
    ]

    assert set(raw_record.keys()) == set(expected_keys)
