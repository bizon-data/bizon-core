import json
from datetime import datetime
from typing import Type
from uuid import uuid4

from google.protobuf.message import Message
from pydantic import BaseModel, Field
from pytz import UTC

from bizon.source.models import SourceRecord


class DestinationRecord(BaseModel):
    bizon_id: str = Field(..., description="Bizon unique identifier of the record")
    bizon_extracted_at: datetime = Field(..., description="Datetime when the record was extracted")
    bizon_loaded_at: datetime = Field(..., description="Datetime when the record was loaded")
    source_record_id: str = Field(..., description="Source record id")
    source_timestamp: datetime = Field(..., description="Timestamp of the source record")
    source_data: dict = Field(..., description="Source record JSON as dict")

    @classmethod
    def from_source_record(cls, source_record: SourceRecord, extracted_at: datetime) -> "DestinationRecord":
        return cls(
            bizon_id=uuid4().hex,
            bizon_extracted_at=extracted_at,
            bizon_loaded_at=datetime.now(tz=UTC),
            source_record_id=source_record.id,
            source_timestamp=source_record.timestamp,
            source_data=source_record.data,
        )

    def to_dict_debezium(self, parquet: bool = False) -> dict:
        """Return the record as a dict with Debezium data"""

        # Extract keys from Debezium message key and unnest
        parsed_debezium_keys = json.loads(self.source_data["_bizon_message_key"])

        # Parse Debezium Operation and deleted record
        if self.source_data.get("op") == "d":
            parsed_source_data = {"__deleted": True, **self.source_data["before"]}
        else:
            parsed_source_data = {"__deleted": False, **self.source_data["after"]}

        if parquet:
            return {
                **{k: str(v) for k, v in parsed_debezium_keys.items()},
                "_bizon_id": self.bizon_id,
                "_bizon_extracted_at": int(self.bizon_extracted_at.timestamp() * 1_000_000),
                "_bizon_loaded_at": self.bizon_loaded_at.timestamp(),
                "_source_record_id": self.source_record_id,
                "_source_timestamp": int(self.source_timestamp.timestamp() * 1_000_000),
                "_source_data": json.dumps(parsed_source_data),
            }

        return {
            **{k: str(v) for k, v in parsed_debezium_keys.items()},
            "_bizon_id": self.bizon_id,
            "_bizon_extracted_at": self.bizon_extracted_at,
            "_bizon_loaded_at": self.bizon_loaded_at,
            "_source_record_id": self.source_record_id,
            "_source_timestamp": self.source_timestamp,
            "_source_data": json.dumps(parsed_source_data),
        }

    def to_dict_raw_json_data(self, parquet: bool = False) -> str:
        """Return the record as a dict with raw JSON data"""

        if parquet:
            return {
                "_bizon_id": self.bizon_id,
                "_bizon_extracted_at": int(self.bizon_extracted_at.timestamp() * 1_000_000),
                "_bizon_loaded_at": self.bizon_loaded_at.timestamp(),
                "_source_record_id": self.source_record_id,
                "_source_timestamp": int(self.source_timestamp.timestamp() * 1_000_000),
                "_source_data": json.dumps(self.source_data),
            }

        return {
            "_bizon_id": self.bizon_id,
            "_bizon_extracted_at": self.bizon_extracted_at,
            "_bizon_loaded_at": self.bizon_loaded_at,
            "_source_record_id": self.source_record_id,
            "_source_timestamp": self.source_timestamp,
            "_source_data": json.dumps(self.source_data),
        }

    def to_protobuf_serialization(self, TableRowClass: Type[Message], debezium=False):

        record = TableRowClass()
        record._bizon_id = self.bizon_id
        record._bizon_extracted_at = str(int(self.bizon_extracted_at.timestamp()))
        record._bizon_loaded_at = str(int(self.bizon_loaded_at.timestamp()))
        record._source_record_id = self.source_record_id
        record._source_timestamp = str(int(self.source_timestamp.timestamp()))

        if debezium:
            parsed_debezium_keys = json.loads(self.source_data["_bizon_message_key"])
            if parsed_debezium_keys:
                for _key in parsed_debezium_keys:
                    setattr(record, _key, str(parsed_debezium_keys[_key]))
            if self.source_data.get("op") == "d":
                source_data = {"__deleted": True, **self.source_data["before"]}
            else:
                source_data = {"__deleted": False, **self.source_data["after"]}

            record._source_data = json.dumps(source_data)
        else:
            record._source_data = json.dumps(self.source_data)

        return record.SerializeToString()
