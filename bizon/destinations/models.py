import json
from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, Field
from pytz import UTC

from bizon.source.models import SourceRecord


class DestinationRecord(BaseModel):
    bizon_id: str = Field(..., description="Bizon unique identifier of the record")
    bizon_loaded_at: str = Field(..., description="Datetime when the record was loaded")
    source_record_id: str = Field(..., description="Source record id")
    source_data: str = Field(..., description="Source record JSON string data")

    @classmethod
    def from_source_record(cls, source_record: SourceRecord) -> "DestinationRecord":
        return cls(
            bizon_id=uuid4().hex,
            bizon_loaded_at=datetime.now(tz=UTC).isoformat(),
            source_record_id=source_record.id,
            source_data=json.dumps(source_record.data),
        )
