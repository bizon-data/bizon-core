import json
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Union

import orjson
import polars as pl
from pydantic import BaseModel, Field, field_validator
from pytz import UTC

# Define the SourceRecord model
source_record_schema = pl.Schema(
    [
        ("id", str),
        ("data", str),  # JSON payload dumped as string
        ("timestamp", pl.Datetime(time_unit="us", time_zone="UTC")),
        ("destination_id", str),
    ]
)


def _json_default(value):
    # Avro decimals decode to Decimal; emit the exact number, as simplejson did.
    if isinstance(value, Decimal) and value.is_finite():
        return orjson.Fragment(str(value))
    # Avro `bytes` fields; simplejson decoded these as strict UTF-8.
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    raise TypeError(f"Type is not JSON serializable: {type(value).__name__}")


def _dump_data(data: dict) -> bytes:
    try:
        return orjson.dumps(data, default=_json_default, option=orjson.OPT_NON_STR_KEYS)
    except TypeError:
        # orjson refuses integers outside 64 bits; the stdlib encoder does not.
        return json.dumps(data, ensure_ascii=False).encode("utf-8")


def source_records_to_df(records: List["SourceRecord"]) -> pl.DataFrame:
    """Build the `source_record_schema` frame the queue carries from a list of records."""
    return pl.DataFrame(
        {
            "id": [record.id for record in records],
            "data": pl.Series([_dump_data(record.data) for record in records], dtype=pl.Binary).cast(pl.String),
            "timestamp": [record.timestamp for record in records],
            "destination_id": [record.destination_id for record in records],
        },
        schema=source_record_schema,
    )


### /!\ These models Source* will be used in all sources so we better never have to change them !!!
class SourceRecord(BaseModel):
    id: str = Field(..., description="Unique identifier of the record in the source")

    data: dict = Field(..., description="JSON payload of the record")

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=UTC),
        description="Timestamp of the record as defined by the source. Default is the time of extraction",
    )

    destination_id: Optional[str] = Field(None, description="Destination id")

    @field_validator("id", mode="before")
    def coerce_int_to_str(value: Union[int, str]) -> str:
        # Coerce int to str in case Source return id as int
        if isinstance(value, int):
            return str(value)
        return value


class SourceIteration(BaseModel):
    next_pagination: dict = Field(..., description="Next pagination to be used in the next iteration")
    records: List[SourceRecord] = Field(..., description="List of records retrieved in the current iteration")
    next_state: Optional[dict] = Field(
        default=None,
        description="State to hand the next incremental run as `source_state.state`. The last non-None value "
        "of a run is persisted when the job succeeds; a run that emits none keeps the previous state.",
    )

    # Fail on the iteration that emits it rather than when the state is persisted at the end of the run.
    @field_validator("next_state", mode="after")
    def check_json_serializable(value: Optional[dict]) -> Optional[dict]:
        if value is not None:
            try:
                json.dumps(value)
            except TypeError as e:
                raise ValueError(f"next_state must be JSON-serializable: {e}") from e
        return value


class SourceIncrementalState(BaseModel):
    last_run: datetime = Field(..., description="Start time (`created_at`) of the last successful job, in UTC")
    state: dict = Field(default_factory=dict, description="The `next_state` persisted by the last successful job")
    cursor_field: Optional[str] = Field(default=None, description="The field name to filter records by timestamp")
    run_started_at: Optional[datetime] = Field(
        default=None,
        description="Start time of the current job, in UTC and stable across resumes. It is the `last_run` the "
        "next run will receive, so using it as this run's upper bound makes consecutive windows tile.",
    )

    # Job timestamps come back naive from the backend's DateTime columns, but are always written in UTC.
    @field_validator("last_run", "run_started_at", mode="after")
    def assume_utc(value: Optional[datetime]) -> Optional[datetime]:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
