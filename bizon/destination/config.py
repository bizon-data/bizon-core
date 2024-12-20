from abc import ABC
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    BIGQUERY_STREAMING = "bigquery_streaming"
    LOGGER = "logger"
    FILE = "file"


class DestinationColumn(BaseModel, ABC):
    name: str = Field(..., description="Name of the column")
    type: str = Field(..., description="Type of the column")
    description: Optional[str] = Field(None, description="Description of the column")


class AbstractDestinationDetailsConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    buffer_size: int = Field(
        default=50,
        description="Buffer size in Mb for the destination. Set to 0 to disable and write directly to the destination.",
    )

    buffer_flush_timeout: int = Field(
        default=600,
        description="Maximum time in seconds for buffering after which the records will be written to the destination. Set to 0 to deactivate the timeout buffer check.",  # noqa
    )

    record_schema: Optional[list[DestinationColumn]] = Field(
        default=None, description="Schema for the records. Required if unnest is set to true."
    )

    unnest: bool = Field(
        default=False,
        description="Unnest the data before writing to the destination. Schema should be provided in the model_config.",
    )

    authentication: Optional[BaseModel] = Field(
        description="Authentication configuration for the destination, if needed", default=None
    )

    @field_validator("unnest", mode="before")
    def validate_record_schema_if_unnest(cls, value, values):
        if bool(value) and values.data.get("record_schema") is None:
            raise ValueError("A `record_schema` must be provided if `unnest` is set to True.")

        return value


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    config: AbstractDestinationDetailsConfig = Field(..., description="Configuration for the destination")
