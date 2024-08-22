from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    LOGGER = "logger"
    FILE = "file"


class AbstractDestinationDetailsConfig(BaseModel):
    buffer_size: int = Field(default=2000, description="Buffer size for the destination")


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    config: AbstractDestinationDetailsConfig = Field(..., description="Configuration for the destination")
