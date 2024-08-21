from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    LOGGER = "logger"


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    config: BaseModel = Field(..., description="Configuration for the destination")
