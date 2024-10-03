from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    LOGGER = "logger"
    FILE = "file"


class NormalizationType(str, Enum):
    TABULAR = "tabular"  # Parse key / value pairs to columns
    NONE = "none"  # No normalization, raw data is stored


class NormalizationConfig(BaseModel):
    type: NormalizationType = Field(description="Normalization type")


class AbstractDestinationDetailsConfig(BaseModel):
    buffer_size: int = Field(
        default=50,
        description="Buffer size in Mb for the destination. Set to 0 to disable and write directly to the destination.",
    )
    buffer_flush_timeout: int = Field(
        default=600,
        description="Maximum time in seconds for buffering after which the records will be written to the destination. Set to 0 to deactivate the timeout buffer check.",
    )
    normalization: Optional[NormalizationConfig] = Field(
        description="Normalization configuration, by default no normalization is applied",
        default=NormalizationConfig(type=NormalizationType.NONE),
    )


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    config: AbstractDestinationDetailsConfig = Field(..., description="Configuration for the destination")
