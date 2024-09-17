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
    buffer_size: int = Field(default=2000, description="Buffer size for the destination")
    normalization: Optional[NormalizationConfig] = Field(
        description="Normalization configuration, by default no normalization is applied",
        default=NormalizationConfig(type=NormalizationType.NONE),
    )


class AbstractDestinationConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    name: DestinationTypes = Field(..., description="Name of the destination")
    config: AbstractDestinationDetailsConfig = Field(..., description="Configuration for the destination")
