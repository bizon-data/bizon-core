from enum import Enum
from typing import Union

from pydantic import BaseModel, Field

from .bigquery.src.config import BigQueryConfig
from .logger.src.config import LoggerDestinationConfig


class DestinationTypes(str, Enum):
    BIGQUERY = "bigquery"
    LOGGER = "logger"


class DestinationConfig(BaseModel):
    name: DestinationTypes = Field(..., description="Name of the destination")
    config: Union[BigQueryConfig, LoggerDestinationConfig] = Field(..., description="Configuration for the destination")
