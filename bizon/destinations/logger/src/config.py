from typing import Literal, Optional

from pydantic import BaseModel

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class LoggerDestinationConfig(AbstractDestinationDetailsConfig):
    dummy: Optional[str] = "bizon"


class LoggerConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.LOGGER]
    config: LoggerDestinationConfig
