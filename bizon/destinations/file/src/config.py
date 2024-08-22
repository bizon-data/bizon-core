from typing import Literal

from pydantic import Field

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class FileDestinationDetailsConfig(AbstractDestinationDetailsConfig):
    filepath: str = Field(..., title="Filepath", description="Path to the file where the data will be written")


class FileDestinationConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.FILE]
    config: FileDestinationDetailsConfig
