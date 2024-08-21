from typing import Union

from pydantic import BaseModel, ConfigDict, Field

from bizon.destinations.bigquery.src.config import BigQueryConfig
from bizon.destinations.logger.src.config import LoggerConfig
from bizon.engine.config import EngineConfig
from bizon.source.config import SourceConfig, SourceSyncModes


class BizonConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    source: SourceConfig = Field(
        description="Source configuration",
        default=...,
    )

    destination: Union[
        BigQueryConfig,
        LoggerConfig,
    ] = Field(
        description="Destination configuration",
        discriminator="name",
        default=...,
    )

    engine: EngineConfig = Field(
        description="Engine configuration",
        default=EngineConfig(),
    )


class SyncMetadata(BaseModel):
    """Model which stores general metadata around a sync.
    Facilitate usage of basic info across entities
    """

    source_name: str
    stream_name: str
    sync_mode: SourceSyncModes
    destination_name: str

    @classmethod
    def from_bizon_config(cls, config: BizonConfig):
        return cls(
            source_name=config.source.source_name,
            stream_name=config.source.stream_name,
            sync_mode=config.source.sync_mode,
            destination_name=config.destination.name,
        )
