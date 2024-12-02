from typing import Union

from pydantic import BaseModel, ConfigDict, Field

from bizon.destinations.bigquery.src.config import BigQueryConfig
from bizon.destinations.bigquery_streaming.src.config import BigQueryStreamingConfig
from bizon.destinations.file.src.config import FileDestinationConfig
from bizon.destinations.logger.src.config import LoggerConfig
from bizon.engine.config import EngineConfig
from bizon.source.config import SourceConfig, SourceSyncModes


class BizonConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    # Unique name to identify the sync configuration
    name: str = Field(..., description="Unique name for this sync configuration")

    source: SourceConfig = Field(
        description="Source configuration",
        default=...,
    )

    destination: Union[
        BigQueryConfig,
        BigQueryStreamingConfig,
        LoggerConfig,
        FileDestinationConfig,
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

    name: str
    job_id: str
    source_name: str
    stream_name: str
    sync_mode: SourceSyncModes
    destination_name: str

    @classmethod
    def from_bizon_config(cls, job_id: str, config: BizonConfig) -> "SyncMetadata":
        return cls(
            name=config.name,
            job_id=job_id,
            source_name=config.source.source_name,
            stream_name=config.source.stream_name,
            sync_mode=config.source.sync_mode,
            destination_name=config.destination.name,
        )
