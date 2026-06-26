from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from bizon.alerting.models import AlertingConfig
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryColumn,
    BigQueryConfig,
)
from bizon.connectors.destinations.bigquery_streaming.src.config import (
    BigQueryStreamingConfig,
)
from bizon.connectors.destinations.bigquery_streaming_v2.src.config import (
    BigQueryStreamingV2Config,
)
from bizon.connectors.destinations.file.src.config import FileDestinationConfig
from bizon.connectors.destinations.logger.src.config import LoggerConfig
from bizon.engine.config import EngineConfig
from bizon.engine.resolvers.config import SecretsConfig
from bizon.monitoring.config import MonitoringConfig
from bizon.source.config import SourceConfig, SourceSyncModes
from bizon.transform.config import TransformModel


class StreamSourceConfig(BaseModel):
    """Source-specific stream routing configuration.

    Uses extra='allow' to support source-specific fields like:
    - topic (Kafka)
    - endpoint (API sources)
    - channel (other streaming sources)
    """

    model_config = ConfigDict(extra="allow")

    # Common field for stream identifier
    name: Optional[str] = Field(None, description="Stream identifier within the source")

    # Kafka-specific
    topic: Optional[str] = Field(None, description="Kafka topic name")

    # API-specific
    endpoint: Optional[str] = Field(None, description="API endpoint path")


class StreamDestinationConfig(BaseModel):
    """Destination configuration for a stream.

    Supports destination-specific schema definitions and options.
    Uses extra='allow' to support destination-specific overrides.
    """

    model_config = ConfigDict(extra="allow")

    # Universal destination identifier
    table_id: str = Field(..., description="Full destination identifier (e.g., project.dataset.table)")

    # BigQuery-specific schema (can be extended for other destinations)
    record_schema: Optional[list[BigQueryColumn]] = Field(None, description="Schema for the destination records")
    clustering_keys: Optional[list[str]] = Field(None, description="Clustering keys for the destination table")


class StreamConfig(BaseModel):
    """Configuration for a single stream.

    Consolidates source stream routing and destination configuration in one place,
    eliminating duplication of destination_id between source and destination configs.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Logical name for this stream")
    source: StreamSourceConfig = Field(..., description="Source-specific routing configuration")
    destination: StreamDestinationConfig = Field(
        ..., description="Destination configuration including table and schema"
    )

    @field_validator("destination")
    @classmethod
    def validate_table_id_format(cls, v: StreamDestinationConfig) -> StreamDestinationConfig:
        """Ensure table_id follows expected format for BigQuery-like destinations."""
        if v.table_id:
            parts = v.table_id.split(".")
            if len(parts) != 3:
                raise ValueError(
                    f"table_id must be in format 'project.dataset.table', got: {v.table_id}. "
                    f"Found {len(parts)} parts instead of 3."
                )
        return v


class BizonConfig(BaseModel):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    # Unique name to identify the sync configuration
    name: str = Field(..., description="Unique name for this sync configuration")

    source: SourceConfig = Field(
        description="Source configuration",
        default=...,
    )

    transforms: Optional[list[TransformModel]] = Field(
        description="List of transformations to apply to the source data",
        default=[],
    )

    destination: Union[
        BigQueryConfig,
        BigQueryStreamingConfig,
        BigQueryStreamingV2Config,
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

    alerting: Optional[AlertingConfig] = Field(
        description="Alerting configuration",
        default=None,
    )

    monitoring: Optional[MonitoringConfig] = Field(
        description="Monitoring configuration",
        default=None,
    )

    secrets: Optional[SecretsConfig] = Field(
        default=None,
        description="Provider defaults for gsm:// / env:// reference resolution",
    )

    streams: Optional[list[StreamConfig]] = Field(
        None,
        description="Stream routing configuration (opt-in for multi-table streaming). "
        "Consolidates source stream definitions with destination tables and schemas.",
    )

    @field_validator("streams")
    @classmethod
    def validate_streams_config(cls, v: Optional[list[StreamConfig]], info) -> Optional[list[StreamConfig]]:
        """Validate streams configuration consistency."""
        if not v:
            return v

        # Check for duplicate stream names
        names = [s.name for s in v]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate stream names found in streams configuration: {set(duplicates)}")

        # Check for duplicate table_ids
        table_ids = [s.destination.table_id for s in v]
        if len(table_ids) != len(set(table_ids)):
            duplicates = [tid for tid in table_ids if table_ids.count(tid) > 1]
            raise ValueError(f"Duplicate table_ids found in streams configuration: {set(duplicates)}")

        # Validate that source sync_mode is 'stream' if streams config is used
        source_config = info.data.get("source") if info.data else None
        if source_config and source_config.sync_mode != SourceSyncModes.STREAM:
            raise ValueError(
                f"Configuration Error: 'streams' configuration requires source.sync_mode='stream'. "
                f"Current sync_mode: {source_config.sync_mode}. "
                f"Please update your config to use:\n"
                f"  source:\n"
                f"    sync_mode: stream"
            )

        return v

    @model_validator(mode="before")
    @classmethod
    def inject_config_from_streams(cls, data: Any) -> Any:
        """Inject source and destination config from streams.

        This runs BEFORE field validation, enriching both source and destination
        configs from the streams configuration. This allows:
        1. Sources (like Kafka) to omit topics - they're extracted from streams
        2. Destinations with unnest=true to work without duplicate record_schemas

        This is source-agnostic: each source type can extract what it needs from streams.
        """
        if not isinstance(data, dict):
            return data

        streams = data.get("streams")
        if not streams:
            return data

        source = data.get("source")
        if source and isinstance(source, dict):
            source_name = source.get("name")

            # Kafka: inject topics from streams
            if source_name == "kafka":
                # Check if topics is missing, None, or empty list
                if not source.get("topics") or source.get("topics") == []:
                    topics = []
                    for stream in streams:
                        if isinstance(stream, dict):
                            stream_src = stream.get("source", {})
                            stream_dest = stream.get("destination", {})
                            if stream_src.get("topic"):
                                topics.append(
                                    {
                                        "name": stream_src.get("topic"),
                                        "destination_id": stream_dest.get("table_id", ""),
                                    }
                                )
                    if topics:
                        source["topics"] = topics

        destination = data.get("destination")
        if not destination or not isinstance(destination, dict):
            return data

        destination_config = destination.get("config")
        if not destination_config or not isinstance(destination_config, dict):
            return data

        # Only inject if record_schemas is not already set or is empty
        if not destination_config.get("record_schemas"):
            # Build record_schemas from streams
            record_schemas = []
            for stream in streams:
                if isinstance(stream, dict):
                    stream_dest = stream.get("destination", {})
                    if stream_dest.get("record_schema"):
                        record_schema_config = {
                            "destination_id": stream_dest.get("table_id"),
                            "record_schema": stream_dest.get("record_schema"),
                            "clustering_keys": stream_dest.get("clustering_keys"),
                        }
                        record_schemas.append(record_schema_config)

            # Inject into destination config
            if record_schemas:
                destination_config["record_schemas"] = record_schemas

        return data


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
    destination_alias: str

    @classmethod
    def from_bizon_config(cls, job_id: str, config: BizonConfig) -> "SyncMetadata":
        return cls(
            name=config.name,
            job_id=job_id,
            source_name=config.source.name,
            stream_name=config.source.stream,
            sync_mode=config.source.sync_mode,
            destination_name=config.destination.name,
            destination_alias=config.destination.alias,
        )
