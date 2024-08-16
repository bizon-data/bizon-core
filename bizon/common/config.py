from abc import ABC
from typing import List, Optional, Union

from pydantic import BaseModel, Field

from bizon.api.auth.config import AuthConfig
from bizon.api.config import APIConfig
from bizon.backend.config import BackendConfig
from bizon.pipeline.config import PipelineConfig


class SourceConfig(BaseModel, ABC):
    source_name: str = Field(..., description="Name of the source to sync")
    stream_name: str = Field(..., description="Name of the stream to sync")
    init_pipeline: Optional[bool] = Field(True, description="Whether to initialize the source to run pipeline directly")
    authentication: AuthConfig = Field(..., description="Configuration for the authentication")
    api_config: APIConfig = Field(APIConfig(retry_limit=3), description="Configuration for the API client")
    max_iterations: Optional[int] = Field(None, description="Maximum number of iterations when running the pipeline")


class ConsumerConfig(BaseModel, ABC):
    enabled: bool = Field(..., description="Whether the consumer is enabled")


class DestinationConfig(ConsumerConfig):
    name: str = Field(..., description="Name of the destination")
    config: dict = Field(dict(), description="Configuration for the destination")


class KafkaConsumerConfig(ConsumerConfig):
    bootstrap_servers: List[str] = Field(["localhost:9092"], description="Kafka bootstrap servers")
    group_id: str = Field("bizon", description="Kafka group id")
    auto_offset_reset: str = Field("earliest", description="Kafka auto offset reset")
    enable_auto_commit: bool = Field(True, description="Kafka enable auto commit")
    consumer_timeout_ms: int = Field(1000, description="Kafka consumer timeout in milliseconds")


class SyncConfig(BaseModel):
    source: SourceConfig = Field(..., description="Configuration for the source")
    consumer: Union[DestinationConfig, KafkaConsumerConfig] = Field(..., description="Configuration for the consumer")
    pipeline: PipelineConfig = Field(..., description="Configuration for the pipeline")
    backend: BackendConfig = Field(..., description="Configuration for the backend")
