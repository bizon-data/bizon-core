from abc import ABC
from typing import Optional

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


class DestinationConfig(BaseModel, ABC):
    name: str = Field(..., description="Name of the destination to sync")
    config: dict = Field(dict(), description="Configuration for the destination")


class SyncConfig(BaseModel):
    source: SourceConfig = Field(..., description="Configuration for the source")
    destination: DestinationConfig = Field(..., description="Configuration for the destination")
    pipeline: PipelineConfig = Field(..., description="Configuration for the pipeline")
    backend: BackendConfig = Field(..., description="Configuration for the backend")
