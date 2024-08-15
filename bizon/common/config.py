from abc import ABC
from typing import Optional

from pydantic import BaseModel, Field

from bizon.api.auth.config import AuthConfig
from bizon.api.config import APIConfig
from bizon.backend.config import BackendConfig
from bizon.pipeline.config import PipelineConfig


class SourceConfig(BaseModel, ABC):
    source_name: str
    stream_name: str
    init_pipeline: Optional[bool] = Field(True, description="Whether to initialize the source to run pipeline directly")
    authentication: AuthConfig
    api_config: APIConfig = APIConfig(retry_limit=3)
    max_iterations: Optional[int] = Field(None, description="Maximum number of iterations when running the pipeline")


class DestinationConfig(BaseModel, ABC):
    name: str
    config: dict


class SyncConfig(BaseModel):
    source: SourceConfig
    destination: DestinationConfig
    pipeline: PipelineConfig
    backend: BackendConfig
