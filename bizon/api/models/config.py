from abc import ABC
from typing import Optional

from pydantic import BaseModel, Field


class APIConfig(BaseModel):
    retry_limit: int


class AuthConfig(BaseModel, ABC):
    type: str


class SourceConfig(BaseModel, ABC):
    name: str
    stream_name: str
    init_pipeline: Optional[bool] = Field(True, description="Whether to initialize the source to run pipeline directly")
    authentication: AuthConfig
    api_config: APIConfig = APIConfig(retry_limit=3)
    max_iterations: Optional[int] = Field(None, description="Maximum number of iterations when running the pipeline")
