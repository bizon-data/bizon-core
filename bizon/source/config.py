from abc import ABC
from typing import Optional

from pydantic import BaseModel, Field

from .auth.config import AuthConfig


class APIConfig(BaseModel):
    retry_limit: Optional[int] = Field(100, description="Number of retries before giving up", example=100)


class SourceConfig(BaseModel, ABC):

    source_name: str = Field(..., description="Name of the source to sync")

    stream_name: str = Field(..., description="Name of the stream to sync")

    init_pipeline: Optional[bool] = Field(True, description="Whether to initialize the source to run pipeline directly")

    authentication: AuthConfig = Field(..., description="Configuration for the authentication")

    max_iterations: Optional[int] = Field(
        None,
        description="Maximum number of iterations when running the pipeline",
    )

    api_config: Optional[APIConfig] = Field(
        APIConfig(retry_limit=10),
        description="Configuration for the API client",
    )
