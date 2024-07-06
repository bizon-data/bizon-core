from abc import ABC

from pydantic import BaseModel


class APIConfig(BaseModel):
    retry_limit: int


class AuthConfig(BaseModel, ABC):
    type: str


class SourceConfig(BaseModel, ABC):
    name: str
    stream_name: str
    authentication: AuthConfig
    api_config: APIConfig = APIConfig(retry_limit=3)
