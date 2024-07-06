from pydantic import BaseModel
from abc import ABC

class APIConfig(BaseModel):
    retry_limit: int

class AuthConfig(BaseModel, ABC):
    type: str

class SourceConfig(BaseModel, ABC):
    name: str
    stream_name: str
    auth_config: AuthConfig
    api_config: APIConfig = APIConfig(retry_limit=3)
