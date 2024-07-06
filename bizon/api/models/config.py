from pydantic import BaseModel

class APIConfig(BaseModel):
    retry_limit: int

class AuthConfig(BaseModel):
    type: str

class SourceConfig(BaseModel):
    name: str
    stream_name: str
    auth_config: AuthConfig
    api_config: APIConfig = APIConfig(retry_limit=3)
