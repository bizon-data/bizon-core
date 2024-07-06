from typing import Optional

from pydantic import BaseModel


class APIConfig(BaseModel):
    retry_limit: int


class DestinationConfig(BaseModel):
    name: str
    api_config: Optional[APIConfig] = None
