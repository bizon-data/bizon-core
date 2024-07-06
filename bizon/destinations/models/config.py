from pydantic import BaseModel
from typing import Optional

class APIConfig(BaseModel):
    retry_limit: int

class DestinationConfig(BaseModel):
    name: str
    api_config: Optional[APIConfig] = None
