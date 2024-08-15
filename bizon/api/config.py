from pydantic import BaseModel, Field


class APIConfig(BaseModel):
    retry_limit: int = Field(100, description="Number of retries before giving up", example=100)
