from typing import Optional

from pydantic import BaseModel, Field


class PipelineConfig(BaseModel):
    log_level: str = Field("INFO", description="Logging level")
