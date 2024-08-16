from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from bizon.destinations.config import DestinationConfig
from bizon.engine.config import EngineConfig
from bizon.source.config import SourceConfig


class JobStatus(str, Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class CursorStatus(str, Enum):
    NOT_STARTED = "not_started"
    SUCCESS = "success"
    FAILED = "failed"


class BizonRecord(BaseModel):
    _bizon_id: str
    _bizon_loaded_at: str
    _bizon_data: str


class BizonConfig(BaseModel):

    source: SourceConfig

    destination: DestinationConfig

    engine: Optional[EngineConfig] = Field(
        description="Engine configuration",
        default=EngineConfig(),
    )
