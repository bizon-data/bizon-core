import json
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field
from pytz import UTC

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


class BizonConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    source: SourceConfig

    destination: DestinationConfig

    engine: Optional[EngineConfig] = Field(
        description="Engine configuration",
        default=EngineConfig(),
    )
