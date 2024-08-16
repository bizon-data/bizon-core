from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class RunnerTypes(str, Enum):
    THREADS = "threads"


class ThreadsConfig(BaseModel):
    consumer_start_delay: Optional[int] = Field(
        description="Duration in seconds to wait before starting the consumer thread",
        default=2,
    )


class RunnerConfig(BaseModel):

    type: RunnerTypes = Field(
        description="Runner to use for the pipeline",
        default=RunnerTypes.THREADS,
    )

    config: Optional[ThreadsConfig] = Field(
        description="Runner configuration",
        default=ThreadsConfig(
            max_queue_size=1000,
            consumer_start_delay=2,
        ),
    )

    log_level: str = Field(
        description="Logging level",
        default="INFO",
    )
