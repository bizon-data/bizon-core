from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class RunnerTypes(str, Enum):
    THREAD = "thread"
    PROCESS = "process"


class RunnerFuturesConfig(BaseModel):
    max_workers: Optional[int] = Field(
        description="Number of workers to use for the runner",
        default=2,
    )
    consumer_start_delay: Optional[int] = Field(
        description="Duration in seconds to wait before starting the consumer thread",
        default=2,
    )
    is_alive_check_interval: Optional[int] = Field(
        description="Duration in seconds to wait between checking if the producer and consumer threads are still running",
        default=2,
    )


class RunnerConfig(BaseModel):

    type: RunnerTypes = Field(
        description="Runner to use for the pipeline",
        default=RunnerTypes.THREAD,
    )

    config: Optional[RunnerFuturesConfig] = Field(
        description="Runner configuration",
        default=RunnerFuturesConfig(),
    )

    log_level: str = Field(
        description="Logging level",
        default="INFO",
    )
