from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from .backend.config import BackendConfig, BackendTypes
from .queue.adapters.python_queue.config import (
    PythonQueueConfig,
    PythonQueueConfigDetails,
    PythonQueueConsumerConfig,
)
from .queue.config import QueueConfig, QueueTypes
from .runners.config import RunnerConfig, RunnerTypes, ThreadsConfig


class EngineConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    backend: Optional[BackendConfig] = Field(
        description="Configuration for the backend",
        default=BackendConfig(
            type=BackendTypes.SQLITE, database="NOT_USED_IN_SQLITE", schema="NOT_USED_IN_SQLITE", syncCursorInDBEvery=2
        ),
    )

    queue: Optional[QueueConfig] = Field(
        description="Configuration for the queue",
        default=QueueConfig(
            type=QueueTypes.PYTHON_QUEUE,
            config=PythonQueueConfig(
                queue=PythonQueueConfigDetails(max_size=1000),
                consumer=PythonQueueConsumerConfig(poll_interval=2),
            ),
        ),
    )

    runner: Optional[RunnerConfig] = Field(
        description="Runner to use for the pipeline",
        default=RunnerConfig(
            type=RunnerTypes.THREADS,
            config=ThreadsConfig(
                consumer_start_delay=2,
            ),
            log_level="INFO",
        ),
    )
