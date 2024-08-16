from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Field

from .adapters.kafka.config import KafkaConfig
from .adapters.threads.config import ThreadsConfig


class RunnerTypes(str, Enum):
    THREADS = "threads"
    KAFKA = "kafka"


class PipelineConfig(BaseModel):
    runner: RunnerTypes = Field(RunnerTypes.THREADS, description="Runner to use for the pipeline")
    runner_config: Optional[Union[KafkaConfig, ThreadsConfig]] = Field(
        ThreadsConfig(
            max_queue_size=1000,
            consumer_start_delay=2,
        ),
        description="Runner configuration",
    )
    log_level: str = Field("INFO", description="Logging level")

    # Validate the runner config based on the runner type
    def validate_runner_config(self):
        if self.runner == RunnerTypes.THREADS:
            ThreadsConfig.model_validate(obj=self.runner_config)
        elif self.runner == RunnerTypes.KAFKA:
            KafkaConfig.model_validate(obj=self.runner_config)
        else:
            raise ValueError(f"Runner type {self.runner} with config {self.runner_config} is not supported")
