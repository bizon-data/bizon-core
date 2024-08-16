from enum import Enum

from pydantic import BaseModel, Field


class RunnerTypes(str, Enum):
    THREADS = "threads"
    KAFKA = "kafka"


class PipelineConfig(BaseModel):
    runner: RunnerTypes = Field(RunnerTypes.THREADS, description="Runner to use for the pipeline")
    log_level: str = Field("INFO", description="Logging level")
