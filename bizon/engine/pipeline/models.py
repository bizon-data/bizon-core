from enum import Enum


class PipelineReturnStatus(str, Enum):
    """Producer error types"""

    BACKEND_ERROR = "backend_error"
    DESTINATION_ERROR = "destination_error"
    KILLED_BY_RUNNER = "killed_by_runner"
    QUEUE_ERROR = "queue_error"
    RUNNING = "running"
    SOURCE_ERROR = "source_error"
    SUCCESS = "success"
    TRANSFORM_ERROR = "transform_error"
