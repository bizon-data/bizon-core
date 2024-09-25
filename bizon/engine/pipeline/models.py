from enum import Enum


class PipelineReturnStatus(Enum):
    """Producer error types"""

    SUCCESS = "success"
    QUEUE_ERROR = "queue_error"
    SOURCE_ERROR = "source_error"
    BACKEND_ERROR = "backend_error"
