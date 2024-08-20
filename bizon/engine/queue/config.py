from abc import ABC
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class QueueTypes(str, Enum):
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    PYTHON_QUEUE = "python_queue"


class AbastractQueueConfigDetails(BaseModel, ABC):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    queue: BaseModel = Field(..., description="Configuration of the queue")
    consumer: BaseModel = Field(..., description="Configuration of the consumer")


class AbstractQueueConfig(BaseModel, ABC):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    type: QueueTypes = Field(..., description="Type of the queue")
    config: AbastractQueueConfigDetails = Field(..., description="Configuration of the queue")
