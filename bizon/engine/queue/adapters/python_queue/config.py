from typing import Literal

from pydantic import BaseModel, Field

from bizon.engine.queue.config import (
    AbastractQueueConfigDetails,
    AbstractQueueConfig,
    QueueTypes,
)


class PythonQueueQueueConfig(BaseModel):
    max_size: int = Field(1000, description="Maximum size of the queue")


class PythonQueueConsumerConfig(BaseModel):
    poll_interval: int = Field(1, description="Interval in seconds to poll the queue in seconds")


class PythonQueueConfigDetails(AbastractQueueConfigDetails):
    queue: PythonQueueQueueConfig = Field(..., description="Configuration of the queue")
    consumer: PythonQueueConsumerConfig = Field(..., description="Kafka consumer configuration")


class PythonQueueConfig(AbstractQueueConfig):
    type: Literal[QueueTypes.PYTHON_QUEUE]
    config: PythonQueueConfigDetails
