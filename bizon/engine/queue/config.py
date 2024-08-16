from abc import ABC
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel, Field

from .adapters.kafka.config import KafkaConfig, KafkaConsumerConfig
from .adapters.python_queue.config import PythonQueueConfig, PythonQueueConsumerConfig


class QueueTypes(str, Enum):
    KAFKA = "kafka"
    PYTHON_QUEUE = "python_queue"


class QueueConfigDetails(BaseModel):
    queue: Optional[Union[KafkaConfig, PythonQueueConfig]] = Field(
        PythonQueueConfig(max_size=1000), description="Configuration of the queue"
    )
    consumer: Optional[Union[KafkaConsumerConfig, PythonQueueConsumerConfig]] = Field(
        PythonQueueConsumerConfig(poll_interval=2), description="Configuration of the consumer"
    )


class QueueConfig(BaseModel):
    type: Optional[QueueTypes] = Field(QueueTypes.PYTHON_QUEUE, description="Type of the queue")
    config: Optional[
        Union[
            KafkaConfig,
            PythonQueueConfig,
        ]
    ] = Field(
        QueueConfigDetails(queue=PythonQueueConfig(max_size=1000), consumer=PythonQueueConsumerConfig(poll_interval=2)),
        escription="Configuration for the queue",
    )
