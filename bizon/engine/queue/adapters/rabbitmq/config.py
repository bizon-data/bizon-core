from typing import Literal

from pydantic import BaseModel, Field

from bizon.engine.queue.config import (
    AbastractQueueConfigDetails,
    AbstractQueueConfig,
    QueueTypes,
)


class RabbitMQQueueConfig(BaseModel):
    host: str = Field(..., description="RabbitMQ host")
    port: int = Field(5672, description="RabbitMQ port")
    queue_name: str = Field(..., description="RabbitMQ queue")
    exchange: str = Field("", description="RabbitMQ exchange")


class RabbitMQConsumerConfig(BaseModel):
    poll_interval: int = Field(1, description="Interval in seconds to poll the queue in seconds")


class RabbitMQConfigDetails(AbastractQueueConfigDetails):
    queue: RabbitMQQueueConfig = Field(..., description="RabbitMQ queue configuration")
    consumer: RabbitMQConsumerConfig = Field(RabbitMQConsumerConfig(), description="Rabbitmq consumer configuration")


class RabbitMQConfig(AbstractQueueConfig):
    type: Literal[QueueTypes.RABBITMQ]
    config: RabbitMQConfigDetails
