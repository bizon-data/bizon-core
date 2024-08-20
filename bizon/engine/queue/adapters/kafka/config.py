from typing import List, Literal

from pydantic import BaseModel, Field

from bizon.engine.queue.config import (
    AbastractQueueConfigDetails,
    AbstractQueueConfig,
    QueueTypes,
)


class KafkaQueueConfig(BaseModel):
    bootstrap_server: str = Field(..., description="Kafka bootstrap servers")
    topic: str = Field("bizon", description="Kafka topic")


class KafkaConsumerConfig(BaseModel):
    group_id: str = Field("bizon", description="Kafka group id")
    auto_offset_reset: str = Field("earliest", description="Kafka auto offset reset")
    enable_auto_commit: bool = Field(True, description="Kafka enable auto commit")
    consumer_timeout_ms: int = Field(1000, description="Kafka consumer timeout in milliseconds")


class KafkaConfigDetails(AbastractQueueConfigDetails):
    queue: KafkaQueueConfig = Field(..., description="Kafka queue configuration")
    consumer: KafkaConsumerConfig = Field(KafkaConsumerConfig(), description="Kafka consumer configuration")


class KafkaConfig(AbstractQueueConfig):
    type: Literal[QueueTypes.KAFKA]
    config: KafkaConfigDetails
