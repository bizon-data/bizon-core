from typing import List, Optional

from pydantic import BaseModel, Field

from bizon.engine.queue.config_details import AbastractQueueConfigDetails


class KafkaQueueConfig(BaseModel):
    bootstrap_servers: Optional[str] = Field("localhost:9092", description="Kafka bootstrap servers")
    topic: str = Field(..., description="Kafka topic")


class KafkaConsumerConfig(BaseModel):
    bootstrap_servers: Optional[List[str]] = Field(["localhost:9092"], description="Kafka bootstrap servers")
    group_id: Optional[str] = Field("bizon", description="Kafka group id")
    auto_offset_reset: Optional[str] = Field("earliest", description="Kafka auto offset reset")
    enable_auto_commit: Optional[bool] = Field(True, description="Kafka enable auto commit")
    consumer_timeout_ms: Optional[int] = Field(1000, description="Kafka consumer timeout in milliseconds")


class KafkaConfig(AbastractQueueConfigDetails):
    queue: KafkaQueueConfig
    consumer: Optional[KafkaConsumerConfig] = Field(KafkaConsumerConfig(), description="Kafka consumer configuration")
