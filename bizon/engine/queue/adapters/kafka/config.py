from typing import List, Optional

from pydantic import BaseModel, Field

from bizon.engine.queue.config_details import AbastractQueueConfigDetails


class KafkaQueueConfig(BaseModel):
    bootstrap_servers: str = Field("localhost:9092", description="Kafka bootstrap servers")
    topic: str = Field("bizon", description="Kafka topic")


class KafkaConsumerConfig(BaseModel):
    bootstrap_servers: List[str] = Field(["localhost:9092"], description="Kafka bootstrap servers")
    group_id: str = Field("bizon", description="Kafka group id")
    auto_offset_reset: str = Field("earliest", description="Kafka auto offset reset")
    enable_auto_commit: bool = Field(True, description="Kafka enable auto commit")
    consumer_timeout_ms: int = Field(1000, description="Kafka consumer timeout in milliseconds")


class KafkaConfig(AbastractQueueConfigDetails):
    queue: Optional[KafkaQueueConfig] = Field(KafkaQueueConfig(), description="Kafka queue configuration")
    consumer: Optional[KafkaConsumerConfig] = Field(KafkaConsumerConfig(), description="Kafka consumer configuration")
