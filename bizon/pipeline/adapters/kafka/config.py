from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    topic: str = Field(..., description="Kafka topic")
