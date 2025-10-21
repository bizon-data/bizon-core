from enum import Enum
from typing import Any, List, Literal, Mapping

from pydantic import BaseModel, Field

from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig


class SchemaRegistryType(str, Enum):
    APICURIO = "apicurio"


class MessageEncoding(str, Enum):
    UTF_8 = "utf-8"
    AVRO = "avro"


class KafkaAuthConfig(AuthConfig):

    type: Literal[AuthType.BASIC] = AuthType.BASIC  # username and password authentication

    # Schema registry authentication
    schema_registry_type: SchemaRegistryType = Field(
        default=SchemaRegistryType.APICURIO, description="Schema registry type"
    )

    schema_registry_url: str = Field(default="", description="Schema registry URL with the format ")
    schema_registry_username: str = Field(default="", description="Schema registry username")
    schema_registry_password: str = Field(default="", description="Schema registry password")


def default_kafka_consumer_config():
    return {
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Turn off auto-commit for manual offset handling
        "session.timeout.ms": 45000,
        "security.protocol": "SASL_SSL",
    }


class TopicConfig(BaseModel):
    name: str = Field(..., description="Kafka topic name")
    destination_id: str = Field(..., description="Destination id")


class KafkaSourceConfig(SourceConfig):

    # Mandatory Kafka configuration
    topics: List[TopicConfig] = Field(..., description="Kafka topic, comma separated")
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    group_id: str = Field(default="bizon", description="Kafka group id")

    skip_message_empty_value: bool = Field(
        default=True, description="Skip messages with empty value (tombstone messages)"
    )
    skip_message_invalid_keys: bool = Field(
        default=False, description="Skip messages with invalid keys (unparsable JSON keys)"
    )
    # Kafka consumer configuration
    batch_size: int = Field(100, description="Kafka batch size, number of messages to fetch at once.")
    consumer_timeout: int = Field(10, description="Kafka consumer timeout in seconds, before returning batch.")

    consumer_config: Mapping[str, Any] = Field(
        default_factory=default_kafka_consumer_config,
        description="Kafka consumer configuration, as described in the confluent-kafka-python documentation",
    )

    message_encoding: str = Field(default=MessageEncoding.AVRO, description="Encoding to use to decode the message")

    authentication: KafkaAuthConfig = Field(..., description="Authentication configuration")
