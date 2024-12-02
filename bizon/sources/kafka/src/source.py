import io
import json
import logging
import struct
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from typing import Any, List, Literal, Mapping, Optional, Tuple

import fastavro
from avro.schema import Schema, parse
from confluent_kafka import Consumer, KafkaException, TopicPartition
from loguru import logger
from pydantic import BaseModel, Field

from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

silent_logger = logging.getLogger()
silent_logger.addHandler(logging.StreamHandler())


class SchemaRegistryType(str, Enum):
    APICURIO = "apicurio"
    CONFLUENT = "confluent"


class KafkaAuthConfig(AuthConfig):

    type: Literal[AuthType.BASIC] = AuthType.BASIC  # username and password authentication

    # Schema registry authentication
    schema_registry_type: SchemaRegistryType = Field(
        default=SchemaRegistryType.APICURIO, description="Schema registry type"
    )

    schema_registry_url: str = Field(default="", description="Schema registry URL with the format ")
    schema_registry_username: str = Field(default="", description="Schema registry username")
    schema_registry_password: str = Field(default="", description="Schema registry password")


class KafkaSourceConfig(SourceConfig):
    topic: str = Field(..., description="Kafka topic")
    bootstrap_server: str = Field(..., description="Kafka bootstrap servers")
    batch_size: int = Field(100, description="Kafka batch size")
    consumer_timeout: int = Field(10, description="Kafka consumer timeout in seconds")
    group_id: str = Field("bizon", description="Kafka group id")

    max_consumer_threads: int = Field(16, description="Maximum number of threads for the consumer")

    nb_bytes_schema_id: Literal[4, 8] = Field(
        4, description="Number of bytes for the schema id. 4 is the default for majority of the cases"
    )
    timestamp_ms_name: Optional[str] = Field(default="", description="Name of the timestamp field in the Avro schema")

    authentication: KafkaAuthConfig = Field(..., description="Authentication configuration")


class OffsetPartition(BaseModel):
    first: int
    last: int
    to_fetch: int = 0


class TopicOffsets(BaseModel):
    name: str
    partitions: Mapping[int, OffsetPartition]

    def set_partition_offset(self, index: int, offset: int):
        self.partitions[index].to_fetch = offset

    def get_partition_offset(self, index: int) -> int:
        return self.partitions[index].to_fetch

    @property
    def total_offset(self) -> int:
        return sum([partition.last for partition in self.partitions.values()])


class KafkaSource(AbstractSource):

    def __init__(self, config: KafkaSourceConfig):
        super().__init__(config)

        self.config: KafkaSourceConfig = config

        self.parse_timestamp: bool = self.config.timestamp_ms_name != ""

        self.kafka_consumer_conf = {
            "bootstrap.servers": self.config.bootstrap_server,
            "group.id": self.config.group_id,
            "sasl.username": self.config.authentication.params.username,
            "sasl.password": self.config.authentication.params.password,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "session.timeout.ms": 45000,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Turn off auto-commit for manual offset handling
        }

        # Consumer instance
        self.consumer = Consumer(self.kafka_consumer_conf, logger=silent_logger)

        # Consumers for each worker thread
        self.consumers_cached: Mapping[int, Consumer] = {}

    @staticmethod
    def streams() -> List[str]:
        return ["topic"]

    def get_authenticator(self):
        # We don't use HTTP authentication for Kafka
        # We use confluence_kafka library to authenticate
        pass

    @staticmethod
    def get_config_class() -> AbstractSource:
        return KafkaSourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        """Check the connection to the Kafka source"""

        logger.info(f"Found: {len(self.consumer.list_topics().topics)} topics")

        topics = self.consumer.list_topics().topics

        if self.config.topic not in topics:
            logger.error(f"Topic {self.config.topic} not found, available topics: {topics.keys()}")
            return False, f"Topic {self.config.topic} not found"

        logger.info(f"Topic {self.config.topic} has {len(topics[self.config.topic].partitions)} partitions")

        return True, None

    def get_number_of_partitions(self) -> int:
        """Get the number of partitions for the topic"""
        return len(self.consumer.list_topics().topics[self.config.topic].partitions)

    def get_offset_partitions(self) -> TopicOffsets:
        """Get the offsets for each partition of the topic"""

        partitions: Mapping[int, OffsetPartition] = {}

        for i in range(self.get_number_of_partitions()):
            offsets = self.consumer.get_watermark_offsets(TopicPartition(self.config.topic, i))
            partitions[i] = OffsetPartition(first=offsets[0], last=offsets[1])

        return TopicOffsets(name=self.config.topic, partitions=partitions)

    def get_total_records_count(self) -> int | None:
        """Get the total number of records in the topic, sum of offsets for each partition"""
        # Init the consumer
        return self.get_offset_partitions().total_offset

    def parse_global_id_from_serialized_message(self, header_message: bytes) -> int:
        """Parse the global id from the serialized message"""

        if self.config.nb_bytes_schema_id == 8:
            return struct.unpack(">bq", header_message)[1]

        if self.config.nb_bytes_schema_id == 4:
            return struct.unpack(">I", header_message)[0]

        raise ValueError(f"Number of bytes for schema id {self.config.nb_bytes_schema_id} not supported")

    def get_apicurio_schema(self, global_id: int) -> dict:
        """Get the schema from the Apicurio schema registry"""

        if self.config.authentication.schema_registry_type == SchemaRegistryType.APICURIO:
            schema = self.session.get(
                f"{self.config.authentication.schema_registry_url}/apis/registry/v2/ids/globalIds/{global_id}",
                auth=(
                    self.config.authentication.schema_registry_username,
                    self.config.authentication.schema_registry_password,
                ),
            ).json()
            return schema

        raise NotImplementedError(
            f"Schema registry of type {self.config.authentication.schema_registry_type} not supported"
        )

    def get_parsed_avro_schema(self, global_id: int) -> Schema:
        """Parse the schema from the Apicurio schema registry"""
        schema = self.get_apicurio_schema(global_id)
        schema["name"] = "Envelope"
        return parse(json.dumps(schema))

    def decode(self, msg_value, schema):
        message_bytes = io.BytesIO(msg_value)
        message_bytes.seek(self.config.nb_bytes_schema_id + 1)
        event_dict = fastavro.schemaless_reader(message_bytes, schema)
        return event_dict

    @lru_cache(maxsize=None)
    def get_message_schema(self, header_message: bytes) -> dict:
        """Get the global id of the schema for the topic"""
        global_id = self.parse_global_id_from_serialized_message(header_message)
        return self.get_parsed_avro_schema(global_id).to_json()

    def parse_encoded_messages(self, encoded_messages: list) -> List[SourceRecord]:

        records = []

        # Set the source timestamp to now, otherwise it will be overwritten by the message timestamp
        source_timestamp = datetime.now(tz=timezone.utc)

        for message in encoded_messages:
            if not message.value():
                logger.debug(
                    f"Message for partition {message.partition()} and offset {message.offset()} and topic {self.config.topic} is empty, skipping."
                )
                continue

            try:
                if self.config.nb_bytes_schema_id == 8:
                    schema = self.get_message_schema(message.value()[:9])
                elif self.config.nb_bytes_schema_id == 4:
                    schema = self.get_message_schema(message.value()[1:5])
                else:
                    raise ValueError(f"Number of bytes for schema id {self.config.nb_bytes_schema_id} not supported")

                data = self.decode(message.value(), schema)
                data["_bizon_message_key"] = message.key().decode("utf-8")

                # Get the source timestamp
                if self.parse_timestamp:
                    source_timestamp = datetime.fromtimestamp(
                        data[self.config.timestamp_ms_name] / 1000, tz=timezone.utc
                    )

                self.topic_offsets.set_partition_offset(message.partition(), message.offset() + 1)

                records.append(
                    SourceRecord(
                        id=f"part_{message.partition()}_offset_{message.offset()}",
                        timestamp=source_timestamp,
                        data=data,
                    )
                )

            except Exception as e:
                logger.error(
                    f"Error while decoding message for partition {message.partition()}: {e} at offset {message.offset()}"
                )
                continue

        return records

    def read_topic(self, pagination: dict = None) -> SourceIteration:
        nb_partitions = self.get_number_of_partitions()

        # Setup offset_pagination
        self.topic_offsets = TopicOffsets.model_validate(pagination) if pagination else self.get_offset_partitions()

        self.consumer.assign(
            [
                TopicPartition(self.config.topic, partition, self.topic_offsets.get_partition_offset(partition))
                for partition in range(nb_partitions)
            ]
        )

        t1 = datetime.now()
        encoded_messages = self.consumer.consume(self.config.batch_size, timeout=self.config.consumer_timeout)
        logger.info(f"Read Kafka: {len(encoded_messages)} messages in {datetime.now() - t1}")

        records = self.parse_encoded_messages(encoded_messages)

        # Update the offset for the partition
        if not records:
            logger.info("No new records found, stopping iteration")
            return SourceIteration(
                next_pagination={},
                records=[],
            )

        return SourceIteration(
            next_pagination=self.topic_offsets.model_dump(),
            records=records,
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        return self.read_topic(pagination)
