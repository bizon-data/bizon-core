import io
import json
import logging
import struct
import traceback
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, List, Literal, Mapping, Tuple

import fastavro
from avro.schema import Schema, parse
from confluent_kafka import Consumer, KafkaException, TopicPartition
from loguru import logger
from pydantic import BaseModel, Field
from pytz import UTC
from requests.exceptions import HTTPError

from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

silent_logger = logging.getLogger()
silent_logger.addHandler(logging.StreamHandler())


class ApicurioSchemaNotFound(Exception):
    pass


class SchemaRegistryType(str, Enum):
    APICURIO = "apicurio"


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


class KafkaSourceConfig(SourceConfig):

    # Mandatory Kafka configuration
    topic: str = Field(..., description="Kafka topic")
    bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    group_id: str = Field(default="bizon", description="Kafka group id")

    skip_message_empty_value: bool = Field(
        default=True, description="Skip messages with empty value (tombstone messages)"
    )

    # Kafka consumer configuration
    batch_size: int = Field(100, description="Kafka batch size, number of messages to fetch at once.")
    consumer_timeout: int = Field(10, description="Kafka consumer timeout in seconds, before returning batch.")

    consumer_config: Mapping[str, Any] = Field(
        default_factory=default_kafka_consumer_config,
        description="Kafka consumer configuration, as described in the confluent-kafka-python documentation",
    )

    # Schema ID header configuration
    nb_bytes_schema_id: Literal[4, 8] = Field(
        description="Number of bytes encode SchemaID in Kafka message. Standard is 4.",
        default=4,
    )

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

        # Kafka consumer configuration
        if self.config.authentication.type == AuthType.BASIC:
            self.config.consumer_config["sasl.mechanisms"] = "PLAIN"
            self.config.consumer_config["sasl.username"] = self.config.authentication.params.username
            self.config.consumer_config["sasl.password"] = self.config.authentication.params.password

        # Set the bootstrap servers and group id
        self.config.consumer_config["group.id"] = self.config.group_id
        self.config.consumer_config["bootstrap.servers"] = self.config.bootstrap_servers

        # Consumer instance
        self.consumer = Consumer(self.config.consumer_config, logger=silent_logger)

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

            try:
                response = self.session.get(
                    f"{self.config.authentication.schema_registry_url}/apis/registry/v2/ids/globalIds/{global_id}",
                    auth=(
                        self.config.authentication.schema_registry_username,
                        self.config.authentication.schema_registry_password,
                    ),
                )

            except HTTPError as e:
                if e.response.status_code == 404:
                    raise ApicurioSchemaNotFound(f"Schema with global id {global_id} not found")

            return response.json()

        else:
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

    def get_header_bytes(self, message: bytes) -> bytes:
        if self.config.nb_bytes_schema_id == 8:
            return message[:9]

        elif self.config.nb_bytes_schema_id == 4:
            return message[1:5]

        else:
            raise ValueError(f"Number of bytes for schema id {self.config.nb_bytes_schema_id} not supported")

    def parse_encoded_messages(self, encoded_messages: list) -> List[SourceRecord]:

        records = []

        for message in encoded_messages:

            if message.error():
                logger.error(
                    (
                        f"Error while consuming message for partition {message.partition()} and offset {message.offset()}: "
                        f"{message.error()}"
                    )
                )
                raise KafkaException(message.error())

            # We skip tombstone messages
            if self.config.skip_message_empty_value and not message.value():
                logger.debug(
                    f"Message for partition {message.partition()} and offset {message.offset()} is empty, skipping."
                )
                continue

            # Get the schema for the message
            try:
                header_message_bytes = self.get_header_bytes(message.value())
                schema = self.get_message_schema(header_message_bytes)

            except ApicurioSchemaNotFound as e:
                message_schema_id = self.parse_global_id_from_serialized_message(header_message_bytes)
                logger.error(
                    (
                        f"Message on partition {message.partition()} at offset {message.offset()} has a  SchemaID of {message_schema_id} which is not found in Registry."
                        f"message value: {message.value()}."
                    )
                )
                logger.error(traceback.format_exc())
                raise e

            except Exception as e:
                logger.error(traceback.format_exc())
                raise e

            # Decode the message
            try:

                data = {
                    "offset": message.offset(),
                    "partition": message.partition(),
                    "timestamp": message.timestamp()[1],
                    "value": self.decode(message.value(), schema),
                    "key": message.key().decode("utf-8"),
                }

                records.append(
                    SourceRecord(
                        id=f"partition_{message.partition()}_offset_{message.offset()}",
                        timestamp=datetime.fromtimestamp(message.timestamp()[1] / 1000, tz=UTC),
                        data=data,
                    )
                )

                # Update the offset for the partition
                self.topic_offsets.set_partition_offset(message.partition(), message.offset() + 1)

            except Exception as e:
                logger.error(
                    (
                        f"Error while decoding message for partition {message.partition()}: {e} at offset {message.offset()} "
                        f"with value: {message.value()} and key: {message.key()}"
                    )
                )
                # Try to parse error message from the message
                try:
                    message_raw_text = message.value().decode("utf-8")
                    logger.error(f"Parsed Kafka value: {message_raw_text}")
                except UnicodeDecodeError:
                    logger.error("Message is not a valid UTF-8 string")

                logger.error(traceback.format_exc())
                raise e

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
        logger.info(f"Kafka consumer read : {len(encoded_messages)} messages in {datetime.now() - t1}")

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
