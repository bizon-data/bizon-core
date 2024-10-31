import io
import json
import struct
from typing import Any, List, Literal, Mapping, Tuple

from avro.io import BinaryDecoder, DatumReader
from avro.schema import Schema, parse
from confluent_kafka import Consumer, KafkaException, TopicPartition
from loguru import logger
from pydantic import BaseModel, Field

from bizon.source.auth.config import AuthConfig, AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class KafkaAuthConfig(AuthConfig):
    type: Literal[AuthType.BASIC] = AuthType.BASIC  # username and password authentication
    schema_registry_url: str = Field(default="", description="Schema registry URL with the format ")
    schema_registry_username: str = Field(default="", description="Schema registry username")
    schema_registry_password: str = Field(default="", description="Schema registry password")


class KafkaSourceConfig(SourceConfig):
    topic: str = Field(..., description="Kafka topic")
    bootstrap_server: str = Field(..., description="Kafka bootstrap servers")
    batch_size: int = Field(100, description="Kafka batch size")
    consumer_timeout: int = Field(10, description="Kafka consumer timeout in seconds")
    group_id: str = Field("bizon", description="Kafka group id")

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
        self.consumer = Consumer(self.kafka_consumer_conf)

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

    def parse_global_id_from_serialized_message(self, message: bytes) -> int:
        """Parse the global id from the serialized message"""
        return struct.unpack(">bq", message[:9])[1]

    def get_apicurio_schema(self, global_id: int) -> dict:
        """Get the schema from the Apicurio schema registry"""
        schema = self.session.get(
            f"{self.config.authentication.schema_registry_url}/apis/registry/v2/ids/globalIds/{global_id}",
            auth=(
                self.config.authentication.schema_registry_username,
                self.config.authentication.schema_registry_password,
            ),
        ).json()
        return schema

    def get_parsed_avro_schema(self, global_id: int) -> Schema:
        """Parse the schema from the Apicurio schema registry"""
        schema = self.get_apicurio_schema(global_id)
        schema["name"] = "Envelope"
        return parse(json.dumps(schema))

    def decode(self, msg_value, schema):
        reader = DatumReader(schema)
        message_bytes = io.BytesIO(msg_value)
        message_bytes.seek(9)
        decoder = BinaryDecoder(message_bytes)
        event_dict = reader.read(decoder)
        return event_dict

    def get_topic_schema(self) -> dict:
        """Get the global id of the schema for the topic"""

        # Read the first message of the topic to know the schema global id
        self.consumer.assign([TopicPartition(self.config.topic, 0, 0)])
        self.consumer.seek(TopicPartition(self.config.topic, 0, 0))

        found_message = False
        MAX_CONSUME_RETRY = 10

        for i in range(MAX_CONSUME_RETRY):
            message = self.consumer.consume(1, timeout=30)[0]

            if message.value():
                found_message = True
                break

        if not found_message:
            logger.error(f"No message found in the topic: {self.config.topic}")
            raise KafkaException("No message found in the topic")

        logger.info(f"Found message at offset {message.offset()}")

        global_id = self.parse_global_id_from_serialized_message(message.value())
        return self.get_parsed_avro_schema(global_id)

    def read_topic(self, pagination: dict = None) -> SourceIteration:

        schema = self.get_topic_schema()

        nb_partitions = self.get_number_of_partitions()

        # Store records
        records: List[SourceRecord] = []

        # Setup offset_pagination
        topic_offsets: TopicOffsets = (
            TopicOffsets.model_validate(pagination) if pagination else self.get_offset_partitions()
        )

        for i in range(nb_partitions):

            # Store encoded messages
            encoded_messages: List[bytes] = []

            # Set consumer offset params
            # TopicPartition (topic, partition, offset)
            self.consumer.assign([TopicPartition(self.config.topic, i, topic_offsets.get_partition_offset(i))])
            self.consumer.seek(TopicPartition(self.config.topic, i, topic_offsets.get_partition_offset(i)))

            # We read the maximum number of messages possible from the to_fetch offset -> to_fetch + batch_size
            encoded_messages.extend(self.consumer.consume(self.config.batch_size, timeout=self.config.consumer_timeout))

            for message in encoded_messages:
                if not message.value():
                    logger.warning(
                        f"Message for partition {i} and offset {message.offset()} and topic {self.config.topic} is empty, skipping."
                    )
                    continue

                try:
                    records.append(
                        SourceRecord(
                            id=f"part_{i}_offset_{message.offset()}",
                            data=self.decode(message.value(), schema),
                        )
                    )
                except Exception as e:
                    logger.error(f"Error while decoding message for partition {i}: {e} at offset {message.offset()}")
                    continue

            # Update the offset for the partition
            if len(encoded_messages) > 0:
                topic_offsets.set_partition_offset(i, encoded_messages[-1].offset() + 1)
            else:
                logger.warning(f"No new messages found for partition {i}")

        # In case we did not find new records
        if not records:
            logger.info("No new records found, stopping iteration")
            return SourceIteration(
                next_pagination={},
                records=[],
            )

        return SourceIteration(
            next_pagination=topic_offsets.model_dump(),
            records=records,
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        return self.read_topic(pagination)
