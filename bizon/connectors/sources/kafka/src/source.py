import re
import traceback
from collections.abc import Mapping
from datetime import datetime
from functools import cache
from typing import Any, List, Tuple

import orjson
from avro.schema import Schema, parse
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from confluent_kafka.cimpl import KafkaException as CimplKafkaException
from loguru import logger
from pydantic import BaseModel
from pytz import UTC

from bizon.source.auth.config import AuthType
from bizon.source.callback import AbstractSourceCallback
from bizon.source.config import SourceSyncModes
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

from .callback import KafkaSourceCallback
from .config import KafkaSourceConfig, MessageEncoding, SchemaRegistryType
from .decode import (
    Hashabledict,
    decode_avro_message,
    parse_global_id_from_serialized_message,
)

# Regex to detect lone Unicode surrogates in JSON text while preserving valid surrogate pairs.
# Valid pairs (high + low) are matched first and kept; lone surrogates are replaced.
_HIGH_SURROGATE = r"\\u[dD][89aAbB][0-9a-fA-F]{2}"
_LOW_SURROGATE = r"\\u[dD][cCdDeEfF][0-9a-fA-F]{2}"
_SURROGATE_RE = re.compile(f"({_HIGH_SURROGATE}{_LOW_SURROGATE})|(\\\\u[dD][89a-fA-F][0-9a-fA-F]{{2}})")


def _sanitize_lone_surrogates(text: str) -> str:
    """Replace lone Unicode surrogate escapes in JSON text with U+FFFD (replacement character).

    Valid surrogate pairs (high + low) are preserved. Only lone surrogates are replaced.
    """

    def _replace(m):
        if m.group(1):  # Valid surrogate pair - keep it
            return m.group(1)
        return "\\ufffd"  # Lone surrogate -> replacement char

    return _SURROGATE_RE.sub(_replace, text)


# Regex to match literal control characters (U+0000–U+001F) except tab, newline, carriage return
# which are valid whitespace outside JSON strings. Inside JSON strings all control chars must be escaped.
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _sanitize_control_characters(text: str) -> str:
    """Remove literal control characters (U+0000–U+001F) that are invalid in JSON strings.

    Tab (\\t), newline (\\n), and carriage return (\\r) are preserved as they are valid
    JSON whitespace. All other control characters are stripped.
    """
    return _CONTROL_CHAR_RE.sub("", text)


class SchemaNotFound(Exception):
    """Schema not found in the Schema Registry"""

    pass


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


def on_error(err: KafkaError):
    # Fires for client-level errors (incl. DNS resolve failures)
    if err.fatal():
        logger.error(f"Kafka client error: {err} | fatal={err.fatal()} retriable={err.retriable()}")
        raise KafkaException(err)
    else:
        logger.warning(f"Kafka client error: {err} | fatal={err.fatal()} retriable={err.retriable()}")


class KafkaSource(AbstractSource):
    def __init__(self, config: KafkaSourceConfig):
        super().__init__(config)

        self.config: KafkaSourceConfig = config

        # Ensure topics is always a list (not None)
        if self.config.topics is None:
            self.config.topics = []

        # Kafka consumer configuration.
        if self.config.authentication.type == AuthType.BASIC:
            self.config.consumer_config["sasl.mechanisms"] = "PLAIN"
            self.config.consumer_config["sasl.username"] = self.config.authentication.params.username
            self.config.consumer_config["sasl.password"] = self.config.authentication.params.password

        # Set the bootstrap servers and group id
        self.config.consumer_config["group.id"] = self.config.group_id
        self.config.consumer_config["bootstrap.servers"] = self.config.bootstrap_servers

        # Set the error callback
        self.config.consumer_config["error_cb"] = on_error

        # Consumer instance
        self.consumer = Consumer(self.config.consumer_config)

        # Map topic_name to destination_id
        self.topic_map = {topic.name: topic.destination_id for topic in self.config.topics}

    def set_streams_config(self, streams: list) -> None:
        """Configure Kafka topics from streams config.

        This method enriches self.config.topics from the streams configuration,
        ensuring that subsequent source instantiations (e.g., in init_job) have
        access to the topics without duplication in the YAML config.

        When a top-level 'streams' configuration is present, this method:
        1. Extracts Kafka topics from streams (topic field)
        2. Builds TopicConfig objects with destination_id from streams
        3. Populates self.config.topics if empty (modifies bizon_config.source in-place)
        4. Updates topic_map for record routing

        Args:
            streams: List of StreamConfig objects from BizonConfig.streams
        """
        from .config import TopicConfig

        # Extract topics from streams
        topics_from_streams = []
        streams_map = {}

        for stream in streams:
            if hasattr(stream.source, "topic") and stream.source.topic:
                topic_name = stream.source.topic
                streams_map[topic_name] = stream

                # Build TopicConfig from stream
                topic_config = TopicConfig(name=topic_name, destination_id=stream.destination.table_id)
                topics_from_streams.append(topic_config)

        # Populate self.config.topics from streams (modifies bizon_config.source in-place)
        # This ensures check_connection() and subsequent source instantiations have topics
        if not self.config.topics and topics_from_streams:
            self.config.topics = topics_from_streams
            logger.info(f"Kafka: Populated {len(topics_from_streams)} topics from streams config")
            for topic_config in topics_from_streams:
                logger.info(f"  - Topic: {topic_config.name} -> {topic_config.destination_id}")

        # Update topic_map with destination table_ids from streams
        for topic, stream_config in streams_map.items():
            self.topic_map[topic] = stream_config.destination.table_id

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

    def get_source_callback_instance(self) -> AbstractSourceCallback:
        """Return an instance of the source callback, used to commit the offsets of the iterations"""
        return KafkaSourceCallback(config=self.config)

    def check_connection(self) -> Tuple[bool | Any | None]:
        """Check the connection to the Kafka source"""

        # Validate that topics have been configured
        if not self.config.topics:
            error_msg = (
                "No topics configured. Either provide topics in source config or use streams configuration. "
                "If using streams config, ensure set_streams_config() is called before check_connection()."
            )
            logger.error(error_msg)
            return False, error_msg

        try:
            # Use a short timeout to avoid hanging on connection issues
            cluster_metadata = self.consumer.list_topics(timeout=self.config.consumer_timeout)
            topics = cluster_metadata.topics

            logger.info(f"Found: {len(topics)} topics")

            config_topics = [topic.name for topic in self.config.topics]

            # Display consumer config
            # We ignore the key sasl.password and sasl.username
            consumer_config = self.config.consumer_config.copy()
            consumer_config.pop("sasl.password", None)
            consumer_config.pop("sasl.username", None)
            logger.info(f"Consumer config: {consumer_config}")

            for topic in config_topics:
                if topic not in topics:
                    logger.error(f"Topic {topic} not found, available topics: {topics.keys()}")
                    return False, f"Topic {topic} not found"

                logger.info(f"Topic {topic} has {len(topics[topic].partitions)} partitions")

            return True, None

        except KafkaException as e:
            error_msg = f"Kafka connection failed: {e}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Connection check failed: {e}"
            logger.error(error_msg)
            return False, error_msg

    def get_number_of_partitions(self, topic: str) -> int:
        """Get the number of partitions for the topic"""
        return len(self.consumer.list_topics(timeout=self.config.consumer_timeout).topics[topic].partitions)

    def get_offset_partitions(self, topic: str) -> TopicOffsets:
        """Get the offsets for each partition of the topic"""

        partitions: Mapping[int, OffsetPartition] = {}

        for i in range(self.get_number_of_partitions(topic)):
            offsets = self.consumer.get_watermark_offsets(
                TopicPartition(topic, i), timeout=self.config.consumer_timeout
            )
            partitions[i] = OffsetPartition(first=offsets[0], last=offsets[1])

        return TopicOffsets(name=topic, partitions=partitions)

    def get_total_records_count(self) -> int | None:
        """Get the total number of records in the topic, sum of offsets for each partition"""
        # Init the consumer
        total_records = 0
        for topic in [topic.name for topic in self.config.topics]:
            total_records += self.get_offset_partitions(topic).total_offset
        return total_records

    @cache
    def get_schema_from_registry(self, global_id: int) -> Tuple[Hashabledict, Schema]:
        """Get the schema from the registry, return a hashable dict and an avro schema object"""

        # Apicurio
        if self.config.authentication.schema_registry_type == SchemaRegistryType.APICURIO:
            try:
                response = self.session.get(
                    f"{self.config.authentication.schema_registry_url}/apis/registry/v2/ids/globalIds/{global_id}",
                    auth=(
                        self.config.authentication.schema_registry_username,
                        self.config.authentication.schema_registry_password,
                    ),
                )
                if response.status_code == 404:
                    raise SchemaNotFound(f"Schema with global id {global_id} not found")

                schema_dict = response.json()

            except Exception as e:
                logger.error(traceback.format_exc())
                raise e

            # Add a name field to the schema as needed by fastavro
            schema_dict["name"] = "Envelope"

            # Convert the schema dict to an avro schema object
            avro_schema = parse(orjson.dumps(schema_dict))

            # Convert the schema dict to a hashable dict
            hashable_dict_schema = Hashabledict(schema_dict)

            return hashable_dict_schema, avro_schema

        else:
            raise ValueError(f"Schema registry type {self.config.authentication.schema_registry_type} not supported")

    def decode_avro(self, message: Message) -> Tuple[dict, dict]:
        """Decode the message as avro and return the parsed message and the schema"""
        global_id, nb_bytes_schema_id = parse_global_id_from_serialized_message(
            message=message.value(),
        )

        try:
            hashable_dict_schema, avro_schema = self.get_schema_from_registry(global_id=global_id)
        except SchemaNotFound as e:
            logger.error(
                f"Message on topic {message.topic()} partition {message.partition()} at offset {message.offset()} has a  SchemaID of {global_id} which is not found in Registry."
                f"message value: {message.value()}."
            )
            logger.error(traceback.format_exc())
            raise e

        return (
            decode_avro_message(
                message_value=message.value(),
                nb_bytes_schema_id=nb_bytes_schema_id,
                avro_schema=avro_schema,
            ),
            hashable_dict_schema,
        )

    def decode_utf_8(self, message: Message) -> Tuple[dict, dict]:
        """Decode the message as utf-8 and return the parsed message and the schema.

        Uses orjson for fast JSON parsing. If orjson rejects the message due to
        lone Unicode surrogate escape sequences (e.g., \\udf31) or unexpected control
        characters, sanitizes them and retries.
        """
        raw_text = message.value().decode("utf-8", errors="replace")
        if "\ufffd" in raw_text:
            logger.warning(
                f"Message on topic {message.topic()} partition {message.partition()} "
                f"offset {message.offset()} contains non-UTF-8 bytes. "
                f"Replaced invalid bytes with U+FFFD."
            )
        try:
            return orjson.loads(raw_text), {}
        except orjson.JSONDecodeError as e:
            error_msg = str(e).lower()
            sanitized = raw_text

            if "surrogate" in error_msg:
                sanitized = _sanitize_lone_surrogates(sanitized)
                if sanitized != raw_text:
                    logger.warning(
                        f"Message on topic {message.topic()} partition {message.partition()} "
                        f"offset {message.offset()} contains lone Unicode surrogates. "
                        f"Sanitized by replacing with U+FFFD."
                    )

            if "control character" in error_msg:
                sanitized = _sanitize_control_characters(sanitized)
                if sanitized != raw_text:
                    logger.warning(
                        f"Message on topic {message.topic()} partition {message.partition()} "
                        f"offset {message.offset()} contains invalid control characters. "
                        f"Sanitized by stripping them."
                    )

            if sanitized == raw_text:
                raise

            return orjson.loads(sanitized), {}

    def decode(self, message) -> Tuple[dict, dict]:
        """Decode the message based on the encoding type
        Returns parsed message and the schema
        """
        if self.config.message_encoding == MessageEncoding.AVRO:
            return self.decode_avro(message)

        elif self.config.message_encoding == MessageEncoding.UTF_8:
            return self.decode_utf_8(message)

        else:
            raise ValueError(f"Message encoding {self.config.message_encoding} not supported")

    def parse_encoded_messages(self, encoded_messages: list) -> List[SourceRecord]:
        """Parse the encoded Kafka messages and return a list of SourceRecord"""

        records = []

        for message in encoded_messages:
            MESSAGE_LOG_METADATA = (
                f"Message for topic {message.topic()} partition {message.partition()} and offset {message.offset()}"
            )

            if message.error():
                # If the message is too large, we skip it and update the offset
                if message.error().code() == KafkaError.MSG_SIZE_TOO_LARGE:
                    logger.error(
                        f"{MESSAGE_LOG_METADATA} is too large. "
                        "Raised MSG_SIZE_TOO_LARGE, if manually setting the offset, the message might not exist. Double-check in Confluent Cloud."
                    )

                logger.error(f"{MESSAGE_LOG_METADATA}: {message.error()}")
                raise KafkaException(message.error())

            # We skip tombstone messages
            if self.config.skip_message_empty_value and not message.value():
                logger.debug(f"{MESSAGE_LOG_METADATA} is empty, skipping.")
                continue

            # Parse message keys
            if message.key():
                try:
                    message_keys = orjson.loads(message.key().decode("utf-8"))
                except orjson.JSONDecodeError as e:
                    # We skip messages with invalid keys
                    if self.config.skip_message_invalid_keys:
                        logger.warning(f"{MESSAGE_LOG_METADATA} has an invalid key={message.key()}, skipping.")
                        # Skip the message
                        continue

                    logger.error(
                        f"{MESSAGE_LOG_METADATA}: Error while parsing message key: {e}, raw key: {message.key()}"
                    )
                    raise e
            else:
                message_keys = {}

            # Decode the message
            try:
                decoded_message, hashable_dict_schema = self.decode(message)

                data = {
                    "topic": message.topic(),
                    "offset": message.offset(),
                    "partition": message.partition(),
                    "timestamp": message.timestamp()[1],
                    "keys": message_keys,
                    "headers": (
                        {key: value.decode("utf-8") for key, value in message.headers()} if message.headers() else {}
                    ),
                    "value": decoded_message,
                    "schema": hashable_dict_schema,
                }

                records.append(
                    SourceRecord(
                        id=f"partition_{message.partition()}_offset_{message.offset()}",
                        timestamp=datetime.fromtimestamp(message.timestamp()[1] / 1000, tz=UTC),
                        data=data,
                        destination_id=self.topic_map[message.topic()],
                    )
                )

            except Exception as e:
                logger.error(
                    f"{MESSAGE_LOG_METADATA}: Error while decoding message: {e} "
                    f"with value: {message.value()} and key: {message.key()}"
                )

                # Skip message if configured to do so on decode errors
                if self.config.skip_message_on_decode_error:
                    logger.warning(
                        f"{MESSAGE_LOG_METADATA} could not be decoded, skipping due to skip_message_on_decode_error=True."
                    )
                    continue

                # Try to parse error message from the message value
                try:
                    message_raw_text = message.value().decode("utf-8")
                    logger.error(f"Parsed Kafka value: {message_raw_text}")
                except UnicodeDecodeError:
                    logger.error("Message value is not a valid UTF-8 string")

                # Try to parse error message from the message headers
                if message.headers():
                    try:
                        headers_dict = {key: value.decode("utf-8") for key, value in message.headers()}
                        logger.error(f"Parsed Kafka headers: {headers_dict}")
                    except UnicodeDecodeError as header_error:
                        logger.error(f"Some message headers are not valid UTF-8 strings: {header_error}")
                        logger.error(f"Raw message headers: {list(message.headers())}")
                else:
                    logger.error("Message headers are None or empty")

                logger.error(traceback.format_exc())
                raise e

        return records

    def read_topics_manually(self, pagination: dict = None) -> SourceIteration:
        """Read the topics manually, we use consumer.assign to assign to the partitions and get the offsets"""

        assert len(self.config.topics) == 1, "Only one topic is supported for manual mode"

        # We will use the first topic for the manual mode
        topic = self.config.topics[0]

        nb_partitions = self.get_number_of_partitions(topic=topic.name)

        # Setup offset_pagination
        self.topic_offsets = (
            TopicOffsets.model_validate(pagination) if pagination else self.get_offset_partitions(topic=topic.name)
        )

        self.consumer.assign(
            [
                TopicPartition(topic.name, partition, self.topic_offsets.get_partition_offset(partition))
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

        # Update the offset for the partition
        self.topic_offsets.set_partition_offset(encoded_messages[-1].partition(), encoded_messages[-1].offset() + 1)

        return SourceIteration(
            next_pagination=self.topic_offsets.model_dump(),
            records=records,
        )

    def read_topics_with_subscribe(self, pagination: dict = None) -> SourceIteration:
        """Read the topics with the subscribe method, pagination will not be used
        We rely on Kafka to get assigned to the partitions and get the offsets
        """
        topics = [topic.name for topic in self.config.topics]
        self.consumer.subscribe(topics)
        t1 = datetime.now()
        encoded_messages = self.consumer.consume(self.config.batch_size, timeout=self.config.consumer_timeout)
        logger.info(f"Kafka consumer read : {len(encoded_messages)} messages in {datetime.now() - t1}")
        records = self.parse_encoded_messages(encoded_messages)
        return SourceIteration(
            next_pagination={},
            records=records,
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.sync_mode == SourceSyncModes.STREAM:
            return self.read_topics_with_subscribe(pagination)
        else:
            return self.read_topics_manually(pagination)

    def commit(self):
        """Commit the offsets of the consumer"""
        try:
            self.consumer.commit(asynchronous=False)
        except CimplKafkaException as e:
            error_code = e.args[0].code() if e.args else None
            # Consumer has been evicted from the group. Further processing causes duplicate
            # writes since the new partition owner is already processing these messages.
            # Close gracefully and raise so the runner restarts the pod with a fresh consumer.
            if error_code in (KafkaError.ILLEGAL_GENERATION, KafkaError.UNKNOWN_MEMBER_ID):
                logger.error(
                    f"Kafka commit rejected - consumer evicted from group (code={error_code}): {e}. "
                    f"Closing consumer and exiting for clean restart."
                )
                try:
                    self.consumer.close()
                except Exception as close_err:
                    logger.warning(f"Error closing consumer during eviction handling: {close_err}")
                raise
            logger.error(f"Kafka exception occurred during commit: {e}")
            logger.info("Gracefully exiting without committing offsets due to Kafka exception")
