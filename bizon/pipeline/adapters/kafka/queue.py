import json
from typing import List

from kafka import KafkaProducer
from loguru import logger

from bizon.pipeline.queue import AbstractQueue, QueueMessage

from .config import KafkaConfig


class KafkaQueue(AbstractQueue):
    def __init__(self, config: KafkaConfig) -> None:
        super().__init__()
        self.config = config
        self.producer = self.get_kafka_producer()

    def get_kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )

    @staticmethod
    def on_success(metadata):
        logger.info(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

    @staticmethod
    def on_error(e):
        logger.error(f"Error sending message: {e}")

    def put(self, records: List[dict], index: int, signal: str = None):
        future = self.producer.send(
            topic=self.config.topic,
            value=QueueMessage(index=index, records=records, signal=signal).model_dump(),
        )
        future.add_callback(self.on_success)
        future.add_errback(self.on_error)
        self.producer.flush()

    def get(self) -> QueueMessage:
        raise NotImplementedError("Kafka does not support getting messages from here. Use KafkaConsumer instead.")

    def terminate(self):
        self.producer.close()
        logger.debug("Terminating Kafka producer ...")
