import json

from kafka import KafkaConsumer
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import AbstractQueueConsumer, QueueMessage

from .config import KafkaConfig


class KafkaConsumer_(AbstractQueueConsumer):
    def __init__(self, config: KafkaConfig, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.config: KafkaConfig = config
        self.consumer = self.get_consumer()
        self.consumer.subscribe(self.config.queue.topic)

    def get_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self.config.consumer.bootstrap_servers,
            group_id=self.config.consumer.group_id,
            auto_offset_reset=self.config.consumer.auto_offset_reset,
            enable_auto_commit=self.config.consumer.enable_auto_commit,
            consumer_timeout_ms=self.config.consumer.consumer_timeout_ms,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def run(self):
        try:
            for message in self.consumer:
                logger.debug(f"Consuming message on topic: {message.partition}|{message.offset} key: {message.key}")
                queue_message = QueueMessage.model_validate(message.value)
                self.destination.write_records(records=queue_message.records)
        except Exception as e:
            logger.error(f"Error occurred while consuming messages: {e}")
        finally:
            self.consumer.close()
