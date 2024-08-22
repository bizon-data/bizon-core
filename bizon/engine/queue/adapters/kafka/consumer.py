import json

from kafka import KafkaConsumer
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueueConsumer,
    QueueMessage,
)

from .config import KafkaConfigDetails


class KafkaConsumer_(AbstractQueueConsumer):
    def __init__(self, config: KafkaConfigDetails, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.config: KafkaConfigDetails = config
        self.consumer = self.get_consumer()
        self.consumer.subscribe(self.config.queue.topic)

    def get_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self.config.queue.bootstrap_server,
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

                if queue_message.signal == QUEUE_TERMINATION:
                    logger.info("Received termination signal, waiting for destination to close gracefully ...")
                    self.destination.write_records_and_update_cursor(
                        source_records=queue_message.source_records,
                        iteration=queue_message.iteration,
                        last_iteration=True,
                    )
                    break

                if not queue_message.source_records:
                    raise ValueError("Received empty message, with no records")

                self.destination.write_records_and_update_cursor(
                    source_records=queue_message.source_records,
                    iteration=queue_message.iteration,
                )

                logger.info(f"Consumed data from queue: {len(queue_message.source_records)}")

        except Exception as e:
            logger.error(f"Error occurred while consuming messages: {e}")
        finally:
            self.consumer.close()
