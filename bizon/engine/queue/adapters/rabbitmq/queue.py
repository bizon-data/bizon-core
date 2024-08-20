import json
from typing import List

import pika
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueue,
    AbstractQueueConsumer,
    QueueMessage,
)
from bizon.source.models import SourceRecord

from .config import RabbitMQConfigDetails
from .consumer import RabbitMQConsumer


class RabbitMQ(AbstractQueue):
    def __init__(self, config: RabbitMQConfigDetails) -> None:
        super().__init__(config)
        self.config: RabbitMQConfigDetails = config

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.config.queue.host))
        channel = self.connection.channel()
        channel.queue_declare(queue=self.config.queue.queue_name)
        self.channel = channel

    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
        return RabbitMQConsumer(config=self.config, destination=destination)

    def put(self, source_records: List[SourceRecord], index: int, signal: str = None):
        self.channel.basic_publish(
            exchange=self.config.queue.exchange,
            routing_key=self.config.queue.queue_name,
            body=json.dumps(QueueMessage(index=index, source_records=source_records, signal=signal).model_dump()),
        )

    def get(self) -> QueueMessage:
        raise NotImplementedError(
            "RabbitMQ does not support getting messages from the queue, directly use callback in consumer."
        )

    def terminate(self) -> bool:
        self.put(source_records=[], index=0, signal=QUEUE_TERMINATION)

        for consumer_tag in self.channel.consumer_tags:
            self.channel.basic_cancel(consumer_tag=consumer_tag)

        logger.info("Sent termination signal to destination.")
        return True
