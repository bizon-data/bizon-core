import json

import pika
import pika.connection
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueueConsumer,
    QueueMessage,
)

from .config import RabbitMQConfigDetails


class RabbitMQConsumer(AbstractQueueConsumer):
    def __init__(self, config: RabbitMQConfigDetails, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.config: RabbitMQConfigDetails = config
        self._channel = None

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, ch, method, properties, body):
        queue_message = QueueMessage.model_validate(json.loads(body))

        if queue_message.signal == QUEUE_TERMINATION:
            logger.info("Received termination signal, waiting for destination to close gracefully ...")
            return

        if not queue_message.source_records:
            raise ValueError("Received empty message, with no records")

        self.destination.write_records(queue_message.source_records)
        logger.info(f"Consumed data from queue: {len(queue_message.source_records)}")

    def run(self) -> None:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.queue.host, port=self.config.queue.port)
        )

        self._channel = connection.channel()

        self._channel.queue_declare(queue=self.config.queue.queue_name)

        self._channel.basic_consume(
            queue=self.config.queue.queue_name,
            on_message_callback=self.on_message,
            auto_ack=True,
        )

        self._channel.start_consuming()
