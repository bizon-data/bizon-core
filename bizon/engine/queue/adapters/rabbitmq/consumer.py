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

    def run(self) -> None:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.config.queue.host, port=self.config.queue.port)
        )

        channel = connection.channel()

        channel.queue_declare(queue=self.config.queue.queue_name)

        for method_frame, properties, body in channel.consume(self.config.queue.queue_name):

            queue_message = QueueMessage.model_validate_json(body)
            if queue_message.signal == QUEUE_TERMINATION:
                logger.info("Received termination signal, waiting for destination to close gracefully ...")
                self.destination.write_records_and_update_cursor(
                    source_records=queue_message.source_records,
                    iteration=queue_message.iteration,
                    extracted_at=queue_message.extracted_at,
                    pagination=queue_message.pagination,
                    last_iteration=True,
                )
                channel.queue_delete(queue=self.config.queue.queue_name)
                channel.close()
                break

            if not queue_message.source_records:
                raise ValueError("Received empty message, with no records")

            self.destination.write_records_and_update_cursor(
                source_records=queue_message.source_records,
                iteration=queue_message.iteration,
                extracted_at=queue_message.extracted_at,
                pagination=queue_message.pagination,
            )
            logger.info(f"Consumed data from queue: {len(queue_message.source_records)}")
