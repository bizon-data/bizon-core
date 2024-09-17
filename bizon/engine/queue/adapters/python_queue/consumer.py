from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueue,
    AbstractQueueConsumer,
    QueueMessage,
)

from .config import PythonQueueConfig


class PythonQueueConsumer(AbstractQueueConsumer):
    def __init__(self, config: PythonQueueConfig, queue: AbstractQueue, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.queue = queue

    def run(self) -> None:
        while True:
            message: dict = self.queue.get()
            queue_message = QueueMessage.model_validate(message)

            if queue_message.signal == QUEUE_TERMINATION:
                logger.info("Received termination signal, waiting for destination to close gracefully ...")
                self.destination.write_records_and_update_cursor(
                    source_records=queue_message.source_records,
                    iteration=queue_message.iteration,
                    extracted_at=queue_message.extracted_at,
                    last_iteration=True,
                )
                break

            if not queue_message.source_records:
                raise ValueError("Received empty message, with no records")

            self.destination.write_records_and_update_cursor(
                source_records=queue_message.source_records,
                iteration=queue_message.iteration,
                extracted_at=queue_message.extracted_at,
            )

            logger.info(f"Consumed data from queue: {len(queue_message.source_records)}")
