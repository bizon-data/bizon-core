import multiprocessing.synchronize
import threading
from typing import Union

import pika
import pika.connection
from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import QueueMessage
from bizon.engine.queue.queue import AbstractQueueConsumer

from .config import RabbitMQConfigDetails


class RabbitMQConsumer(AbstractQueueConsumer):
    def __init__(self, config: RabbitMQConfigDetails, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.config: RabbitMQConfigDetails = config

    def run(self, stop_event: Union[threading.Event, multiprocessing.synchronize.Event]) -> None:

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config.queue.host,
                port=self.config.queue.port,
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue=self.config.queue.queue_name)

        for method_frame, properties, body in channel.consume(self.config.queue.queue_name):

            # Handle kill signal from the runner
            if stop_event.is_set():
                logger.info("Stop event is set, closing consumer ...")
                return PipelineReturnStatus.KILLED_BY_RUNNER

            queue_message = QueueMessage(**body)

            status = self.process_queue_message(queue_message)

            if status != PipelineReturnStatus.RUNNING:
                channel.queue_delete(queue=self.config.queue.queue_name)
                channel.close()
                return status
