import pika
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.queue.queue import QUEUE_TERMINATION, AbstractQueue, QueueMessage

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

    def put_queue_message(self, queue_message: QueueMessage):
        self.channel.basic_publish(
            exchange=self.config.queue.exchange,
            routing_key=self.config.queue.queue_name,
            body=queue_message.model_dump_json(),
        )

    def get(self) -> QueueMessage:
        raise NotImplementedError(
            "RabbitMQ does not support getting messages from the queue, directly use callback in consumer."
        )

    def terminate(self, iteration: int) -> bool:
        self.put(source_records=[], iteration=iteration, signal=QUEUE_TERMINATION)
        logger.info("Sent termination signal to destination.")
        return True
