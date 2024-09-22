from queue import Queue

import pytest

from bizon.common.models import SyncMetadata
from bizon.engine.queue.adapters.kafka.config import (
    KafkaConfig,
    KafkaConfigDetails,
    KafkaConsumerConfig,
    KafkaQueueConfig,
)
from bizon.engine.queue.adapters.kafka.queue import KafkaQueue
from bizon.engine.queue.adapters.python_queue.config import (
    PythonQueueConfig,
    PythonQueueConfigDetails,
    PythonQueueConsumerConfig,
    PythonQueueQueueConfig,
)
from bizon.engine.queue.adapters.python_queue.queue import PythonQueue
from bizon.engine.queue.adapters.rabbitmq.config import (
    RabbitMQConfig,
    RabbitMQConfigDetails,
    RabbitMQConsumerConfig,
    RabbitMQQueueConfig,
)
from bizon.engine.queue.adapters.rabbitmq.queue import RabbitMQ
from bizon.engine.queue.config import QueueTypes
from bizon.engine.queue.queue import QueueFactory


def test_queue_factory():

    config = PythonQueueConfig(
        type=QueueTypes.PYTHON_QUEUE,
        config=PythonQueueConfigDetails(
            queue=PythonQueueQueueConfig(max_size=1000),
            consumer=PythonQueueConsumerConfig(poll_interval=1),
        ),
    )

    kwargs = {"queue": Queue(maxsize=3000)}

    queue = QueueFactory().get_queue(config=config, **kwargs)
    assert isinstance(queue, PythonQueue)


def test_queue_factory_kafka():

    config = KafkaConfig(
        type=QueueTypes.KAFKA,
        config=KafkaConfigDetails(
            queue=KafkaQueueConfig(
                bootstrap_server="localhost:9092",
                topic="bizon",
            ),
            consumer=KafkaConsumerConfig(
                group_id="bizon",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            ),
        ),
    )

    queue = QueueFactory().get_queue(config=config)
    assert isinstance(queue, KafkaQueue)


def test_queue_rabbitmq():

    config = RabbitMQConfig(
        type=QueueTypes.RABBITMQ,
        config=RabbitMQConfigDetails(
            queue=RabbitMQQueueConfig(
                host="localhost",
                port=5672,
                queue_name="bizon",
                exchange="",
            ),
            consumer=RabbitMQConsumerConfig(
                poll_interval=1,
            ),
        ),
    )

    queue = QueueFactory().get_queue(config=config)
    assert isinstance(queue, RabbitMQ)
