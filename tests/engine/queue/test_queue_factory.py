from bizon.engine.queue.adapters.kafka.config import (
    KafkaConfig,
    KafkaConsumerConfig,
    KafkaQueueConfig,
)
from bizon.engine.queue.adapters.kafka.queue import KafkaQueue
from bizon.engine.queue.adapters.python_queue.config import PythonQueueConfig
from bizon.engine.queue.adapters.python_queue.queue import PythonQueue
from bizon.engine.queue.config import QueueConfig, QueueTypes
from bizon.engine.queue.queue import QueueFactory


def test_queue_factory():

    queue = QueueFactory().get_queue(config=QueueConfig(type=QueueTypes.PYTHON_QUEUE, config=PythonQueueConfig()))
    assert isinstance(queue, PythonQueue)


def test_default_python_queue():
    config = PythonQueueConfig()
    assert config.queue.max_size == 1000
    assert config.consumer.poll_interval == 1


def test_kafka_python_queue_default_config():
    config = KafkaConfig()
    assert config.queue.bootstrap_servers == "localhost:9092"
    assert config.queue.topic == "bizon"
    assert config.consumer.bootstrap_servers == ["localhost:9092"]
    assert config.consumer.group_id == "bizon"
    assert config.consumer.auto_offset_reset == "earliest"
    assert config.consumer.enable_auto_commit == True
    assert config.consumer.consumer_timeout_ms == 1000


def test_kafkaqueue_config():
    config = KafkaQueueConfig()
    assert config.bootstrap_servers == "localhost:9092"
    assert config.topic == "bizon"


def test_kafka_consumer_config():
    config = KafkaConsumerConfig()
    assert config.bootstrap_servers == ["localhost:9092"]
    assert config.group_id == "bizon"
    assert config.auto_offset_reset == "earliest"
    assert config.enable_auto_commit == True
    assert config.consumer_timeout_ms == 1000


def test_queue_factory_kafka():
    queue = QueueFactory().get_queue(config=QueueConfig(type=QueueTypes.KAFKA, config=KafkaConfig()))
    assert isinstance(queue, KafkaQueue)
