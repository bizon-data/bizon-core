import os

from yaml import safe_load

from bizon.cli.utils import parse_from_yaml
from bizon.destinations.logger.src.destination import LoggerDestination
from bizon.engine.backend.config import BackendTypes
from bizon.engine.engine import RunnerFactory
from bizon.engine.queue.adapters.kafka.queue import KafkaQueue
from bizon.engine.queue.adapters.rabbitmq.queue import RabbitMQ


def test_parse_from_yaml():
    config = parse_from_yaml(os.path.abspath("tests/engine/dummy_pipeline_sqlite.yml"))
    runner = RunnerFactory.create_from_config_dict(config=config)
    assert runner.is_running is False


def test_parse_task_runner_python_queue():
    config_yaml = """
        name: test_job

        source:
            name: dummy
            stream: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            name: logger
            config:
                dummy: dummy

        engine:
            backend:
                type: sqlite_in_memory
                config:
                    database: not_used
                    schema: not_used
                    syncCursorInDBEvery: 400
            runner:
                log_level: INFO
        """
    config = safe_load(config_yaml)

    runner = RunnerFactory.create_from_config_dict(config=config)

    backend = runner.get_backend(bizon_config=runner.bizon_config)
    destination = runner.get_destination(bizon_config=runner.bizon_config, backend=backend, job_id="123")

    assert isinstance(destination, LoggerDestination)

    # Check if the backend per default is of type sqlite
    assert backend.type == BackendTypes.SQLITE_IN_MEMORY

    assert runner.bizon_config.engine.runner.log_level == "INFO"

    assert backend.config.syncCursorInDBEvery == 400


def test_parse_task_runner_kafka_queue():
    config_yaml = """
        name: test_job

        source:
            name: dummy
            stream: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            name: logger
            config:
                dummy: dummy

        engine:
            backend:
                type: sqlite_in_memory
                config:
                    database: not_used
                    schema: not_used
                    syncCursorInDBEvery: 200

            queue:
                type: kafka
                config:
                    queue:
                        bootstrap_server: localhost:9092

            runner:
                log_level: DEBUG
        """
    config = safe_load(config_yaml)
    runner = RunnerFactory.create_from_config_dict(config=config)

    backend = runner.get_backend(bizon_config=runner.bizon_config)
    destination = runner.get_destination(bizon_config=runner.bizon_config, backend=backend, job_id="123")
    queue = runner.get_queue(bizon_config=runner.bizon_config)

    assert isinstance(destination, LoggerDestination)

    assert backend.type == BackendTypes.SQLITE_IN_MEMORY

    assert isinstance(queue, KafkaQueue)
    assert queue.config.queue.bootstrap_server == "localhost:9092"


def test_parse_task_runner_rabbitmq_queue():
    config_yaml = """

        name: test_job

        source:
            name: dummy
            stream: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            name: logger
            config:
                dummy: dummy

        engine:
            backend:
                type: sqlite_in_memory
                config:
                    database: not_used
                    schema: not_used
                    syncCursorInDBEvery: 200

            queue:
                type: rabbitmq
                config:
                    queue:
                        host: localhost
                        port: 5672
                        queue_name: bizon
                    consumer:
                        poll_interval: 1
            runner:
                log_level: DEBUG
        """
    config = safe_load(config_yaml)
    runner = RunnerFactory.create_from_config_dict(config=config)

    assert runner.bizon_config.name == "test_job"

    backend = runner.get_backend(bizon_config=runner.bizon_config)
    destination = runner.get_destination(bizon_config=runner.bizon_config, backend=backend, job_id="123")
    queue = runner.get_queue(bizon_config=runner.bizon_config)

    assert isinstance(destination, LoggerDestination)

    assert backend.type == BackendTypes.SQLITE_IN_MEMORY

    assert isinstance(queue, RabbitMQ)
    assert queue.config.queue.queue_name == "bizon"
    assert queue.config.queue.host == "localhost"
    assert queue.config.queue.port == 5672
    assert queue.config.consumer.poll_interval == 1
