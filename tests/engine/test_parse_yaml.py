import os

from yaml import safe_load

from bizon.cli.parser import parse_from_yaml
from bizon.engine.runner import RunnerFactory


def test_parse_from_yaml():
    config = parse_from_yaml(os.path.abspath("tests/engine/dummy_pipeline.yml"))
    runner = RunnerFactory.create_from_config_dict(config=config)
    assert runner.is_running is False


def test_parse_task_runner_python_queue():
    config_yaml = """
        source:
            source_name: dummy
            stream_name: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            # Must have the bigquery.dataEditor and storage.objectUser on the supplied dataset and bucket
            name: bigquery
            config:
                dataset_id: bizon_test
                dataset_location: US
                project_id: <MY_PROJECT_ID>
                gcs_buffer_bucket: bizon-buffer
                gcs_buffer_format: parquet

        engine:
            backend:
                type: sqlite
                database: not_used
                schema: not_used
                syncCursorInDBEvery: 400
            runner:
                log_level: INFO
        """
    config = safe_load(config_yaml)
    runner = RunnerFactory.create_from_config_dict(config=config)

    from bizon.destinations.bigquery.src.destination import BigQueryDestination
    from bizon.engine.backend.config import BackendTypes

    assert isinstance(runner.destination, BigQueryDestination)

    # Check if the backend per default is of type sqlite
    assert runner.backend.config.type == BackendTypes.SQLITE

    assert runner.bizon_config.engine.runner.log_level == "INFO"

    assert runner.backend.config.syncCursorInDBEvery == 400


def test_parse_task_runner_kafka_queue():
    config_yaml = """
        source:
            source_name: dummy
            stream_name: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            # Must have the bigquery.dataEditor and storage.objectUser on the supplied dataset and bucket
            name: bigquery
            config:
                dataset_id: bizon_test
                dataset_location: US
                project_id: <MY_PROJECT_ID>
                gcs_buffer_bucket: bizon-buffer
                gcs_buffer_format: parquet

        engine:
            backend:
                type: sqlite
                database: not_used
                schema: not_used
                syncCursorInDBEvery: 200

            queue:
                type: kafka
                config:
                    queue:
                        bootstrap_servers: localhost:9092
                    consumer:
                        bootstrap_servers:
                            - localhost:9092

            runner:
                log_level: DEBUG
        """
    config = safe_load(config_yaml)
    runner = RunnerFactory.create_from_config_dict(config=config)

    from bizon.destinations.bigquery.src.destination import BigQueryDestination
    from bizon.engine.backend.config import BackendTypes
    from bizon.engine.queue.adapters.kafka.queue import KafkaQueue

    assert isinstance(runner.destination, BigQueryDestination)

    assert runner.backend.config.type == BackendTypes.SQLITE

    assert isinstance(runner.queue, KafkaQueue)
    assert runner.queue.config.queue.bootstrap_servers == "localhost:9092"


def test_parse_task_runner_rabbitmq_queue():
    config_yaml = """
        source:
            source_name: dummy
            stream_name: creatures
            authentication:
                type: api_key
                params:
                    token: dummy_key

        destination:
            # Must have the bigquery.dataEditor and storage.objectUser on the supplied dataset and bucket
            name: bigquery
            config:
                dataset_id: bizon_test
                dataset_location: US
                project_id: <MY_PROJECT_ID>
                gcs_buffer_bucket: bizon-buffer
                gcs_buffer_format: parquet

        engine:
            backend:
                type: sqlite
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

    from bizon.destinations.bigquery.src.destination import BigQueryDestination
    from bizon.engine.backend.config import BackendTypes
    from bizon.engine.queue.adapters.rabbitmq.queue import RabbitMQ

    assert isinstance(runner.destination, BigQueryDestination)

    assert runner.backend.config.type == BackendTypes.SQLITE

    assert isinstance(runner.queue, RabbitMQ)
    assert runner.queue.config.queue.queue_name == "bizon"
    assert runner.queue.config.queue.host == "localhost"
    assert runner.queue.config.queue.port == 5672
    assert runner.queue.config.consumer.poll_interval == 1
