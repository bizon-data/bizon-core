from typing import Union

from pydantic import BaseModel, ConfigDict, Field

from .backend.adapters.sqlalchemy.config import (
    BigQuerySQLAlchemyConfig,
    PostgresSQLAlchemyConfig,
    SQLiteConfigDetails,
    SQLiteInMemoryConfig,
    SQLiteSQLAlchemyConfig,
)
from .backend.config import BackendTypes
from .queue.adapters.kafka.config import KafkaConfig
from .queue.adapters.python_queue.config import (
    PythonQueueConfig,
    PythonQueueConfigDetails,
    PythonQueueConsumerConfig,
    PythonQueueQueueConfig,
)
from .queue.adapters.rabbitmq.config import RabbitMQConfig
from .queue.config import QueueTypes
from .runners.config import RunnerConfig, RunnerTypes, ThreadsConfig


class EngineConfig(BaseModel):

    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    backend: Union[
        PostgresSQLAlchemyConfig,
        SQLiteInMemoryConfig,
        SQLiteSQLAlchemyConfig,
        BigQuerySQLAlchemyConfig,
    ] = Field(
        description="Configuration for the backend",
        default=SQLiteInMemoryConfig(
            type=BackendTypes.SQLITE_IN_MEMORY,
            config=SQLiteConfigDetails(
                database="NOT_USED_IN_SQLITE",
                schema="NOT_USED_IN_SQLITE",
            ),
            syncCursorInDBEvery=2,
        ),
        discriminator="type",
    )

    queue: Union[
        KafkaConfig,
        RabbitMQConfig,
        PythonQueueConfig,
    ] = Field(
        description="Configuration for the queue",
        default=PythonQueueConfig(
            type=QueueTypes.PYTHON_QUEUE,
            config=PythonQueueConfigDetails(
                queue=PythonQueueQueueConfig(max_size=1000),
                consumer=PythonQueueConsumerConfig(poll_interval=2),
            ),
        ),
        discriminator="type",
    )

    runner: RunnerConfig = Field(
        description="Runner to use for the pipeline",
        default=RunnerConfig(
            type=RunnerTypes.THREADS,
            config=ThreadsConfig(
                consumer_start_delay=2,
            ),
            log_level="INFO",
        ),
    )
