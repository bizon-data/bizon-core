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
from .runner.config import RunnerConfig, RunnerFuturesConfig, RunnerTypes


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
        default=SQLiteSQLAlchemyConfig(
            type=BackendTypes.SQLITE,
            config=SQLiteConfigDetails(
                database="bizon",
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
                queue=PythonQueueQueueConfig(max_size=0),
                consumer=PythonQueueConsumerConfig(poll_interval=2),
            ),
        ),
        discriminator="type",
    )

    runner: RunnerConfig = Field(
        description="Runner to use for the pipeline",
        default=RunnerConfig(
            type=RunnerTypes.THREAD,
            config=RunnerFuturesConfig(),
            log_level="INFO",
        ),
    )
