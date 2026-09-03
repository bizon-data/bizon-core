from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict, Field
from pytz import UTC

QUEUE_TERMINATION = "TERMINATION"

# Sent instead of QUEUE_TERMINATION when the producer stopped on an error. The consumer must
# drain and stop as usual, but must NOT publish: see AbstractQueueConsumer.process_queue_message.
QUEUE_TERMINATION_ERROR = "TERMINATION_ERROR"

# Both signals end the consumer loop; only QUEUE_TERMINATION publishes.
QUEUE_TERMINATION_SIGNALS = (QUEUE_TERMINATION, QUEUE_TERMINATION_ERROR)


@dataclass
class QueueMessage:
    iteration: int
    df_source_records: pl.DataFrame
    extracted_at: datetime = datetime.now(tz=UTC)
    pagination: Optional[dict] = None
    signal: Optional[str] = None


class QueueTypes(str, Enum):
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    PYTHON_QUEUE = "python_queue"


class AbastractQueueConfigDetails(BaseModel, ABC):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    max_nb_messages: int = Field(1_000_000, description="Maximum number of messages in the queue")

    queue: BaseModel = Field(..., description="Configuration of the queue")
    consumer: BaseModel = Field(..., description="Configuration of the consumer")


class AbstractQueueConfig(BaseModel, ABC):
    # Forbid extra keys in the model
    model_config = ConfigDict(extra="forbid")

    type: QueueTypes = Field(..., description="Type of the queue")
    config: AbastractQueueConfigDetails = Field(..., description="Configuration of the queue")
