from abc import ABC
from enum import Enum
import polars as pl
from datetime import datetime
from typing import Optional
from pytz import UTC

from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field

QUEUE_TERMINATION = "TERMINATION"

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
