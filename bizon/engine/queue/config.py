from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict, Field
from pytz import UTC

QUEUE_TERMINATION = "TERMINATION"

# Sent by the producer when it stops because of an ERROR rather than because the
# source was exhausted. The consumer must NOT treat this as the last iteration:
# doing so would finalize the destination (for BigQuery, a WRITE_TRUNCATE swap of
# the temp table into the production table) and mark the stream job SUCCEEDED on
# top of a partial extract.
QUEUE_TERMINATION_ERROR = "TERMINATION_ERROR"


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
