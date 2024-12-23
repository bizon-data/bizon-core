import json
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict, Field
from pytz import UTC

QUEUE_TERMINATION = "TERMINATION"


@dataclass
class QueueMessage:
    iteration: int
    df_source_records: pl.DataFrame
    extracted_at: datetime = datetime.now(tz=UTC)
    pagination: Optional[dict] = None
    signal: Optional[str] = None

    def dict(self) -> dict:
        _dict = self.__dict__.copy()
        _dict["df_source_records"] = self.df_source_records.to_dict()
        return _dict

    def to_json(self) -> str:
        return json.dumps(self.dict())

    @classmethod
    def from_dict(cls, data: dict):
        data["df_source_records"] = pl.DataFrame(data["df_source_records"])
        return cls(**data)

    @classmethod
    def from_json(cls, data: str):
        return cls.from_dict(json.loads(data))


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
