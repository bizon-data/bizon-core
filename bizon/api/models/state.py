from abc import ABC
from datetime import datetime

from pydantic import BaseModel


class AbstractState(BaseModel, ABC):
    stream_name: str
    iteration: int
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = None
