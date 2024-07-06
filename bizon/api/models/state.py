from abc import ABC
from pydantic import BaseModel
from datetime import datetime

class AbstractState(BaseModel, ABC):
    stream_name: str
    iteration: int
    created_at: datetime = datetime.now(datetime.UTC)
    updated_at: datetime = None
