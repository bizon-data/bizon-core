from abc import ABC
from pydantic import BaseModel
from datetime import datetime

class AbstractState(BaseModel, ABC):
    stream_name: str
    iteration: int
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = None
