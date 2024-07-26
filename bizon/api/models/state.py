from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class FetchStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


class AbstractState(BaseModel, ABC):
    stream_name: str
    created_at: datetime = datetime.utcnow()
    records_fetched: int
    fetch_status: FetchStatus
    is_finished: bool = False

    # To implement based on the source
    paging: Any = None
