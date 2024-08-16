from typing import Optional

from pydantic import BaseModel


class LoggerDestinationConfig(BaseModel):
    dummy: Optional[str] = "bizon"
