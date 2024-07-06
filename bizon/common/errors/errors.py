from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class FailureType(str, Enum):
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"


class ErrorTraceMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(..., description="A user-friendly message that indicates the cause of the error")
    internal_message: Optional[str] = Field(None, description="The internal error that caused the failure")
    stack_trace: Optional[str] = Field(None, description="The full stack trace of the error")
    failure_type: Optional[FailureType] = Field(None, description="The type of error")
