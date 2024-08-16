from enum import Enum

from pydantic import BaseModel, Field


class BackendTypes(str, Enum):
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"


class BackendConfig(BaseModel):
    type: BackendTypes
    database: str
    schema: str
    echoEngine: bool = Field(False, description="Echo the engine in logs")
    syncCursorInDBEvery: int = Field(10, description="Number of iterations before syncing the cursor in the database")
