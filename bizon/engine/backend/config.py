from abc import ABC
from enum import Enum

from pydantic import BaseModel, Field


class BackendTypes(str, Enum):
    POSTGRES = "postgres"
    BIGQUERY = "bigquery"

    # For testing purposes or local if no database available
    SQLITE = "sqlite"  # Store in a file bizon.db
    SQLITE_IN_MEMORY = "sqlite_in_memory"  # Only for testing purposes, no persistence


class AbstractBackendConfigDetails(BaseModel, ABC):
    database: str = Field(..., description="Database name")
    schema_name: str = Field(..., description="Schema name", alias="schema")
    syncCursorInDBEvery: int = Field(10, description="Number of iterations before syncing the cursor in the database")


class AbstractBackendConfig(BaseModel, ABC):
    type: BackendTypes = Field(..., description="Type of the backend")
    config: AbstractBackendConfigDetails = Field(..., description="Configuration details for the backend")
