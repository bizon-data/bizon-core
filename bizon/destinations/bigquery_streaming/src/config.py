from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class TimePartitioning(str, Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"


class BigQueryAuthentication(BaseModel):
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryConfigDetails(AbstractDestinationDetailsConfig):
    project_id: str
    dataset_id: str
    dataset_location: Optional[str] = "US"
    table_id: Optional[str] = Field(
        default=None, description="Table ID, if not provided it will be inferred from source name"
    )
    time_partitioning: Optional[TimePartitioning] = Field(
        default=TimePartitioning.DAY, description="BigQuery Time partitioning type"
    )
    authentication: Optional[BigQueryAuthentication] = None

    buffer_size: int = Field(default=0, description="Buffer size in MB")

    @field_validator("buffer_size", mode="after")
    def validate_buffer_size(cls, value: int) -> int:
        if value != 0:
            raise ValueError("Buffer size must be 0, we directly stream to BigQuery")
        return value


class BigQueryStreamingConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY_STREAMING]
    config: BigQueryConfigDetails
