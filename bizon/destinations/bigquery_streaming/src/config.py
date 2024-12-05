from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


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


class BigQueryStreamingConfigDetails(AbstractDestinationDetailsConfig):
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
    bq_max_rows_per_request: Optional[int] = Field(30000, description="Max rows per buffer streaming request.")


class BigQueryStreamingConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY_STREAMING]
    config: BigQueryStreamingConfigDetails
