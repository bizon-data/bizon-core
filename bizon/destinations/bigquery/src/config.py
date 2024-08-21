from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field

from bizon.destinations.config import AbstractDestinationConfig, DestinationTypes


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class BigQueryConfigDetails(BaseModel):
    dataset_id: str
    dataset_location: Optional[str] = "US"
    project_id: str
    gcs_buffer_bucket: str
    gcs_buffer_format: Optional[GCSBufferFormat] = GCSBufferFormat.PARQUET

    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY]
    config: BigQueryConfigDetails
