from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class BigQueryConfig(BaseModel):
    dataset_id: str
    dataset_location: Optional[str] = "US"
    project_id: str
    gcs_buffer_bucket: str
    gcs_buffer_format: Optional[GCSBufferFormat] = GCSBufferFormat.PARQUET

    service_account_key: Optional[str] = Field(
        description="Service Accouner Key JSON string. If empty it will be infered",
        default="",
    )
