from enum import Enum
from typing import Optional

from pydantic import BaseModel

from bizon.common.config import ConsumerConfig


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class BigQueryAuth(BaseModel):
    dataset_id: str
    dataset_location: Optional[str] = "US"
    project_id: str
    gcs_buffer_bucket: str
    gcs_buffer_format: Optional[GCSBufferFormat] = GCSBufferFormat.PARQUET
    service_account_key: Optional[str] = ""  # If empty it will be infered


class BigQueryConfig(ConsumerConfig):
    name: str = "bigquery"
    config: BigQueryAuth
