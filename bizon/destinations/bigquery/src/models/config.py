from typing import Optional

from pydantic import BaseModel

from bizon.api.models.config import DestinationConfig


class BigQueryAuth(BaseModel):
    dataset_id: str
    dataset_location: Optional[str] = "US"
    project_id: str
    gcs_buffer_bucket: str
    gcs_buffer_format: Optional[str] = "parquet"
    service_account_key: Optional[str] = ""  # If empty it will be infered


class BigQueryConfig(DestinationConfig):
    name: str = "bigquery"
    config: BigQueryAuth
