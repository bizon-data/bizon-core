from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field

from bizon.destinations.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class NormalizationType(str, Enum):
    TABULAR = "tabular"
    NONE = "none"


class BigQueryNormalizationConfig(BaseModel):
    type: NormalizationType = Field(description="Normalization type")


class BigQueryConfigDetails(AbstractDestinationDetailsConfig):
    project_id: str
    dataset_id: str
    dataset_location: Optional[str] = "US"
    table_id: Optional[str] = Field(
        default=None, description="Table ID, if not provided it will be inferred from source name"
    )
    gcs_buffer_bucket: str
    gcs_buffer_format: Optional[GCSBufferFormat] = GCSBufferFormat.PARQUET
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )
    normalization: Optional[BigQueryNormalizationConfig] = Field(
        description="Normalization configuration, by default no normalization is applied",
        default=BigQueryNormalizationConfig(type=NormalizationType.NONE),
    )


class BigQueryConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY]
    buffer_size: Optional[int] = 2000
    config: BigQueryConfigDetails
