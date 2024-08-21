from typing import Literal, Optional

from pydantic import Field

from bizon.engine.backend.config import (
    AbstractBackendConfig,
    AbstractBackendConfigDetails,
    BackendTypes,
)


class BigQueryBackendConfigDetails(AbstractBackendConfigDetails):
    dataset_id: str
    dataset_location: Optional[str] = "US"
    project_id: str

    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryBackendConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.BIGQUERY]
    config: BigQueryBackendConfigDetails
