from typing import Literal, Optional

from pydantic import BaseModel, Field

from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryRecordSchemaConfig,
    TimePartitioning,
    TimePartitioningWindow,
)
from bizon.connectors.destinations.bigquery.src.table_naming import BIZON_TABLE_PREFIX
from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)

# `TimePartitioning` and `TimePartitioningWindow` used to be declared here, byte-identically to the
# copies in the batch and legacy-streaming configs. They now live in the batch config (already this
# package's shared BigQuery module) and are re-exported so existing imports from here keep working.
__all__ = [
    "BigQueryAuthentication",
    "BigQueryStreamingV2Config",
    "BigQueryStreamingV2ConfigDetails",
    "TimePartitioning",
    "TimePartitioningWindow",
]


class BigQueryAuthentication(BaseModel):
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryStreamingV2ConfigDetails(AbstractDestinationDetailsConfig):
    project_id: str
    dataset_id: str
    dataset_location: Optional[str] = "US"
    time_partitioning: Optional[TimePartitioning] = Field(
        default=TimePartitioning(type=TimePartitioningWindow.DAY, field="_bizon_loaded_at"),
        description="BigQuery Time partitioning type",
    )
    enforce_partitioning: bool = Field(
        default=False,
        description="On a full refresh, drop the destination table when its partitioning does not match the "
        "configured spec, so it is recreated partitioned. BigQuery cannot change an existing table's "
        "partitioning in place, so this is the only way to fix a legacy table. Off by default: the table is "
        "briefly absent, and dropping it discards table-level metadata (description, labels, ACLs, policy tags).",
    )
    authentication: Optional[BigQueryAuthentication] = None
    bq_max_rows_per_request: Optional[int] = Field(
        5000,
        description="Max rows per buffer streaming request. Must not exceed 10000.",
        le=10000,
    )
    record_schemas: Optional[list[BigQueryRecordSchemaConfig]] = Field(
        default=None, description="Schema for the records. Required if unnest is set to true."
    )
    table_prefix: str = Field(
        default=BIZON_TABLE_PREFIX,
        description="Prefix applied to auto-generated table names (when destination_id is not set). "
        "An existing legacy (unprefixed) table is reused if present. Set to '' to disable.",
    )


class BigQueryStreamingV2Config(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY_STREAMING_V2]
    alias: str = "bigquery"
    config: BigQueryStreamingV2ConfigDetails
