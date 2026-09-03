from typing import Literal, Optional

from loguru import logger
from pydantic import BaseModel, Field, model_validator

from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryRecordSchemaConfig,
    TimePartitioning,
    TimePartitioningWindow,
    resolve_partition_field,
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
    # default_factory, not default=TimePartitioning(...): pydantic v2 does not copy a BaseModel
    # passed as `default`, so every config built in the process would share one instance -- and the
    # validator below rewrites it. It also keeps `model_fields_set` truthful, which is how
    # resolve_partition_field() tells an explicit `field` from the inherited default.
    time_partitioning: Optional[TimePartitioning] = Field(
        default_factory=TimePartitioning,
        description="BigQuery time partitioning. Accepts `{type, field}` or a bare window (`DAY`). "
        "Set to null to disable partitioning entirely.",
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

    @model_validator(mode="after")
    def _resolve_partition_field(self) -> "BigQueryStreamingV2ConfigDetails":
        """Resolve the partition field, and *warn* -- never raise -- when it cannot work.

        In `stream` sync mode -- what this destination is normally run in -- `finalize()` returns
        immediately, so the only thing that ever applies `time_partitioning` is `_ensure_table()`,
        and once the table exists BigQuery answers `create_table` with `Conflict` and discards the
        spec. A pipeline can therefore have carried a wrong field for as long as its table has
        existed and be working perfectly; raising here would stop it on upgrade for a setting that
        has never had any effect.

        The destination raises instead of warning wherever the spec does reach a table: creating one
        that does not exist yet, and the full-refresh publish DDL when it declares the configured
        spec. Those are the cases BigQuery itself rejects.
        """
        self.time_partitioning, problem = resolve_partition_field(self)

        if problem:
            logger.warning(f"{problem} Partitioning is only applied when the table is created.")

        return self


class BigQueryStreamingV2Config(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY_STREAMING_V2]
    alias: str = "bigquery"
    config: BigQueryStreamingV2ConfigDetails
