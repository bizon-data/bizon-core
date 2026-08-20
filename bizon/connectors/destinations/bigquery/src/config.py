from enum import Enum
from typing import Any, Literal, Optional

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationColumn,
    DestinationTypes,
)

from .table_naming import BIZON_TABLE_PREFIX

# NOTE: this module is imported by `bizon.common.models` on every CLI invocation (pydantic needs the
# destination config classes to build its discriminated union), so it must NOT import
# `google.cloud.bigquery`. Importing it here is what made `pip install bizon` with no extras produce
# an unusable CLI in 0.5.1. Anything needing the BigQuery client lives in `partitioning.py` instead.

DEFAULT_PARTITION_FIELD = "_bizon_loaded_at"

# The columns bizon writes when `unnest` is off, mirroring BigQueryDestination.get_bigquery_schema().
# Declared as plain strings (not SchemaField) so config validation can check the partition column
# without importing the BigQuery client. `test_metadata_columns_match_schema` keeps the two in sync.
BIZON_METADATA_COLUMNS = frozenset(
    {
        "_source_record_id",
        "_source_timestamp",
        "_source_data",
        "_bizon_extracted_at",
        DEFAULT_PARTITION_FIELD,
        "_bizon_id",
    }
)


class GCSBufferFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class TimePartitioningWindow(str, Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    MONTH = "MONTH"
    YEAR = "YEAR"


class TimePartitioning(BaseModel):
    """BigQuery time partitioning, shared by the three BigQuery destinations.

    Accepts either the full mapping (`time_partitioning: {type: DAY, field: my_ts}`) or, for
    backwards compatibility with the batch destination's original config, a bare window
    (`time_partitioning: DAY`). `field: null` selects ingestion-time partitioning.
    """

    model_config = ConfigDict(extra="forbid")

    type: TimePartitioningWindow = Field(default=TimePartitioningWindow.DAY, description="Partitioning window")
    field: Optional[str] = Field(
        default=DEFAULT_PARTITION_FIELD,
        description="Column to partition on. Must exist in the destination schema. "
        "Set to null for ingestion-time partitioning.",
    )

    @model_validator(mode="before")
    @classmethod
    def _accept_bare_window(cls, data: Any) -> Any:
        # Backwards compatibility: `time_partitioning: DAY` used to be the whole config.
        if isinstance(data, str):
            return {"type": data}
        return data


class BigQueryColumnType(str, Enum):
    BOOLEAN = "BOOLEAN"
    BYTES = "BYTES"
    DATE = "DATE"
    DATETIME = "DATETIME"
    FLOAT = "FLOAT"
    FLOAT64 = "FLOAT64"
    GEOGRAPHY = "GEOGRAPHY"
    INTEGER = "INTEGER"
    INT64 = "INT64"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    JSON = "JSON"
    RECORD = "RECORD"
    STRING = "STRING"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"


class BigQueryColumnMode(str, Enum):
    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"


BIGQUERY_TO_POLARS_TYPE_MAPPING = {
    "STRING": pl.String,
    "BYTES": pl.Binary,
    "INTEGER": pl.Int64,
    "INT64": pl.Int64,
    "FLOAT": pl.Float64,
    "FLOAT64": pl.Float64,
    "NUMERIC": pl.Float64,  # Can be refined for precision with Decimal128 if needed
    "BIGNUMERIC": pl.Float64,  # Similar to NUMERIC
    "BOOLEAN": pl.Boolean,
    "BOOL": pl.Boolean,
    "TIMESTAMP": pl.String,  # We use BigQuery internal parsing to convert to datetime
    "DATE": pl.String,  # We use BigQuery internal parsing to convert to datetime
    "DATETIME": pl.String,  # We use BigQuery internal parsing to convert to datetime
    "TIME": pl.Time,
    "GEOGRAPHY": pl.Object,  # Polars doesn't natively support geography types
    "ARRAY": pl.List,  # Requires additional handling for element types
    "JSON": pl.String,
}


class BigQueryColumn(DestinationColumn):
    name: str = Field(..., description="Name of the column")
    type: BigQueryColumnType = Field(..., description="Type of the column")
    mode: BigQueryColumnMode = Field(..., description="Mode of the column")
    description: Optional[str] = Field(None, description="Description of the column")
    default_value_expression: Optional[str] = Field(None, description="Default value expression")

    @property
    def polars_type(self):
        return BIGQUERY_TO_POLARS_TYPE_MAPPING.get(self.type.upper())


class BigQueryAuthentication(BaseModel):
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQueryRecordSchemaConfig(BaseModel):
    destination_id: str = Field(..., description="Destination ID")
    record_schema: list[BigQueryColumn] = Field(..., description="Record schema")

    # BigQuery Clustering Keys
    clustering_keys: Optional[list[str]] = Field(None, description="Clustering keys")


class BigQueryConfigDetails(AbstractDestinationDetailsConfig):
    # Table details
    project_id: str = Field(..., description="BigQuery Project ID")
    dataset_id: str = Field(..., description="BigQuery Dataset ID")
    dataset_location: str = Field(default="US", description="BigQuery Dataset location")
    create_dataset: bool = Field(
        default=False,
        description="Create the dataset if it does not exist. When False, a missing dataset raises a clear error.",
    )

    # GCS Buffer
    gcs_buffer_bucket: str = Field(..., description="GCS Buffer bucket")
    gcs_buffer_format: GCSBufferFormat = Field(default=GCSBufferFormat.PARQUET, description="GCS Buffer format")

    # Time partitioning
    time_partitioning: TimePartitioning = Field(
        default_factory=TimePartitioning,
        description="BigQuery time partitioning. Accepts `{type, field}` or a bare window (`DAY`).",
    )

    enforce_partitioning: bool = Field(
        default=False,
        description="On a full refresh, drop the destination table when its partitioning does not match the "
        "configured spec, so the publish job recreates it partitioned. BigQuery cannot change an existing "
        "table's partitioning in place, so this is the only way to fix a legacy table. Off by default: the "
        "table is briefly absent, and dropping it discards table-level metadata (description, labels, "
        "table ACLs, policy tags).",
    )

    # Async / batched load jobs (throughput + load-job quota optimization).
    # When enabled, buffer flushes upload to GCS and submit load jobs without blocking,
    # batching multiple GCS files into a single load job. Cursors are created only once a
    # load lands (preserving the at-least-once recovery contract). Disabled by default.
    async_load: bool = Field(
        default=False,
        description="Submit BigQuery load jobs asynchronously and batch GCS files into fewer jobs.",
    )
    load_files_per_job: int = Field(
        default=10,
        ge=1,
        description="Number of GCS files batched into a single load job when async_load is enabled.",
    )
    load_max_in_flight_jobs: int = Field(
        default=3,
        ge=1,
        description="Max concurrent in-flight load jobs before back-pressuring when async_load is enabled.",
    )

    # Schema for unnesting
    record_schemas: Optional[list[BigQueryRecordSchemaConfig]] = Field(
        default=None, description="Schema for the records. Required if unnest is set to true."
    )

    table_prefix: str = Field(
        default=BIZON_TABLE_PREFIX,
        description="Prefix applied to auto-generated table names (when destination_id is not set). "
        "An existing legacy (unprefixed) table is reused if present. Set to '' to disable.",
    )

    authentication: Optional[BigQueryAuthentication] = None

    @model_validator(mode="after")
    def _validate_partition_field(self) -> "BigQueryConfigDetails":
        """Reject a partition column that cannot exist in the table this destination writes.

        Partitioning is materialized by the load job, so an unknown column otherwise only surfaces
        mid-run as an opaque BigQuery error, after data has already been staged in GCS and the temp
        table. This cannot be the only check: the stream runner assigns `record_schemas` after
        validation (`bizon/engine/runner/adapters/streaming.py`), so the destination re-checks the
        resolved schema at runtime. This layer exists for the error message.
        """
        field = self.time_partitioning.field

        if field is None:
            return self  # Ingestion-time partitioning needs no column.

        if not self.unnest:
            if field not in BIZON_METADATA_COLUMNS:
                raise ValueError(
                    f"`time_partitioning.field` is '{field}', which is not a column bizon writes "
                    f"({sorted(BIZON_METADATA_COLUMNS)}). Pick one of those, or set "
                    f"`time_partitioning.field: null` for ingestion-time partitioning."
                )
            return self

        for record_schema in self.record_schemas or []:
            column_names = {column.name for column in record_schema.record_schema}
            if field not in column_names:
                raise ValueError(
                    f"`time_partitioning.field` is '{field}' but the record_schema for "
                    f"'{record_schema.destination_id}' does not declare it (columns: {sorted(column_names)}). "
                    f"With `unnest: true` the table holds only the columns in record_schema, so the "
                    f"`_bizon_*` metadata columns are not available. Point the field at an existing "
                    f"TIMESTAMP/DATE/DATETIME column, or set it to null for ingestion-time partitioning."
                )
        return self


class BigQueryConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY]
    alias: str = "bigquery"
    buffer_size: Optional[int] = 400
    config: BigQueryConfigDetails
