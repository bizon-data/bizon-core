from enum import Enum
from typing import Any, Literal, Mapping, Optional

import polars as pl
from loguru import logger
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

# The columns bizon writes when `unnest` is off, mirroring get_bigquery_schema() on all three
# BigQuery destinations. Declared as plain strings (not SchemaField) so config validation can check
# the partition column without importing the BigQuery client.
#
# Types matter as much as names here: `_bizon_id` and `_source_record_id` exist but are STRING, so
# presence alone never meant a column could be partitioned on. Only the three timestamps can.
# `_source_data` is the one column the destinations disagree on -- JSON on the streaming pair, STRING
# on the batch destination, which round-trips through Parquet -- but it is unpartitionable either
# way, so a single entry is enough for this mapping's one purpose.
# `test_metadata_columns_match_schema` holds all of that to the real schemas.
BIZON_METADATA_COLUMN_TYPES = {
    "_source_record_id": "STRING",
    "_source_timestamp": "TIMESTAMP",
    "_source_data": "JSON",
    "_bizon_extracted_at": "TIMESTAMP",
    DEFAULT_PARTITION_FIELD: "TIMESTAMP",
    "_bizon_id": "STRING",
}

BIZON_METADATA_COLUMNS = frozenset(BIZON_METADATA_COLUMN_TYPES)

# BigQuery only time-partitions on these. (Integer range partitioning exists but bizon never emits it.)
PARTITIONABLE_COLUMN_TYPES = frozenset({"TIMESTAMP", "DATE", "DATETIME"})


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


UNNEST_HINT = (
    "With `unnest: true` the table holds only the columns in record_schema, so the `_bizon_*` "
    "metadata columns are not available. "
)


def describe_partition_problem(
    field: Optional[str],
    window: Optional[str],
    columns: Mapping[str, str],
    where: str,
    hint: str = "",
) -> Optional[str]:
    """Why `field` cannot time-partition a table with these columns, or None if it can.

    Reports instead of raising: the same misconfiguration is fatal in some places and harmless in
    others, and only the caller knows which. On the batch destination every load job stamps
    `time_partitioning` onto the temp table it creates, so a bad field fails every run. On the two
    streaming destinations the spec is only ever applied by `create_table`, which BigQuery answers
    with `Conflict` once the table exists -- discarding the spec and completing the run. Raising
    there would stop pipelines that have been running correctly for as long as their table has
    existed. See `_partition_field_or_raise` (batch) and `_ensure_table` (streaming) for the two
    severities this feeds.

    `columns` maps column name to BigQuery type. `where` names the schema for the message, e.g.
    "the record_schema for 'my-project.my_dataset.orders'". `hint` is an extra sentence for the
    absent-column case, where the caller may know *why* the column is missing.
    """
    if not field:
        return None  # Ingestion-time partitioning needs no column.

    column_type = (columns.get(field) or "").upper()

    if not column_type:
        return (
            f"`time_partitioning.field` is '{field}' but {where} does not declare it "
            f"(columns: {sorted(columns)}). {hint}Point the field at an existing "
            f"TIMESTAMP/DATE/DATETIME column, or set it to null for ingestion-time partitioning."
        )

    if column_type not in PARTITIONABLE_COLUMN_TYPES:
        return (
            f"`time_partitioning.field` is '{field}', which {where} declares as {column_type}. "
            f"BigQuery only time-partitions on {sorted(PARTITIONABLE_COLUMN_TYPES)}. Point the field "
            f"at such a column, or set it to null for ingestion-time partitioning."
        )

    if column_type == "DATE" and window == TimePartitioningWindow.HOUR.value:
        return (
            f"`time_partitioning` is HOUR on '{field}', a DATE column. BigQuery does not support "
            f"HOUR partitioning on DATE. Use DAY/MONTH/YEAR, or change the column to TIMESTAMP "
            f"or DATETIME."
        )

    return None


def resolve_partition_field(config) -> tuple:
    """Settle the partition field for a BigQuery destination config, and say what is wrong with it.

    Returns `(time_partitioning, problem)`. The caller assigns the first back onto the config and
    decides what to do about the second -- see `describe_partition_problem` for why the severity
    cannot be decided here.

    The one thing this *does* decide is the implicit default. `time_partitioning.field` defaults to
    `_bizon_loaded_at`, which under `unnest: true` is a column the table cannot contain: the table
    holds exactly the `record_schema` columns. Failing on that value would break configs that never
    mentioned partitioning at all, purely because they upgraded, so an unwritten field falls back to
    ingestion-time partitioning. A field the user actually wrote is left alone and checked.

    Every `record_schema` is checked, not just the first: with the `streams:` block each stream
    contributes one, and a stream added later is exactly how the partition field and the record
    schema drift apart. The message names the offending `destination_id`.
    """
    time_partitioning = config.time_partitioning

    if time_partitioning is None:
        return None, None  # Partitioning disabled outright (streaming destinations allow this).

    if not config.unnest:
        problem = describe_partition_problem(
            time_partitioning.field,
            time_partitioning.type.value,
            BIZON_METADATA_COLUMN_TYPES,
            "the schema bizon writes",
        )
        return time_partitioning, problem

    if "field" not in time_partitioning.model_fields_set and time_partitioning.field:
        logger.info(
            f"`unnest: true` writes only the record_schema columns, so the default "
            f"`time_partitioning.field: {time_partitioning.field}` cannot exist. Falling back to "
            f"ingestion-time partitioning ({time_partitioning.type.value}). Set "
            f"`time_partitioning.field` explicitly to partition on one of your own columns."
        )
        return time_partitioning.model_copy(update={"field": None}), None

    for record_schema in config.record_schemas or []:
        problem = describe_partition_problem(
            time_partitioning.field,
            time_partitioning.type.value,
            {column.name: column.type for column in record_schema.record_schema},
            f"the record_schema for '{record_schema.destination_id}'",
            hint=UNNEST_HINT,
        )
        if problem:
            return time_partitioning, problem

    return time_partitioning, None


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
        """Reject a partition column that cannot partition the table this destination writes.

        A hard failure here, unlike on the two streaming destinations, because every batch load job
        stamps `time_partitioning` onto the temp table it creates (`_build_load_job_config`) -- so a
        bad field fails every single run, whatever state the destination table is in. Without this
        the failure surfaces mid-run as an opaque BigQuery error, after data has already been staged
        in GCS and the temp table.

        This cannot be the only check: the stream runner assigns `record_schemas` after validation
        (`bizon/engine/runner/adapters/streaming.py`), so the destination re-checks the resolved
        schema at runtime. This layer exists for the error message.
        """
        self.time_partitioning, problem = resolve_partition_field(self)

        if problem:
            raise ValueError(problem)

        return self


class BigQueryConfig(AbstractDestinationConfig):
    name: Literal[DestinationTypes.BIGQUERY]
    alias: str = "bigquery"
    buffer_size: Optional[int] = 400
    config: BigQueryConfigDetails
