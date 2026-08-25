import os
import tempfile
from typing import List, Tuple

import orjson
import polars as pl
import urllib3.exceptions
from google.api_core.exceptions import (
    Conflict,
    NotFound,
    RetryError,
    ServerError,
    ServiceUnavailable,
)
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import DatasetReference, TimePartitioning
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    ProtoRows,
    ProtoSchema,
)
from loguru import logger
from requests.exceptions import ConnectionError, SSLError, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    UNNEST_HINT,
    BigQueryColumnMode,
    BigQueryColumnType,
    describe_partition_problem,
)
from bizon.connectors.destinations.bigquery.src.partitioning import (
    describe,
    should_apply_partitioning,
    spec_from_config,
    spec_from_table,
)
from bizon.connectors.destinations.bigquery.src.table_naming import resolve_default_table_id
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback

from .config import BigQueryStreamingConfigDetails


class BigQueryStreamingDestination(AbstractDestination):
    # Add constants for limits
    MAX_REQUEST_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (max is 10MB)
    MAX_ROW_SIZE_BYTES = 0.9 * 1024 * 1024  # 1 MB

    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryStreamingConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):  # type: ignore
        super().__init__(sync_metadata, config, backend, source_callback, monitor)
        self.config: BigQueryStreamingConfigDetails = config

        if config.authentication and config.authentication.service_account_key:
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.authentication.service_account_key.encode())
                temp_file_path = temp.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

        self.project_id = config.project_id
        self.bq_client = bigquery.Client(project=self.project_id)
        self.bq_storage_client = bigquery_storage_v1.BigQueryWriteClient()
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location
        self.bq_max_rows_per_request = config.bq_max_rows_per_request
        self._resolved_default_table_id: str | None = None
        # Tables already warned about, so the per-flush create_table call does not repeat either
        # warning (unusable partition field, and partitioning that drifted from the live table).
        self._partition_warned: set[str] = set()
        self._drift_warned: set[str] = set()

    @property
    def table_id(self) -> str:
        # Explicit destination_id (full project.dataset.table path) is the user's choice -> never prefixed.
        if self.destination_id:
            return self.destination_id
        # Auto-generated name: resolve once (reuses a legacy table if present, else `_bizon_` prefix).
        if self._resolved_default_table_id is None:
            base_name = f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
            self._resolved_default_table_id = resolve_default_table_id(
                self.bq_client, self.project_id, self.dataset_id, base_name, self.config.table_prefix
            )
        return self._resolved_default_table_id

    def get_bigquery_schema(self) -> List[bigquery.SchemaField]:
        if self.config.unnest:
            if len(list(self.record_schemas.keys())) == 1:
                self.destination_id = list(self.record_schemas.keys())[0]

            return [
                bigquery.SchemaField(
                    name=col.name,
                    field_type=col.type,
                    mode=col.mode,
                    description=col.description,
                    default_value_expression=col.default_value_expression,
                )
                for col in self.record_schemas[self.destination_id]
            ]

        # Case we don't unnest the data
        else:
            return [
                bigquery.SchemaField(
                    "_source_record_id",
                    BigQueryColumnType.STRING,
                    mode=BigQueryColumnMode.REQUIRED,
                    description="The source record id",
                ),
                bigquery.SchemaField(
                    "_source_timestamp",
                    BigQueryColumnType.TIMESTAMP,
                    mode=BigQueryColumnMode.REQUIRED,
                    description="The source timestamp",
                ),
                bigquery.SchemaField(
                    "_source_data",
                    BigQueryColumnType.JSON,
                    mode=BigQueryColumnMode.NULLABLE,
                    description="The source data",
                ),
                bigquery.SchemaField(
                    "_bizon_extracted_at",
                    BigQueryColumnType.TIMESTAMP,
                    mode=BigQueryColumnMode.REQUIRED,
                    description="The bizon extracted at",
                ),
                bigquery.SchemaField(
                    "_bizon_loaded_at",
                    BigQueryColumnType.TIMESTAMP,
                    mode=BigQueryColumnMode.REQUIRED,
                    default_value_expression="CURRENT_TIMESTAMP()",
                    description="The bizon loaded at",
                ),
                bigquery.SchemaField(
                    "_bizon_id",
                    BigQueryColumnType.STRING,
                    mode=BigQueryColumnMode.REQUIRED,
                    description="The bizon id",
                ),
            ]

    def check_connection(self) -> bool:
        dataset_ref = DatasetReference(self.project_id, self.dataset_id)

        try:
            self.bq_client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.dataset_location
            # exists_ok=True: swallow the 409 if another worker created it in the race window
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
        return True

    def append_rows_to_stream(
        self,
        write_client: bigquery_storage_v1.BigQueryWriteClient,
        stream_name: str,
        proto_schema: ProtoSchema,
        serialized_rows: List[bytes],
    ):
        request = AppendRowsRequest(
            write_stream=stream_name,
            proto_rows=AppendRowsRequest.ProtoData(
                rows=ProtoRows(serialized_rows=serialized_rows),
                writer_schema=proto_schema,
            ),
        )
        response = write_client.append_rows(iter([request]))
        return response.code().name

    @retry(
        retry=retry_if_exception_type(
            (
                ServerError,
                ServiceUnavailable,
                SSLError,
                ConnectionError,
                Timeout,
                RetryError,
                urllib3.exceptions.ProtocolError,
                urllib3.exceptions.SSLError,
            )
        ),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        stop=stop_after_attempt(8),
        before_sleep=lambda retry_state: logger.warning(
            f"Attempt {retry_state.attempt_number} failed. Retrying in {retry_state.next_action.sleep} seconds..."
        ),
    )
    def _insert_batch(self, table, batch):
        """Helper method to insert a batch of rows with retry logic"""
        logger.debug(f"Inserting batch in table {table.table_id}")
        try:
            # Handle streaming batch
            if batch.get("stream_batch") and len(batch["stream_batch"]) > 0:
                self.bq_client.insert_rows_json(
                    table,
                    batch["stream_batch"],
                    row_ids=[None] * len(batch["stream_batch"]),
                    timeout=300,  # 5 minutes timeout per request
                )

            # Handle large rows batch
            if batch.get("json_batch") and len(batch["json_batch"]) > 0:
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=table.schema,
                    ignore_unknown_values=True,
                )

                load_job = self.bq_client.load_table_from_json(
                    batch["json_batch"], table, job_config=job_config, timeout=300
                )
                load_job.result()

                if load_job.state != "DONE":
                    raise Exception(f"Failed to load rows to BigQuery: {load_job.errors}")

                self.monitor.track_large_records_synced(
                    num_records=len(batch["json_batch"]), extra_tags={"destination_id": self.destination_id}
                )

        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}, type: {type(e)}")
            raise

    def _partition_problem(self, schema) -> str:
        """Why the configured partition field cannot partition this table, or None.

        The config validator checks the same thing for the message, but cannot be the only check:
        the stream runner assigns `record_schemas` after validation, so in a `streams:` run this is
        the first look at the schema actually being written.
        """
        if not self.config.time_partitioning:
            return None

        return describe_partition_problem(
            self.config.time_partitioning.field,
            self.config.time_partitioning.type.value,
            {field.name: field.field_type for field in schema},
            f"the schema written to {self.table_id}",
            hint=UNNEST_HINT if self.config.unnest else "",
        )

    def _warn_on_partitioning_drift(self, table) -> bool:
        """Report a live table whose partitioning no longer matches the config. Returns True if it does.

        This destination appends straight into the table and has no `finalize()`, so `create_table`
        is the only thing that ever applies `time_partitioning` -- and it just raised `Conflict`,
        which means BigQuery kept the table's own spec and ignored ours. BigQuery cannot repartition
        in place, so there is nothing to do about it here, but silence is how a config partitioning
        on one column and a table partitioned on another stay divergent indefinitely. Rebuilding the
        table is the only fix.
        """
        intended = spec_from_config(
            self.config.time_partitioning,
            self.clustering_keys.get(self.destination_id) if self.clustering_keys else None,
        )
        actual = spec_from_table(table)

        if intended == actual:
            return True

        if self.table_id not in self._drift_warned:
            self._drift_warned.add(self.table_id)
            logger.warning(
                f"{self.table_id} is {describe(actual)}, but the config asks for {describe(intended)}. "
                f"BigQuery cannot change an existing table's partitioning, so it stays as it is and "
                f"the configured spec has no effect. Rebuild the table to change it."
            )

        return False

    def load_to_bigquery_via_legacy_streaming(self, df_destination_records: pl.DataFrame) -> str:
        # Create table if it does not exist
        schema = self.get_bigquery_schema()
        table = bigquery.Table(self.table_id, schema=schema)

        if self.config.time_partitioning and should_apply_partitioning(
            self.bq_client, self.table_id, self._partition_problem(schema), self._partition_warned
        ):
            table.time_partitioning = TimePartitioning(
                field=self.config.time_partitioning.field, type_=self.config.time_partitioning.type
            )

        if self.clustering_keys and self.clustering_keys[self.destination_id]:
            table.clustering_fields = self.clustering_keys[self.destination_id]
        try:
            table = self.bq_client.create_table(table)
        except Conflict:
            table = self.bq_client.get_table(self.table_id)
            self._warn_on_partitioning_drift(table)
            # Compare and update schema if needed
            existing_fields = {field.name: field for field in table.schema}
            new_fields = {field.name: field for field in self.get_bigquery_schema()}

            # Find fields that need to be added
            fields_to_add = [field for name, field in new_fields.items() if name not in existing_fields]

            if fields_to_add:
                logger.warning(f"Adding new fields to table schema: {[field.name for field in fields_to_add]}")
                updated_schema = table.schema + fields_to_add
                table.schema = updated_schema
                table = self.bq_client.update_table(table, ["schema"])

        if self.config.unnest:
            # We cannot use the `json_decode` method here because of the issue: https://github.com/pola-rs/polars/issues/22371
            rows_to_insert = [orjson.loads(row) for row in df_destination_records["source_data"].to_list()]
        else:
            df_destination_records = df_destination_records.with_columns(
                pl.col("bizon_extracted_at").dt.strftime("%Y-%m-%d %H:%M:%S").alias("bizon_extracted_at"),
                pl.col("bizon_loaded_at").dt.strftime("%Y-%m-%d %H:%M:%S").alias("bizon_loaded_at"),
                pl.col("source_timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("source_timestamp"),
            )
            df_destination_records = df_destination_records.rename(
                {
                    "bizon_id": "_bizon_id",
                    "bizon_extracted_at": "_bizon_extracted_at",
                    "bizon_loaded_at": "_bizon_loaded_at",
                    "source_record_id": "_source_record_id",
                    "source_timestamp": "_source_timestamp",
                    "source_data": "_source_data",
                }
            )
            rows_to_insert = [row for row in df_destination_records.iter_rows(named=True)]

        errors = []
        for batch in self.batch(rows_to_insert):
            try:
                batch_errors = self._insert_batch(table, batch)
                if batch_errors:
                    errors.extend(batch_errors)
            except Exception as e:
                logger.error(f"Failed to insert batch on destination {self.destination_id} after all retries: {str(e)}")
                if isinstance(e, RetryError):
                    logger.error(f"Retry error details: {e.cause if hasattr(e, 'cause') else 'No cause available'}")
                raise

        if errors:
            logger.error("Encountered errors while inserting rows:")
            for error in errors:
                if error.get("errors") and len(error["errors"]) > 0:
                    logger.error("The following row failed to be inserted:")
                    if batch.get("stream_batch") and len(batch["stream_batch"]) > 0:
                        logger.error(f"{batch['stream_batch'][error['index']]}")
                    else:
                        logger.error(f"{batch['json_batch'][error['index']]}")
                    for error_detail in error["errors"]:
                        logger.error(f"Location (column): {error_detail['location']}")
                        logger.error(f"Reason: {error_detail['reason']}")
                        logger.error(f"Message: {error_detail['message']}")
            raise Exception(f"Encountered errors while inserting rows: {errors}")

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        logger.debug("Using BigQuery legacy streaming API...")
        self.load_to_bigquery_via_legacy_streaming(df_destination_records=df_destination_records)
        return True, ""

    def batch(self, iterable):
        """
        Yield successive batches respecting both row count and size limits.
        """
        current_batch = []
        current_batch_size = 0
        large_rows = []

        for item in iterable:
            # Estimate the size of the item (as JSON)
            item_size = len(str(item).encode("utf-8"))

            # If adding this item would exceed either limit, yield current batch and start new one
            if (
                len(current_batch) >= self.bq_max_rows_per_request
                or current_batch_size + item_size > self.MAX_REQUEST_SIZE_BYTES
            ):
                logger.debug(
                    f"Yielding batch of {len(current_batch)} rows, size: {current_batch_size / 1024 / 1024:.2f}MB"
                )
                yield {"stream_batch": current_batch, "json_batch": large_rows}
                current_batch = []
                current_batch_size = 0
                large_rows = []

            if item_size > self.MAX_ROW_SIZE_BYTES:
                large_rows.append(item)
                logger.debug(f"Large row detected: {item_size} bytes")
            else:
                current_batch.append(item)
                current_batch_size += item_size

        # Yield the last batch
        if current_batch:
            logger.debug(
                f"Yielding streaming batch of {len(current_batch)} rows, size: {current_batch_size / 1024 / 1024:.2f}MB"
            )
            logger.debug(f"Yielding large rows batch of {len(large_rows)} rows")
            yield {"stream_batch": current_batch, "json_batch": large_rows}
