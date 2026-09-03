import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Type

import orjson
import polars as pl
import urllib3.exceptions
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import (
    Conflict,
    InvalidArgument,
    NotFound,
    RetryError,
    ServerError,
    ServiceUnavailable,
)
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, TimePartitioning
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    ProtoRows,
    ProtoSchema,
)
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
from google.protobuf.message import EncodeError, Message
from loguru import logger
from requests.exceptions import ConnectionError, SSLError, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import UNNEST_HINT, describe_partition_problem
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
from bizon.source.config import SourceSyncModes

from .config import BigQueryStreamingV2ConfigDetails
from .proto_utils import get_proto_schema_and_class


class BigQueryStreamingV2Destination(AbstractDestination):
    # Add constants for limits
    MAX_REQUEST_SIZE_BYTES = 9.5 * 1024 * 1024  # 9.5 MB (max is 10MB)
    MAX_ROW_SIZE_BYTES = 8 * 1024 * 1024  # 8 MB (max is 10MB)

    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryStreamingV2ConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):  # type: ignore
        super().__init__(sync_metadata, config, backend, source_callback, monitor)
        self.config: BigQueryStreamingV2ConfigDetails = config

        if config.authentication and config.authentication.service_account_key:
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.authentication.service_account_key.encode())
                temp_file_path = temp.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

        self.project_id = config.project_id
        self.bq_client = bigquery.Client(project=self.project_id)
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location
        self.bq_max_rows_per_request = config.bq_max_rows_per_request
        self.bq_storage_client_options = ClientOptions(
            quota_project_id=self.project_id,
        )
        # Cache of (temp_table_id, schema_fingerprint) pairs already ensured in this process.
        # Prevents calling create_table on every flush, which otherwise hits BigQuery's
        # per-table metadata quota (5 ops / 10s) and returns 403 rateLimitExceeded.
        self._ensured_tables: set[tuple[str, int]] = set()
        self._resolved_default_table_id: str | None = None
        # Tables already warned about for an unusable partition field (see should_apply_partitioning).
        self._partition_warned: set[str] = set()

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

    @property
    def temp_table_id(self) -> str:
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            return f"{self.table_id}_temp"
        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            return f"{self.table_id}_incremental"
        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            return f"{self.table_id}"
        # Default fallback
        return f"{self.table_id}"

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
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"
                ),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
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
                InvalidArgument,
            )
        ),
        wait=wait_exponential(multiplier=2, min=4, max=120),
        stop=stop_after_attempt(8),
        before_sleep=lambda retry_state: logger.warning(
            f"Streaming append attempt {retry_state.attempt_number} failed. "
            f"Retrying in {retry_state.next_action.sleep} seconds..."
        ),
    )
    def append_rows_to_stream(
        self,
        stream_name: str,
        proto_schema: ProtoSchema,
        serialized_rows: List[bytes],
    ):
        write_client = BigQueryWriteClient(client_options=self.bq_storage_client_options)

        request = AppendRowsRequest(
            write_stream=stream_name,
            proto_rows=AppendRowsRequest.ProtoData(
                rows=ProtoRows(serialized_rows=serialized_rows),
                writer_schema=proto_schema,
            ),
        )
        try:
            response = write_client.append_rows(iter([request]))
            return response.code().name
        except Exception as e:
            logger.error(f"Error in append_rows_to_stream: {str(e)}")
            logger.error(f"Stream name: {stream_name}")
            raise

    @staticmethod
    def to_protobuf_serialization(TableRowClass: Type[Message], row: dict) -> bytes:
        """Convert a row to a Protobuf serialization."""
        # Proto schema only has scalar types — convert any dict/list values to JSON strings
        row = {k: orjson.dumps(v).decode("utf-8") if isinstance(v, (dict, list)) else v for k, v in row.items()}
        try:
            record = ParseDict(row, TableRowClass())
        except ParseError as e:
            logger.error(f"Error serializing record: {e} for row: {row}.")
            raise e

        try:
            serialized_record = record.SerializeToString()
        except EncodeError as e:
            logger.error(f"Error serializing record: {e} for row: {row}.")
            raise e
        return serialized_record

    @staticmethod
    def from_protobuf_serialization(
        TableRowClass: Type[Message],
        serialized_data: bytes,
    ) -> dict:
        """Convert protobuf serialization back to a dictionary."""
        record = TableRowClass()
        record.ParseFromString(serialized_data)
        return MessageToDict(record, preserving_proto_field_name=True)

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
    def process_streaming_batch(
        self,
        stream_name: str,
        proto_schema: ProtoSchema,
        batch: dict,
        table_row_class: Type[Message],
    ) -> List[Tuple[str, str]]:
        """Process a single batch for streaming and/or large rows with retry logic."""
        results = []
        try:
            # Handle streaming batch
            if batch.get("stream_batch") and len(batch["stream_batch"]) > 0:
                result = self.append_rows_to_stream(stream_name, proto_schema, batch["stream_batch"])
                results.append(("streaming", result))

            # Handle large rows batch
            if batch.get("json_batch") and len(batch["json_batch"]) > 0:
                # Deserialize protobuf bytes back to JSON for the load job
                deserialized_rows = []
                for serialized_row in batch["json_batch"]:
                    deserialized_row = self.from_protobuf_serialization(table_row_class, serialized_row)
                    deserialized_rows.append(deserialized_row)

                # For large rows, we need to use the main client (write to temp_table_id)
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=self.bq_client.get_table(self.temp_table_id).schema,
                    ignore_unknown_values=True,
                )
                load_job = self.bq_client.load_table_from_json(
                    deserialized_rows, self.temp_table_id, job_config=job_config, timeout=300
                )
                result = load_job.result()
                if load_job.state != "DONE":
                    raise Exception(f"Failed to load rows to BigQuery: {load_job.errors}")

                # Track large rows
                self.monitor.track_large_records_synced(
                    num_records=len(batch["json_batch"]), extra_tags={"destination_id": self.destination_id}
                )

                results.append(("large_rows", "DONE"))

            if not results:
                results.append(("empty", "SKIPPED"))

            return results
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise

    def _clustering_fields(self) -> Optional[list]:
        """Clustering keys configured for this destination, if any."""
        if self.clustering_keys and self.clustering_keys.get(self.destination_id):
            return self.clustering_keys[self.destination_id]
        return None

    def _partition_problem(self, schema, table_id: str) -> Optional[str]:
        """Why the configured partition field cannot partition `table_id`, or None.

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
            f"the schema written to {table_id}",
            hint=UNNEST_HINT if self.config.unnest else "",
        )

    def _ensure_table(self, table_id: str):
        """Create `table_id` with the configured partitioning and clustering if it does not exist.

        Only once per (table, schema) in this process: calling create_table on every flush otherwise
        hits BigQuery's per-table metadata quota (5 ops / 10s) and the API returns 403
        rateLimitExceeded, which the google-cloud-bigquery SDK silently retries with exponential
        backoff. On Conflict the schema is reconciled additively; partitioning and clustering of an
        existing table cannot be changed by BigQuery at all.
        """
        schema = self.get_bigquery_schema()
        schema_fingerprint = hash(tuple((f.name, f.field_type, f.mode) for f in schema))
        cache_key = (table_id, schema_fingerprint)

        if cache_key in self._ensured_tables:
            return

        table = bigquery.Table(table_id, schema=schema)
        if self.config.time_partitioning and should_apply_partitioning(
            self.bq_client, table_id, self._partition_problem(schema, table_id), self._partition_warned
        ):
            table.time_partitioning = TimePartitioning(
                field=self.config.time_partitioning.field, type_=self.config.time_partitioning.type
            )
        clustering_fields = self._clustering_fields()
        if clustering_fields:
            table.clustering_fields = clustering_fields
        try:
            self.bq_client.create_table(table)
        except Conflict:
            existing_table = self.bq_client.get_table(table_id)
            existing_fields = {field.name: field for field in existing_table.schema}
            new_fields = {field.name: field for field in schema}
            fields_to_add = [field for name, field in new_fields.items() if name not in existing_fields]

            if fields_to_add:
                logger.warning(f"Adding new fields to table schema: {[field.name for field in fields_to_add]}")
                existing_table.schema = existing_table.schema + fields_to_add
                self.bq_client.update_table(existing_table, ["schema"])
        self._ensured_tables.add(cache_key)

    def load_to_bigquery_via_streaming(self, df_destination_records: pl.DataFrame) -> str:
        schema = self.get_bigquery_schema()
        self._ensure_table(self.temp_table_id)

        # Create the stream (use temp_table_id for staging)
        temp_table_parts = self.temp_table_id.split(".")
        if len(temp_table_parts) == 3:
            project, dataset, table_name = temp_table_parts
            parent = BigQueryWriteClient.table_path(project, dataset, table_name)
        else:
            parent = BigQueryWriteClient.table_path(self.project_id, self.dataset_id, temp_table_parts[-1])

        stream_name = f"{parent}/_default"

        # Generating the protocol buffer representation of the message descriptor.
        proto_schema, TableRow = get_proto_schema_and_class(schema)

        if self.config.unnest:
            serialized_rows = [
                self.to_protobuf_serialization(TableRowClass=TableRow, row=orjson.loads(row))
                for row in df_destination_records["source_data"].to_list()
            ]
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

            serialized_rows = [
                self.to_protobuf_serialization(TableRowClass=TableRow, row=row)
                for row in df_destination_records.iter_rows(named=True)
            ]

        streaming_results = []
        large_rows_results = []

        # Collect all batches first
        batches = list(self.batch(serialized_rows))

        if not batches:
            logger.info("No batches to process, skipping streaming upload")
            return

        # Use ThreadPoolExecutor for parallel processing
        max_workers = min(len(batches), self.config.max_concurrent_threads)
        logger.info(f"Processing {len(batches)} batches with {max_workers} concurrent threads")

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batch processing tasks
                future_to_batch = {
                    executor.submit(self.process_streaming_batch, stream_name, proto_schema, batch, TableRow): batch
                    for batch in batches
                }

                # Collect results as they complete
                for future in as_completed(future_to_batch):
                    batch_results = future.result()
                    for batch_type, result in batch_results:
                        if batch_type == "streaming":
                            streaming_results.append(result)
                        if batch_type == "large_rows":
                            large_rows_results.append(result)

        except Exception as e:
            logger.error(f"Error in multithreaded batch processing: {str(e)}, type: {type(e)}")
            if isinstance(e, RetryError):
                logger.error(f"Retry error details: {e.cause if hasattr(e, 'cause') else 'No cause available'}")
            raise

        if len(streaming_results) > 0:
            assert all([r == "OK" for r in streaming_results]) is True, "Failed to append rows to stream"
        if len(large_rows_results) > 0:
            assert all([r == "DONE" for r in large_rows_results]) is True, "Failed to load rows to BigQuery"

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        self.load_to_bigquery_via_streaming(df_destination_records=df_destination_records)
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
                logger.warning(f"Large row detected: {item_size} bytes")
            else:
                current_batch.append(item)
                current_batch_size += item_size

        # Yield the last batch
        if current_batch or large_rows:
            logger.info(
                f"Yielding streaming batch of {len(current_batch)} rows, size: {current_batch_size / 1024 / 1024:.2f}MB"
            )
            if large_rows:
                logger.warning(f"Yielding large rows batch of {len(large_rows)} rows")
            yield {"stream_batch": current_batch, "json_batch": large_rows}

    def _partition_clause(self, spec=None) -> str:
        """`PARTITION BY` matching the temp table, spelled out because CTAS does not inherit it.

        The function is picked from the column's declared BigQuery type, so this can never emit an
        arbitrary user expression.

        Validation is against the spec being emitted, not against the config: `_publish_spec()`
        returns the destination table's *current* spec whenever it differs and `enforce_partitioning`
        is off, so a bad configured field never reaches this DDL in that case -- and must not stop a
        publish it has no part in. When the configured spec is the one going out, BigQuery would
        reject the statement, so raise.
        """
        window, field, _ = self._intended_spec() if spec is None else spec

        if window is None:
            return ""

        if not field:
            return "PARTITION BY _PARTITIONDATE"  # Ingestion-time partitioning.

        schema = self.get_bigquery_schema()
        problem = describe_partition_problem(
            field,
            window,
            {f.name: f.field_type for f in schema},
            f"the schema written to {self.table_id}",
            hint=UNNEST_HINT if self.config.unnest else "",
        )
        if problem:
            raise ValueError(f"Cannot publish {self.table_id}. {problem}")

        quoted = f"`{field}`"
        column_type = {f.name: (f.field_type or "").upper() for f in schema}[field]

        if column_type == "TIMESTAMP":
            return f"PARTITION BY TIMESTAMP_TRUNC({quoted}, {window})"
        if column_type == "DATETIME":
            return f"PARTITION BY DATETIME_TRUNC({quoted}, {window})"
        # DATE: describe_partition_problem already rejected HOUR, which BigQuery does not support here.
        if window == "DAY":
            return f"PARTITION BY {quoted}"
        return f"PARTITION BY DATE_TRUNC({quoted}, {window})"

    def _clustering_clause(self, spec=None) -> str:
        clustering_fields = spec[2] if spec is not None else self._clustering_fields()
        if not clustering_fields:
            return ""
        return "CLUSTER BY " + ", ".join(f"`{key}`" for key in clustering_fields)

    def _publish_clauses(self, spec=None) -> str:
        return " ".join(clause for clause in (self._partition_clause(spec), self._clustering_clause(spec)) if clause)

    def finalize(self):
        """Finalize the sync by moving data from temp table to main table based on sync mode.

        Publishing stays query-based rather than moving to the free copy jobs the batch `bigquery`
        destination uses. Rows written through the Storage Write API `_default` stream sit in
        write-optimized storage, and BigQuery does not guarantee they are visible to copy and export
        jobs for some time after the write, while `SELECT *` always sees them. finalize() runs
        seconds after the last append, so a copy job here could silently publish a table missing the
        tail of the run -- trading a partitioning bug for a data-loss bug. Do not "unify" the two
        publish paths without solving that first.
        """
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            # CTAS creates the main table, so the PARTITION BY / CLUSTER BY have to be spelled out:
            # a plain `CREATE OR REPLACE TABLE ... AS SELECT` produces an unpartitioned, unclustered
            # table. BigQuery refuses to replace a table with a different partitioning spec *in
            # either direction*, so the DDL has to describe whatever spec the table will actually
            # end up with -- see _publish_spec().
            clauses = self._publish_clauses(self._publish_spec())

            logger.info(f"Loading temp table {self.temp_table_id} data into {self.table_id} ...")
            query = f"CREATE OR REPLACE TABLE `{self.table_id}` {clauses} AS SELECT * FROM `{self.temp_table_id}`"
            self.bq_client.query(" ".join(query.split())).result()
            logger.info(f"Deleting temp table {self.temp_table_id} ...")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
            # The CTAS replaced the main table, so any cached entry for it is stale too.
            self._ensured_tables = {k for k in self._ensured_tables if k[0] not in (self.temp_table_id, self.table_id)}
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            # Create the main table partitioned and clustered before the first INSERT: `INSERT INTO`
            # fails with 404 when the table does not exist, and a table created any other way (or by
            # an older bizon) carries no partitioning.
            self._ensure_table(self.table_id)
            self._warn_on_partitioning_mismatch()
            logger.info(f"Appending data from {self.temp_table_id} to {self.table_id} ...")
            self.bq_client.query(f"INSERT INTO `{self.table_id}` SELECT * FROM `{self.temp_table_id}`").result()
            logger.info(f"Deleting incremental temp table {self.temp_table_id} ...")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
            # Only the temp table is gone; the main table persists and stays validly cached.
            self._ensured_tables = {k for k in self._ensured_tables if k[0] != self.temp_table_id}
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            # Direct writes, no finalization needed
            return True

        return True

    def _intended_spec(self):
        return spec_from_config(self.config.time_partitioning, self._clustering_fields())

    def _warn_on_partitioning_mismatch(self) -> bool:
        """Warn when the destination table's partitioning differs from the configured spec.

        Returns True when it matches (or the table does not exist yet), False on a mismatch.
        """
        try:
            destination_table = self.bq_client.get_table(self.table_id)
        except NotFound:
            return True  # Absent: it will be created with the configured spec.

        current = spec_from_table(destination_table)
        intended = self._intended_spec()

        if current == intended:
            return True

        logger.warning(
            f"Partitioning mismatch on {self.table_id}: the table is {describe(current)}, but the config asks "
            f"for {describe(intended)}. BigQuery cannot change an existing table's partitioning, so it stays "
            f"{describe(current)}."
        )
        return False

    def _publish_spec(self):
        """The partitioning spec the full-refresh CTAS should declare.

        BigQuery rejects `CREATE OR REPLACE TABLE` whenever the declared spec differs from the
        existing table's -- in *either* direction ("Cannot replace a table with a different
        partitioning spec. Instead, DROP the table, and then recreate it."). So the DDL cannot simply
        state the configured spec: against a legacy unpartitioned table, or a table partitioned on a
        column the config has since changed, that would turn a silent problem into a failed run.

        Absent or already matching -> the configured spec, and the table ends up right.
        Mismatched -> keep the run working by declaring the table's *current* spec, and warn. Unless
        enforce_partitioning is set, in which case drop the table so the configured spec applies.
        """
        intended = self._intended_spec()

        try:
            destination_table = self.bq_client.get_table(self.table_id)
        except NotFound:
            return intended  # Absent: created fresh with the configured layout.

        current = spec_from_table(destination_table)
        if current == intended:
            return intended

        logger.warning(
            f"Partitioning mismatch on {self.table_id}: the table is {describe(current)}, but the config asks "
            f"for {describe(intended)}. BigQuery cannot change an existing table's partitioning."
        )

        if not self.config.enforce_partitioning:
            logger.warning(
                f"Publishing {self.table_id} as {describe(current)} to keep the run working. Set "
                f"`enforce_partitioning: true` on the destination config to have a full refresh drop and rebuild "
                f"it with the configured partitioning instead."
            )
            return current

        logger.warning(
            f"enforce_partitioning: dropping {self.table_id} so it is recreated {describe(intended)}. "
            f"The table is absent until the publish query lands, and table-level metadata (description, labels, "
            f"ACLs, policy tags) is not recreated."
        )
        self.bq_client.delete_table(self.table_id, not_found_ok=True)
        self._ensured_tables = {k for k in self._ensured_tables if k[0] != self.table_id}
        return intended
