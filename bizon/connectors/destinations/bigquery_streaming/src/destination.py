import os
import tempfile
from datetime import datetime
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
from google.protobuf.json_format import ParseDict
from google.protobuf.message import Message
from loguru import logger
from requests.exceptions import ConnectionError, SSLError, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.source.callback import AbstractSourceCallback

from .config import BigQueryStreamingConfigDetails


class BigQueryStreamingDestination(AbstractDestination):

    # Add constants for limits
    MAX_ROWS_PER_REQUEST = 5000  # 5000 (max is 10000)
    MAX_REQUEST_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (max is 10MB)
    MAX_ROW_SIZE_BYTES = 0.9 * 1024 * 1024  # 1 MB

    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryStreamingConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
    ):  # type: ignore
        super().__init__(sync_metadata, config, backend, source_callback)
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

    @property
    def table_id(self) -> str:
        tabled_id = f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return self.destination_id or f"{self.project_id}.{self.dataset_id}.{tabled_id}"

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
            dataset = self.bq_client.create_dataset(dataset)
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

    def safe_cast_record_values(self, row: dict):
        """
        Safe cast record values to the correct type for BigQuery.
        """
        for col in self.record_schemas[self.destination_id]:

            # Handle dicts as strings
            if col.type == "STRING":
                if isinstance(row[col.name], dict) or isinstance(row[col.name], list):
                    row[col.name] = orjson.dumps(row[col.name]).decode("utf-8")

            # Handle timestamps
            if col.type in ["TIMESTAMP", "DATETIME"] and col.default_value_expression is None:
                if isinstance(row[col.name], int):
                    if row[col.name] > datetime(9999, 12, 31).timestamp():
                        row[col.name] = datetime.fromtimestamp(row[col.name] / 1_000_000).strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        )
                    else:
                        try:
                            row[col.name] = datetime.fromtimestamp(row[col.name]).strftime("%Y-%m-%d %H:%M:%S.%f")
                        except ValueError:
                            error_message = (
                                f"Error casting timestamp for destination '{self.destination_id}' column '{col.name}'. "
                                f"Invalid timestamp value: {row[col.name]} ({type(row[col.name])}). "
                                "Consider using a transformation."
                            )
                            logger.error(error_message)
                            raise ValueError(error_message)
        return row

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
                return self.bq_client.insert_rows_json(
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

        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}, type: {type(e)}")
            raise

    def load_to_bigquery_via_legacy_streaming(self, df_destination_records: pl.DataFrame) -> str:
        # Create table if it does not exist
        schema = self.get_bigquery_schema()
        table = bigquery.Table(self.table_id, schema=schema)
        time_partitioning = TimePartitioning(
            field=self.config.time_partitioning.field, type_=self.config.time_partitioning.type
        )
        table.time_partitioning = time_partitioning

        if self.clustering_keys and self.clustering_keys[self.destination_id]:
            table.clustering_fields = self.clustering_keys[self.destination_id]
        try:
            table = self.bq_client.create_table(table)
        except Conflict:
            table = self.bq_client.get_table(self.table_id)
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
            rows_to_insert = [
                self.safe_cast_record_values(row)
                for row in df_destination_records["source_data"].str.json_decode(infer_schema_length=None).to_list()
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
                len(current_batch) >= self.MAX_ROWS_PER_REQUEST
                or current_batch_size + item_size > self.MAX_REQUEST_SIZE_BYTES
            ):
                logger.debug(f"Yielding batch of {len(current_batch)} rows, size: {current_batch_size/1024/1024:.2f}MB")
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
                f"Yielding streaming batch of {len(current_batch)} rows, size: {current_batch_size/1024/1024:.2f}MB"
            )
            logger.debug(f"Yielding large rows batch of {len(large_rows)} rows")
            yield {"stream_batch": current_batch, "json_batch": large_rows}
