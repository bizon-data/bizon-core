import io
import os
import tempfile
import traceback
from typing import List, Tuple
from uuid import uuid4

import polars as pl
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import DatasetReference, TimePartitioning
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.source.config import SourceSyncModes

from .config import BigQueryColumn, BigQueryConfigDetails


class BigQueryDestination(AbstractDestination):

    def __init__(self, sync_metadata: SyncMetadata, config: BigQueryConfigDetails, backend: AbstractBackend):
        super().__init__(sync_metadata, config, backend)
        self.config: BigQueryConfigDetails = config

        if config.authentication and config.authentication.service_account_key:
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.authentication.service_account_key.encode())
                temp_file_path = temp.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path

        self.project_id = config.project_id
        self.bq_client = bigquery.Client(project=self.project_id)
        self.gcs_client = storage.Client(project=self.project_id)
        self.buffer_bucket_name = config.gcs_buffer_bucket
        self.buffer_bucket = self.gcs_client.bucket(config.gcs_buffer_bucket)
        self.buffer_format = config.gcs_buffer_format
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location

    @property
    def table_id(self) -> str:
        tabled_id = self.config.table_id or f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return f"{self.project_id}.{self.dataset_id}.{tabled_id}"

    @property
    def temp_table_id(self) -> str:

        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            return f"{self.table_id}_temp"

        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            return f"{self.table_id}_incremental"

        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            return f"{self.table_id}"

    def get_bigquery_schema(self, df_destination_records: pl.DataFrame) -> List[bigquery.SchemaField]:

        # Case we unnest the data
        if self.config.unnest:
            return [
                bigquery.SchemaField(
                    col.name,
                    col.type,
                    mode=col.mode,
                    description=col.description,
                )
                for col in self.config.record_schema
            ]

        # Case we don't unnest the data
        else:
            return [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
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

    def cleanup(self, gcs_file: str):
        blob = self.buffer_bucket.blob(gcs_file)
        blob.delete()

    # TO DO: Add backoff to common exceptions => looks like most are hanlded by the client
    # https://cloud.google.com/python/docs/reference/storage/latest/retry_timeout
    # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dbapi.DataError

    def convert_and_upload_to_buffer(self, df_destination_records: pl.DataFrame) -> str:

        if self.buffer_format == "parquet":

            # Upload the Parquet file to GCS
            file_name = f"{self.sync_metadata.source_name}/{self.sync_metadata.stream_name}/{str(uuid4())}.parquet"

            with io.BytesIO() as stream:
                df_destination_records.write_parquet(stream)
                stream.seek(0)

                blob = self.buffer_bucket.blob(file_name)
                blob.upload_from_file(stream, content_type="application/octet-stream")

            return file_name

        raise NotImplementedError(f"Buffer format {self.buffer_format} is not supported")

    @staticmethod
    def unnest_data(df_destination_records: pl.DataFrame, record_schema: list[BigQueryColumn]) -> pl.DataFrame:
        """Unnest the source_data field into separate columns"""

        # Check if the schema matches the expected schema
        source_data_fields = (
            pl.DataFrame(df_destination_records["source_data"].str.json_decode()).schema["source_data"].fields
        )

        record_schema_fields = [col.name for col in record_schema]

        for field in source_data_fields:
            assert field.name in record_schema_fields, f"Column {field.name} not found in BigQuery schema"

        # Parse the JSON and unnest the fields to polar type
        return df_destination_records.select(
            pl.col("source_data").str.json_path_match(f"$.{col.name}").cast(col.polars_type).alias(col.name)
            for col in record_schema
        )

    def load_to_bigquery(self, gcs_file: str, df_destination_records: pl.DataFrame):

        # We always partition by the loaded_at field
        time_partitioning = TimePartitioning(field="_bizon_loaded_at", type_=self.config.time_partitioning)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=self.get_bigquery_schema(df_destination_records=df_destination_records),
            time_partitioning=time_partitioning,
        )

        load_job = self.bq_client.load_table_from_uri(
            f"gs://{self.buffer_bucket_name}/{gcs_file}", self.temp_table_id, job_config=job_config
        )
        result = load_job.result()  # Waits for the job to complete
        assert result.state == "DONE", f"Job failed with state {result.state} with error {result.error_result}"

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:

        # Rename fields to match BigQuery schema
        df_destination_records = df_destination_records.rename(
            {
                # Bizon fields
                "bizon_extracted_at": "_bizon_extracted_at",
                "bizon_id": "_bizon_id",
                "bizon_loaded_at": "_bizon_loaded_at",
                # Source fields
                "source_record_id": "_source_record_id",
                "source_timestamp": "_source_timestamp",
                "source_data": "_source_data",
            },
        )

        gs_file_name = self.convert_and_upload_to_buffer(df_destination_records=df_destination_records)

        try:
            self.load_to_bigquery(gcs_file=gs_file_name, df_destination_records=df_destination_records)
            self.cleanup(gs_file_name)
        except Exception as e:
            self.cleanup(gs_file_name)
            logger.error(f"Error loading data to BigQuery: {e}")
            logger.error(traceback.format_exc())
            return False, str(e)
        return True, ""

    def finalize(self):
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            logger.info(f"Loading temp table {self.temp_table_id} data into {self.table_id} ...")
            self.bq_client.query(f"CREATE OR REPLACE TABLE {self.table_id} AS SELECT * FROM {self.temp_table_id}")
            logger.info(f"Deleting temp table {self.temp_table_id} ...")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            # TO DO: Implement incremental sync
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            # Nothing to do as we write directly to the final table
            return True
