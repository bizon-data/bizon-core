import io
import os
import tempfile
from typing import List, Tuple
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import DatasetReference
from loguru import logger
from pytz import UTC

from bizon.common.models import SyncMetadata
from bizon.destinations.config import NormalizationType
from bizon.destinations.destination import AbstractDestination
from bizon.destinations.models import DestinationRecord
from bizon.engine.backend.backend import AbstractBackend

from .config import BigQueryConfigDetails


class BigQueryDestination(AbstractDestination):

    def __init__(self, sync_metadata: SyncMetadata, config: BigQueryConfigDetails, backend: AbstractBackend):
        super().__init__(sync_metadata, config, backend)
        self.config: BigQueryConfigDetails = config

        if bool(config.service_account_key):
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp.write(config.service_account_key.encode())
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

    def delete_table(self) -> bool:
        self.bq_client.delete_table(self.table_id, not_found_ok=True)  # Make an API request.
        return True

    def get_bigquery_schema(self, destination_records: List[DestinationRecord]) -> List[bigquery.SchemaField]:

        # we keep raw data in the column source_data
        if self.config.normalization.type == NormalizationType.NONE:
            return [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                # bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

        # If normalization is tabular, we parse key / value pairs to columns
        elif self.config.normalization.type == NormalizationType.TABULAR:
            first_record_keys = destination_records[0].source_data.keys()
            return [bigquery.SchemaField(key, "STRING", mode="NULLABLE") for key in first_record_keys] + [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                # bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

        raise NotImplementedError(f"Normalization type {self.config.normalization.type} is not supported")

    def get_batch_records_as_df(self, destination_records: List[DestinationRecord]) -> pd.DataFrame:

        # We keep raw data in a column -> convert the SourceRecord to a DestinationRecord
        if self.config.normalization.type == NormalizationType.NONE:
            df = pd.DataFrame([record.to_dict_raw_json_data(parquet=True) for record in destination_records])
            df["_bizon_loaded_at"] = pd.Timestamp.now(tz=UTC)

        # If normalization is tabular, we can just convert the data to a DataFrame parsing first-level keys
        elif self.config.normalization.type == NormalizationType.TABULAR:
            list_data_dict = [record.source_data for record in destination_records]
            df = pd.DataFrame(list_data_dict).astype(str)
            df["_bizon_id"] = [uuid4().hex for _ in range(len(destination_records))]
            # df["_bizon_extracted_at"] = [pd.to_datetime(record.bizon_extracted_at) for record in destination_records]
            df["_bizon_loaded_at"] = pd.Timestamp.now(tz=UTC)
            df["_source_record_id"] = [record.source_record_id for record in destination_records]

        else:
            raise NotImplementedError(f"Normalization type {self.config.normalization.type} is not supported")

        return df

    def convert_and_upload_to_buffer(self, destination_records: List[DestinationRecord]):

        df = self.get_batch_records_as_df(destination_records)

        # Convert DataFrame to Parquet in-memory
        if self.buffer_format == "parquet":
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Upload the Parquet file to GCS
            file_name = f"{self.sync_metadata.source_name}/{self.sync_metadata.stream_name}/{str(uuid4())}.parquet"
            blob = self.buffer_bucket.blob(file_name)
            blob.upload_from_file(buffer, content_type="application/octet-stream")
            return file_name

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

    def load_to_bigquery(self, gcs_file: str, destination_records: List[DestinationRecord]):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=self.get_bigquery_schema(destination_records=destination_records),
        )

        load_job = self.bq_client.load_table_from_uri(
            f"gs://{self.buffer_bucket_name}/{gcs_file}", self.table_id, job_config=job_config
        )
        load_job.result()

    def write_records(self, destination_records: List[DestinationRecord]) -> Tuple[bool, str]:

        # Here we can check if these IDs are already present in BigQuery
        # Using SourceRecord.id values

        gs_file_name = self.convert_and_upload_to_buffer(destination_records=destination_records)

        try:
            self.load_to_bigquery(gs_file_name, destination_records=destination_records)
            self.cleanup(gs_file_name)
        except Exception as e:
            self.cleanup(gs_file_name)
            raise e
        return True, ""
