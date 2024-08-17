import io
import os
import tempfile
from typing import List
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import DatasetReference
from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.destinations.models import DestinationRecord
from bizon.source.models import SourceRecord

from .config import BigQueryConfig


class BigQueryDestination(AbstractDestination):
    def __init__(self, source_name: str, stream_name: str, config: BigQueryConfig):
        super().__init__(source_name, stream_name, config)

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

    def convert_and_upload_to_buffer(self, source_records: List[SourceRecord]):

        bizon_records = [
            DestinationRecord.from_source_record(source_record).model_dump() for source_record in source_records
        ]

        logger.info(bizon_records[0])

        df = pd.DataFrame(bizon_records)

        # Convert DataFrame to Parquet in-memory
        if self.buffer_format == "parquet":
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            # Upload the Parquet file to GCS
            file_name = f"{self.source_name}/{self.stream_name}/{str(uuid4())}.parquet"
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
    def load_to_bigquery(self, gcs_file: str):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("bizon_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("bizon_loaded_at", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_data", "STRING", mode="NULLABLE"),
            ],
        )

        table_id = f"{self.project_id}.{self.dataset_id}.{self.source_name}_{self.stream_name}"

        load_job = self.bq_client.load_table_from_uri(
            f"gs://{self.buffer_bucket_name}/{gcs_file}", table_id, job_config=job_config
        )
        load_job.result()

    def write_records(self, source_records: List[SourceRecord]):

        # Here we can check if these IDs are already present in BigQuery
        # Using SourceRecord.id values

        gs_file_name = self.convert_and_upload_to_buffer(source_records=source_records)

        try:
            self.load_to_bigquery(gs_file_name)
            self.cleanup(gs_file_name)
        except Exception as e:
            self.cleanup(gs_file_name)
            raise e
        return True
