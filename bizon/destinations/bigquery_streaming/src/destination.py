import json
import os
import tempfile
from typing import List, Tuple

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage_v1, storage
from google.cloud.bigquery import DatasetReference, TimePartitioning
from google.cloud.bigquery_storage_v1.types import AppendRowsRequest, ProtoRows
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destinations.config import NormalizationType
from bizon.destinations.destination import AbstractDestination
from bizon.destinations.models import DestinationRecord
from bizon.engine.backend.backend import AbstractBackend

from .config import BigQueryConfigDetails
from .proto_utils import get_proto_schema_and_class


class BigQueryStreamingDestination(AbstractDestination):

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
        self.bq_storage_client = bigquery_storage_v1.BigQueryWriteClient()
        self.gcs_client = storage.Client(project=self.project_id)
        self.dataset_id = config.dataset_id
        self.dataset_location = config.dataset_location

    @property
    def table_id(self) -> str:
        tabled_id = self.config.table_id or f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        return f"{self.project_id}.{self.dataset_id}.{tabled_id}"

    def get_bigquery_schema(self, destination_records: List[DestinationRecord]) -> List[bigquery.SchemaField]:

        # we keep raw data in the column source_data
        if self.config.normalization.type == NormalizationType.NONE:
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

        elif self.config.normalization.type == NormalizationType.DEBEZIUM:
            assert (
                "_bizon_message_key" in destination_records[0].source_data
            ), "Debezium records must have a '_bizon_message_key' key"
            message_keys = json.loads(destination_records[0].source_data["_bizon_message_key"])
            return [bigquery.SchemaField(key, "STRING", mode="NULLABLE") for key in message_keys] + [
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"
                ),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

        # If normalization is tabular, we parse key / value pairs to columns
        elif self.config.normalization.type == NormalizationType.TABULAR:
            first_record_keys = destination_records[0].source_data.keys()
            return [bigquery.SchemaField(key, "STRING", mode="NULLABLE") for key in first_record_keys] + [
                bigquery.SchemaField("_source_record_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField(
                    "_bizon_loaded_at", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP()"
                ),
                bigquery.SchemaField("_bizon_id", "STRING", mode="REQUIRED"),
            ]

        raise NotImplementedError(f"Normalization type {self.config.normalization.type} is not supported")

    def check_connection(self) -> bool:
        dataset_ref = DatasetReference(self.project_id, self.dataset_id)

        try:
            self.bq_client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.dataset_location
            dataset = self.bq_client.create_dataset(dataset)
        return True

    def load_to_bigquery_via_streaming(self, destination_records: List[DestinationRecord]) -> str:
        clustering_keys = []

        if self.config.normalization.type == NormalizationType.DEBEZIUM:
            clustering_keys = list(json.loads(destination_records[0].source_data["_bizon_message_key"]).keys())

        # Create table if it doesnt exist
        schema = self.get_bigquery_schema(destination_records=destination_records)
        table = bigquery.Table(self.table_id, schema=schema)
        time_partitioning = TimePartitioning(field="_bizon_loaded_at", type_=self.config.time_partitioning)
        table.time_partitioning = time_partitioning

        if clustering_keys:
            table.clustering_fields = clustering_keys

        table = self.bq_client.create_table(table, exists_ok=True)

        # Create the stream
        write_client = self.bq_storage_client
        tabled_id = self.config.table_id or f"{self.sync_metadata.source_name}_{self.sync_metadata.stream_name}"
        parent = write_client.table_path(self.project_id, self.dataset_id, tabled_id)
        stream_name = f"{parent}/_default"

        # Generating the protocol buffer representation of the message descriptor.
        proto_schema, TableRow = get_proto_schema_and_class(clustering_keys)

        serialized_rows = [
            record.to_protobuf_serialization(
                TableRow, debezium=self.config.normalization.type == NormalizationType.DEBEZIUM
            )
            for record in destination_records
        ]

        request = AppendRowsRequest(
            write_stream=stream_name,
            proto_rows=AppendRowsRequest.ProtoData(
                rows=ProtoRows(serialized_rows=serialized_rows),
                writer_schema=proto_schema,
            ),
        )
        response = write_client.append_rows(iter([request]))
        assert response.code().name == "OK"

    def write_records(self, destination_records: List[DestinationRecord]) -> Tuple[bool, str]:
        self.load_to_bigquery_via_streaming(destination_records=destination_records)
        return True, ""
