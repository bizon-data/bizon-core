import io
import os
import tempfile
import traceback
from typing import List, Tuple
from uuid import uuid4

import polars as pl
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import DatasetReference, TimePartitioning
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination, DestinationIteration
from bizon.engine.backend.backend import AbstractBackend
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.config import SourceSyncModes
from bizon.source.source import AbstractSourceCallback

from .config import BigQueryColumn, BigQueryConfigDetails
from .partitioning import describe, spec_from_table
from .table_naming import resolve_default_table_id


class BigQueryDestination(AbstractDestination):
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: BigQueryConfigDetails,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        super().__init__(sync_metadata, config, backend, source_callback, monitor)
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
        self._resolved_default_table_id: str | None = None

        # State for the async / batched load-job path (config.async_load).
        # _pending_files: uploaded GCS files not yet submitted to a load job.
        # _inflight_loads: submitted load jobs (FIFO, in iteration order) awaiting completion.
        self._pending_files: List[Tuple[str, DestinationIteration]] = []
        self._inflight_loads: List[dict] = []
        self._any_load_failed = False

        self._dataset_ensured = False
        self._temp_table_ensured = False

    @property
    def table_id(self) -> str:
        # Explicit destination_id (bare table name) is the user's choice -> never prefixed.
        if self.destination_id:
            return f"{self.project_id}.{self.dataset_id}.{self.destination_id}"
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

    def get_bigquery_schema(self, df_destination_records: pl.DataFrame = None) -> List[bigquery.SchemaField]:
        # Case we unnest the data
        if self.config.unnest:
            return [
                bigquery.SchemaField(
                    col.name,
                    col.type,
                    mode=col.mode,
                    description=col.description,
                )
                for col in self.record_schemas[self.destination_id]
            ]

        # Case we don't unnest the data
        else:
            # Metadata columns are NULLABLE (not REQUIRED): BigQuery forbids promoting an existing
            # NULLABLE column to REQUIRED, so a REQUIRED schema is rejected by load jobs against any
            # table whose metadata columns are already NULLABLE. bizon always populates these fields,
            # so NULLABLE drops only a BQ-level NOT NULL guarantee, not data.
            return [
                bigquery.SchemaField("_source_record_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_source_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("_source_data", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("_bizon_extracted_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField(
                    "_bizon_loaded_at", "TIMESTAMP", mode="NULLABLE", default_value_expression="CURRENT_TIMESTAMP()"
                ),
                bigquery.SchemaField("_bizon_id", "STRING", mode="NULLABLE"),
            ]

    def _ensure_dataset(self):
        """Ensure the target dataset exists before writing (checked once per run).

        Creates it when `create_dataset` is enabled; otherwise a missing dataset raises a
        clear error instead of letting load jobs fail with retried 404s mid-run.
        """
        if self._dataset_ensured:
            return

        dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
        except NotFound:
            if not self.config.create_dataset:
                raise RuntimeError(
                    f"BigQuery dataset {self.project_id}.{self.dataset_id} does not exist. "
                    f"Create it manually or set `create_dataset: true` on the destination config."
                )
            logger.info(f"Dataset {self.project_id}.{self.dataset_id} not found, creating it ...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.dataset_location
            self.bq_client.create_dataset(dataset, exists_ok=True)

        self._dataset_ensured = True

    def _ensure_clean_temp_table(self):
        """Drop a stale temp table once per run, before the first load.

        Loads always WRITE_APPEND into the temp table, so rows left behind by an earlier crashed run
        would be published by finalize()'s WRITE_TRUNCATE copy and end up in a table the user asked to
        be replaced. This applies to every run that publishes with WRITE_TRUNCATE, which is exactly the
        runs staging into `_temp`: a plain full refresh, and a reset (which from_bizon_config maps onto
        full_refresh precisely so it reuses this path). A plain incremental stages into `_incremental`
        and appends across runs, so it must be left alone.
        """
        if self._temp_table_ensured or self.sync_metadata.sync_mode != SourceSyncModes.FULL_REFRESH:
            return

        self._temp_table_ensured = True

        # A run that already wrote cursors is being resumed after a crash: the producer restarts from
        # the last destination cursor, so the temp table holds iterations it will not re-fetch.
        if self.backend.get_last_cursor_by_job_id(job_id=self.sync_metadata.job_id) is not None:
            logger.info(f"Resuming an in-flight run, keeping temp table {self.temp_table_id} ...")
            return

        logger.info(f"Dropping stale temp table {self.temp_table_id} ...")
        self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)

    def check_connection(self) -> bool:
        self._ensure_dataset()
        # Fail before uploading anything if the partition column cannot exist. Skipped when the
        # unnest schema for this destination_id is not resolvable yet (the stream runner injects
        # record_schemas after config validation), in which case the load path checks it instead.
        if not self.config.unnest or (self.record_schemas and self.destination_id in self.record_schemas):
            self._partition_field_or_raise()
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
            pl.DataFrame(df_destination_records["source_data"].str.json_decode(infer_schema_length=None))
            .schema["source_data"]
            .fields
        )

        record_schema_fields = [col.name for col in record_schema]

        for field in source_data_fields:
            assert field.name in record_schema_fields, f"Column {field.name} not found in BigQuery schema"

        # Parse the JSON and unnest the fields to polar type
        return df_destination_records.select(
            pl.col("source_data").str.json_path_match(f"$.{col.name}").cast(col.polars_type).alias(col.name)
            for col in record_schema
        )

    @staticmethod
    def _rename_for_bq(df_destination_records: pl.DataFrame) -> pl.DataFrame:
        """Rename bizon/source fields to their underscore-prefixed BigQuery column names."""
        return df_destination_records.rename(
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

    def _partition_field_or_raise(self) -> str:
        """The configured partition column, checked against the schema this run actually writes.

        `get_bigquery_schema()` returns the user's own columns in unnest mode, where the default
        `_bizon_loaded_at` need not exist. Handing an unknown column to a load job fails with an
        opaque BigQuery error part-way through a run, after data is already staged in GCS.

        `BigQueryConfigDetails` validates the same thing at config load for the better message, but
        cannot be the only check: the stream runner assigns `record_schemas` after validation.
        """
        field = self.config.time_partitioning.field

        if field is None:
            return None  # Ingestion-time partitioning.

        column_names = {column.name for column in self.get_bigquery_schema()}
        if field not in column_names:
            raise ValueError(
                f"Partition field '{field}' is not in the schema written to {self.temp_table_id} "
                f"(columns: {sorted(column_names)}). Fix `destination.config.time_partitioning.field`, "
                f"add the column to the record_schema, or set it to null for ingestion-time partitioning."
            )
        return field

    def _build_load_job_config(self) -> bigquery.LoadJobConfig:
        # NB: `TimePartitioning` here is google.cloud.bigquery's (imported at the top of this module),
        # not the pydantic model of the same name in .config, which is reached via self.config.
        return bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=self.get_bigquery_schema(),
            # Self-heal schema drift: relax any pre-existing REQUIRED column to NULLABLE and tolerate
            # additive source-schema changes, so tables created by older code converge without a
            # manual migration. Relaxation is one-way (REQUIRED -> NULLABLE) and safe.
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ],
            time_partitioning=TimePartitioning(
                field=self._partition_field_or_raise(),
                type_=self.config.time_partitioning.type.value,
            ),
        )

    def _gcs_uri(self, gcs_file: str) -> str:
        return f"gs://{self.buffer_bucket_name}/{gcs_file}"

    def load_to_bigquery(self, gcs_file: str, df_destination_records: pl.DataFrame = None):
        load_job = self.bq_client.load_table_from_uri(
            self._gcs_uri(gcs_file), self.temp_table_id, job_config=self._build_load_job_config()
        )
        result = load_job.result()  # Waits for the job to complete
        assert result.state == "DONE", f"Job failed with state {result.state} with error {result.error_result}"

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        self._ensure_dataset()
        self._ensure_clean_temp_table()
        gs_file_name = self.convert_and_upload_to_buffer(
            df_destination_records=self._rename_for_bq(df_destination_records)
        )

        try:
            self.load_to_bigquery(gcs_file=gs_file_name)
            self.cleanup(gs_file_name)
        except Exception as e:
            self.cleanup(gs_file_name)
            logger.error(f"Error loading data to BigQuery: {e}")
            logger.error(traceback.format_exc())
            return False, str(e)
        return True, ""

    # ------------------------------------------------------------------
    # Async / batched load-job path (config.async_load)
    # ------------------------------------------------------------------
    def buffer_flush_handler(self, session=None) -> DestinationIteration:
        """Override the synchronous flush handler with an async, batched load path.

        Each flush uploads the buffer to GCS without blocking on a load job. Files are
        batched into fewer load jobs to cut the per-table load-job quota, and cursors are
        created only once a load lands (preserving the at-least-once recovery contract).
        """
        if not self.config.async_load:
            return super().buffer_flush_handler(session=session)

        self._ensure_dataset()
        self._ensure_clean_temp_table()

        # Snapshot iteration metadata before the buffer is flushed by the caller.
        destination_iteration = DestinationIteration(
            success=False,
            records_written=self.buffer.df_destination_records.height,
            pagination=self.buffer.pagination,
            from_source_iteration=self.buffer.from_iteration,
            to_source_iteration=self.buffer.to_iteration,
        )

        gcs_file = self.convert_and_upload_to_buffer(
            df_destination_records=self._rename_for_bq(self.buffer.df_destination_records)
        )
        self._pending_files.append((gcs_file, destination_iteration))

        if len(self._pending_files) >= self.config.load_files_per_job:
            self._submit_pending_load()

        self._enforce_max_in_flight()
        self._reap_landed_loads()

        # Optimistically successful unless an already-reaped load failed. The last flush's
        # own load is drained in finalize() — the same failure window as the existing copy.
        destination_iteration.success = not self._any_load_failed
        return destination_iteration

    def _submit_pending_load(self):
        """Submit the accumulated GCS files as a single (non-blocking) load job."""
        if not self._pending_files:
            return

        gcs_files = [f for f, _ in self._pending_files]
        iterations = [di for _, di in self._pending_files]
        uris = [self._gcs_uri(f) for f in gcs_files]

        load_job = self.bq_client.load_table_from_uri(
            uris, self.temp_table_id, job_config=self._build_load_job_config()
        )
        logger.info(f"Submitted async load job for {len(uris)} file(s) into {self.temp_table_id}")
        self._inflight_loads.append({"job": load_job, "gcs_files": gcs_files, "iterations": iterations})
        self._pending_files = []

    def _complete_load(self, entry: dict):
        """Finalize a single (FIFO) load job: wait for it, create its cursors, clean up its GCS files."""
        success = True
        error_message = None
        try:
            result = entry["job"].result()  # Waits if not yet done
            if getattr(result, "state", "DONE") != "DONE":
                success = False
                error_message = f"Load job failed with state {result.state}"
        except Exception as e:
            success = False
            error_message = str(e)
            logger.error(f"Async load job failed: {e}")

        for destination_iteration in entry["iterations"]:
            destination_iteration.success = success
            destination_iteration.error_message = error_message
            self.create_cursors(destination_iteration=destination_iteration)

        for gcs_file in entry["gcs_files"]:
            self.cleanup(gcs_file)

        if not success:
            # Fail fast. Loads are reaped strictly in iteration order, so at this point every
            # cursor already written is a contiguous successful prefix and this batch's cursor is
            # marked failed. Aborting prevents any later (higher-iteration) batch from writing a
            # success cursor that would create a gap -- recovery resumes from the last contiguous
            # success (get_last_cursor_by_job_id) and re-fetches this range, so no records are lost.
            self._any_load_failed = True
            raise RuntimeError(f"BigQuery async load job failed, aborting to preserve cursors: {error_message}")

    def _reap_landed_loads(self):
        """Complete any in-flight load jobs that have landed, in submission order."""
        while self._inflight_loads and self._inflight_loads[0]["job"].done():
            self._complete_load(self._inflight_loads.pop(0))

    def _enforce_max_in_flight(self):
        """Back-pressure: block on the oldest job once we exceed the in-flight limit."""
        while len(self._inflight_loads) >= self.config.load_max_in_flight_jobs:
            self._complete_load(self._inflight_loads.pop(0))

    def _drain_all_loads(self):
        """Submit any remaining files and wait for all in-flight loads to complete."""
        self._submit_pending_load()
        while self._inflight_loads:
            self._complete_load(self._inflight_loads.pop(0))

    def _check_destination_partitioning(self):
        """Warn when publishing will not give the destination table the configured partitioning.

        BigQuery cannot change an existing table's partitioning spec: a copy job into a table that
        already exists silently keeps *that table's* spec. So a table created before 0.4.0 by
        `CREATE TABLE AS SELECT` is unpartitioned and stays unpartitioned run after run, however
        `time_partitioning` is configured, with nothing in the logs to say so.

        The only fix is to drop and recreate, which is what `enforce_partitioning` does — and only
        on a full refresh, where the temp table already holds every row that will exist.
        """
        try:
            temp_table = self.bq_client.get_table(self.temp_table_id)
        except NotFound:
            # Nothing was staged (a run that produced no records never creates the temp table).
            # Returning here is load-bearing: it is what makes it impossible for enforce_partitioning
            # to drop the destination table on a run that has nothing to publish in its place.
            return

        try:
            destination_table = self.bq_client.get_table(self.table_id)
        except NotFound:
            return  # First run: the copy job creates the table with the temp table's partitioning.

        # Compare the two tables' actual specs rather than config against the table: the temp table's
        # spec is precisely what a fresh copy would produce, and stays truthful if plumbing drifts.
        staged = spec_from_table(temp_table)
        current = spec_from_table(destination_table)

        if staged == current:
            return

        logger.warning(
            f"Partitioning mismatch on {self.table_id}: the table is {describe(current)}, but this run "
            f"stages data {describe(staged)}. BigQuery cannot change an existing table's partitioning, "
            f"so publishing leaves it {describe(current)}."
        )

        if not self.config.enforce_partitioning:
            logger.warning(
                f"Set `enforce_partitioning: true` on the destination config to have a full refresh drop and "
                f"rebuild {self.table_id} with the configured partitioning."
            )
            return

        if self.sync_metadata.sync_mode != SourceSyncModes.FULL_REFRESH:
            logger.warning(
                f"`enforce_partitioning` only rebuilds on a full refresh, which republishes every row. This run "
                f"is {self.sync_metadata.sync_mode} and stages only its own delta, so rebuilding here would "
                f"discard history. Run `bizon stream reset <config>` to rebuild {self.table_id} with the "
                f"configured partitioning; incremental resumes from that run. This needs a source that can "
                f"re-fetch the full stream -- otherwise repartition the table by hand."
            )
            return

        logger.warning(
            f"enforce_partitioning: dropping {self.table_id} so the publish copy job recreates it "
            f"{describe(staged)}. The table is absent until the copy job lands, and table-level metadata "
            f"(description, labels, ACLs, policy tags) is not recreated."
        )
        self.bq_client.delete_table(self.table_id, not_found_ok=True)

    def _copy_temp_to_main(self, write_disposition: str):
        """Materialize the temp table into the main table with a copy job.

        Copy jobs are free (metadata-only) and near-instant, unlike DML
        (CREATE OR REPLACE / INSERT INTO ... SELECT *) which scans + rewrites
        the whole temp table and is billed on bytes processed. A copy job also
        gives a *newly created* main table the temp table's partitioning and clustering --
        but it cannot change the spec of a main table that already exists, which is what
        _check_destination_partitioning() reports on.
        """
        job_config = bigquery.CopyJobConfig(write_disposition=write_disposition)
        copy_job = self.bq_client.copy_table(self.temp_table_id, self.table_id, job_config=job_config)
        try:
            result = copy_job.result()  # Waits for the job to complete
        except BadRequest as e:
            message = str(e)
            if "artitioning" in message or "lustering" in message:
                raise RuntimeError(
                    f"BigQuery refused to publish {self.temp_table_id} into {self.table_id}: the two tables "
                    f"have incompatible partitioning or clustering. Set `enforce_partitioning: true` and run a "
                    f"full refresh (or `bizon stream reset <config>` for an incremental stream) to rebuild "
                    f"{self.table_id}. Original error: {message}"
                ) from e
            raise
        logger.info(f"BigQuery copy job ({write_disposition}) {self.temp_table_id} -> {self.table_id}: {result}")

    def finalize(self):
        # Drain any async load jobs (and still-pending files) before publishing the table.
        if self.config.async_load:
            self._drain_all_loads()

        if self.sync_metadata.sync_mode in (SourceSyncModes.FULL_REFRESH, SourceSyncModes.INCREMENTAL):
            self._check_destination_partitioning()

        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            logger.info(f"Replacing {self.table_id} with temp table {self.temp_table_id} via copy job ...")
            self._copy_temp_to_main(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            # Check if the destination table exists by fetching it; raise if it doesn't exist
            try:
                self.bq_client.get_table(self.table_id)
            except NotFound:
                logger.error(f"Table {self.table_id} not found")
                raise Exception(f"Table {self.table_id} not found")
            # Cleanup
            logger.info(f"Deleting temp table {self.temp_table_id} ...")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL:
            # WRITE_APPEND creates the main table on the first run (preserving the
            # temp table's partitioning) and appends to it on subsequent runs.
            logger.info(f"Appending temp table {self.temp_table_id} to {self.table_id} via copy job ...")
            self._copy_temp_to_main(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            logger.info(f"Deleting incremental temp table {self.temp_table_id} ...")
            self.bq_client.delete_table(self.temp_table_id, not_found_ok=True)
            return True

        elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM:
            # Nothing to do as we write directly to the final table
            return True
