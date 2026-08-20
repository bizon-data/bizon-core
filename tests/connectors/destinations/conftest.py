"""Shared fixtures for the BigQuery destination tests (CI-safe, no live BigQuery).

`build_bq_destination` constructs any of the three BigQuery destinations with the google clients
patched, then swaps in a controllable `bq_client` -- the same shape as the harness in
`bigquery/test_table_naming.py`, extended with a per-table-id `get_table` so partitioning specs can
differ between the staging table and the destination table.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import BigQueryConfigDetails
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.connectors.destinations.bigquery_streaming.src.config import BigQueryStreamingConfigDetails
from bizon.connectors.destinations.bigquery_streaming.src.destination import BigQueryStreamingDestination
from bizon.connectors.destinations.bigquery_streaming_v2.src.config import BigQueryStreamingV2ConfigDetails
from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import BigQueryStreamingV2Destination

BQ_PROJECT = "my_project"
BQ_DATASET = "bizon_test"
BQ_SOURCE = "cookie"
BQ_STREAM = "test"

BQ_VARIANT_MODULES = {
    "bigquery": "bizon.connectors.destinations.bigquery.src.destination",
    "streaming": "bizon.connectors.destinations.bigquery_streaming.src.destination",
    "streaming_v2": "bizon.connectors.destinations.bigquery_streaming_v2.src.destination",
}


def make_bq_sync_metadata(sync_mode: str = "full_refresh") -> SyncMetadata:
    return SyncMetadata(
        name="test_job",
        job_id="test_job_id",
        source_name=BQ_SOURCE,
        stream_name=BQ_STREAM,
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode=sync_mode,
    )


def _make_bq_table(time_partitioning=None, clustering_fields=None, schema=None) -> MagicMock:
    """A stand-in for `google.cloud.bigquery.Table` carrying only what the code under test reads."""
    table = MagicMock()
    table.time_partitioning = time_partitioning
    table.clustering_fields = clustering_fields
    table.schema = schema or []
    return table


def _partitioned(field="_bizon_loaded_at", window="DAY") -> bigquery.TimePartitioning:
    return bigquery.TimePartitioning(field=field, type_=window)


# Exposed as fixtures rather than imported: `tests/` has no __init__.py, so sibling test modules
# cannot import from conftest directly.
@pytest.fixture
def make_bq_table():
    return _make_bq_table


@pytest.fixture
def partitioned():
    return _partitioned


@pytest.fixture
def bq_ids():
    """The table ids the harness resolves to, so tests can key `get_table` on them."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_SOURCE}_{BQ_STREAM}"
    return {
        "table": table_id,
        "temp": f"{table_id}_temp",
        "incremental": f"{table_id}_incremental",
    }


@pytest.fixture
def build_bq_destination():
    """Factory yielding a destination whose `bq_client.get_table` is driven by `tables`.

    `tables` maps a table id to the object `get_table` should return; anything absent raises
    NotFound. Pass `table_prefix=""` by default so ids stay predictable (no prefix lookup).
    """

    @contextmanager
    def _build(variant: str, sync_mode: str = "full_refresh", tables: dict = None, **config_overrides):
        module = BQ_VARIANT_MODULES[variant]
        config_overrides.setdefault("table_prefix", "")
        mock_args = dict(
            sync_metadata=make_bq_sync_metadata(sync_mode),
            backend=MagicMock(),
            source_callback=MagicMock(),
            monitor=MagicMock(),
        )

        if variant == "bigquery":
            config = BigQueryConfigDetails(
                project_id=BQ_PROJECT, dataset_id=BQ_DATASET, gcs_buffer_bucket="bizon-buffer", **config_overrides
            )
            with patch(f"{module}.bigquery.Client"), patch(f"{module}.storage.Client"):
                destination = BigQueryDestination(config=config, **mock_args)
        elif variant == "streaming":
            config = BigQueryStreamingConfigDetails(project_id=BQ_PROJECT, dataset_id=BQ_DATASET, **config_overrides)
            with patch(f"{module}.bigquery.Client"), patch(f"{module}.bigquery_storage_v1.BigQueryWriteClient"):
                destination = BigQueryStreamingDestination(config=config, **mock_args)
        elif variant == "streaming_v2":
            config = BigQueryStreamingV2ConfigDetails(project_id=BQ_PROJECT, dataset_id=BQ_DATASET, **config_overrides)
            with patch(f"{module}.bigquery.Client"):
                destination = BigQueryStreamingV2Destination(config=config, **mock_args)
        else:
            raise ValueError(variant)

        known_tables = tables or {}

        def get_table(table_id):
            if table_id in known_tables:
                return known_tables[table_id]
            raise NotFound(f"{table_id} not found")

        destination.bq_client = MagicMock()
        destination.bq_client.get_table.side_effect = get_table
        yield destination

    return _build


@pytest.fixture
def loguru_warnings():
    """Collect WARNING+ log messages emitted during the test."""
    messages = []
    handler_id = logger.add(lambda message: messages.append(message), level="WARNING", format="{message}")
    yield messages
    logger.remove(handler_id)
