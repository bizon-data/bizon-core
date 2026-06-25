"""Unit tests for the `_bizon_` table-prefix resolution (CI-safe, no live BigQuery)."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import Forbidden, NotFound

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.bigquery.src.config import (
    BigQueryConfig,
    BigQueryConfigDetails,
)
from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination
from bizon.connectors.destinations.bigquery.src.table_naming import (
    BIZON_TABLE_PREFIX,
    resolve_default_table_id,
)
from bizon.connectors.destinations.bigquery_streaming.src.config import BigQueryStreamingConfigDetails
from bizon.connectors.destinations.bigquery_streaming.src.destination import BigQueryStreamingDestination
from bizon.connectors.destinations.bigquery_streaming_v2.src.config import BigQueryStreamingV2ConfigDetails
from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import BigQueryStreamingV2Destination
from bizon.destination.config import DestinationTypes

PROJECT = "my_project"
DATASET = "bizon_test"
SOURCE = "cookie"
STREAM = "test"
BASE_NAME = f"{SOURCE}_{STREAM}"
LEGACY_ID = f"{PROJECT}.{DATASET}.{BASE_NAME}"
PREFIXED_ID = f"{PROJECT}.{DATASET}.{BIZON_TABLE_PREFIX}{BASE_NAME}"


def make_bq_client(get_table_side_effect):
    client = MagicMock()
    client.get_table.side_effect = get_table_side_effect
    return client


# ---------------------------------------------------------------------------
# resolve_default_table_id: the core logic
# ---------------------------------------------------------------------------


def test_resolve_reuses_existing_legacy_table():
    """An existing unprefixed table is reused (backwards compatibility)."""
    client = make_bq_client(get_table_side_effect=lambda table_id: MagicMock())
    resolved = resolve_default_table_id(client, PROJECT, DATASET, BASE_NAME)
    assert resolved == LEGACY_ID
    client.get_table.assert_called_once_with(LEGACY_ID)


def test_resolve_prefixes_brand_new_table():
    """When no legacy table exists, the `_bizon_` prefix is applied."""
    client = make_bq_client(get_table_side_effect=NotFound("missing"))
    resolved = resolve_default_table_id(client, PROJECT, DATASET, BASE_NAME)
    assert resolved == PREFIXED_ID


def test_resolve_empty_prefix_disables_prefixing():
    """An empty prefix keeps the historical (unprefixed) name and skips the lookup."""
    client = make_bq_client(get_table_side_effect=NotFound("missing"))
    resolved = resolve_default_table_id(client, PROJECT, DATASET, BASE_NAME, prefix="")
    assert resolved == LEGACY_ID
    client.get_table.assert_not_called()


def test_resolve_non_notfound_error_falls_back_to_legacy():
    """Any non-NotFound error falls back to the legacy name (== current behavior, no regression)."""
    client = make_bq_client(get_table_side_effect=Forbidden("no permission"))
    resolved = resolve_default_table_id(client, PROJECT, DATASET, BASE_NAME)
    assert resolved == LEGACY_ID


def test_resolve_custom_prefix():
    client = make_bq_client(get_table_side_effect=NotFound("missing"))
    resolved = resolve_default_table_id(client, PROJECT, DATASET, BASE_NAME, prefix="staging_")
    assert resolved == f"{PROJECT}.{DATASET}.staging_{BASE_NAME}"


# ---------------------------------------------------------------------------
# table_id wiring across the three destination variants
# ---------------------------------------------------------------------------


@pytest.fixture
def sync_metadata() -> SyncMetadata:
    return SyncMetadata(
        name="gcs_loading",
        job_id="rfou98C9DJH",
        source_name=SOURCE,
        stream_name=STREAM,
        destination_name="bigquery",
        destination_alias="bigquery",
        sync_mode="full_refresh",
    )


@contextmanager
def build_destination(variant: str, sync_metadata: SyncMetadata, get_table_side_effect, **config_overrides):
    """Construct a destination with all google clients patched, then swap in a controllable bq_client."""
    mock_args = dict(
        sync_metadata=sync_metadata,
        backend=MagicMock(),
        source_callback=MagicMock(),
        monitor=MagicMock(),
    )

    if variant == "bigquery":
        module = "bizon.connectors.destinations.bigquery.src.destination"
        config = BigQueryConfigDetails(
            project_id=PROJECT, dataset_id=DATASET, gcs_buffer_bucket="bizon-buffer", **config_overrides
        )
        with patch(f"{module}.bigquery.Client"), patch(f"{module}.storage.Client"):
            dest = BigQueryDestination(config=config, **mock_args)
    elif variant == "streaming":
        module = "bizon.connectors.destinations.bigquery_streaming.src.destination"
        config = BigQueryStreamingConfigDetails(project_id=PROJECT, dataset_id=DATASET, **config_overrides)
        with patch(f"{module}.bigquery.Client"), patch(f"{module}.bigquery_storage_v1.BigQueryWriteClient"):
            dest = BigQueryStreamingDestination(config=config, **mock_args)
    elif variant == "streaming_v2":
        module = "bizon.connectors.destinations.bigquery_streaming_v2.src.destination"
        config = BigQueryStreamingV2ConfigDetails(project_id=PROJECT, dataset_id=DATASET, **config_overrides)
        with patch(f"{module}.bigquery.Client"):
            dest = BigQueryStreamingV2Destination(config=config, **mock_args)
    else:
        raise ValueError(variant)

    dest.bq_client = make_bq_client(get_table_side_effect)
    yield dest


ALL_VARIANTS = ["bigquery", "streaming", "streaming_v2"]


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_table_id_reuses_legacy(variant, sync_metadata):
    with build_destination(variant, sync_metadata, get_table_side_effect=lambda t: MagicMock()) as dest:
        assert dest.table_id == LEGACY_ID


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_table_id_prefixes_new(variant, sync_metadata):
    with build_destination(variant, sync_metadata, get_table_side_effect=NotFound("missing")) as dest:
        assert dest.table_id == PREFIXED_ID


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_table_id_resolution_is_cached(variant, sync_metadata):
    with build_destination(variant, sync_metadata, get_table_side_effect=NotFound("missing")) as dest:
        _ = dest.table_id
        _ = dest.table_id
        _ = dest.table_id
        assert dest.bq_client.get_table.call_count == 1


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_table_id_empty_prefix_keeps_legacy(variant, sync_metadata):
    with build_destination(variant, sync_metadata, get_table_side_effect=NotFound("missing"), table_prefix="") as dest:
        assert dest.table_id == LEGACY_ID
        dest.bq_client.get_table.assert_not_called()


def test_table_id_explicit_destination_id_bigquery(sync_metadata):
    """bigquery: destination_id is a bare name, wrapped with project.dataset, never prefixed or looked up."""
    with build_destination(
        "bigquery", sync_metadata, get_table_side_effect=NotFound("missing"), destination_id="custom_table"
    ) as dest:
        assert dest.table_id == f"{PROJECT}.{DATASET}.custom_table"
        dest.bq_client.get_table.assert_not_called()


@pytest.mark.parametrize("variant", ["streaming", "streaming_v2"])
def test_table_id_explicit_destination_id_streaming(variant, sync_metadata):
    """streaming variants: destination_id is a full path, used as-is, never prefixed or looked up."""
    full_path = "other_project.other_dataset.custom_table"
    with build_destination(
        variant, sync_metadata, get_table_side_effect=NotFound("missing"), destination_id=full_path
    ) as dest:
        assert dest.table_id == full_path
        dest.bq_client.get_table.assert_not_called()


def test_temp_table_inherits_resolved_name(sync_metadata):
    """Temp/staging table names are derived from the resolved (prefixed) table_id."""
    with build_destination("bigquery", sync_metadata, get_table_side_effect=NotFound("missing")) as dest:
        assert dest.temp_table_id == f"{PREFIXED_ID}_temp"


def test_default_config_prefix_is_bizon():
    config = BigQueryConfig(
        name=DestinationTypes.BIGQUERY,
        config=BigQueryConfigDetails(project_id=PROJECT, dataset_id=DATASET, gcs_buffer_bucket="bizon-buffer"),
    )
    assert config.config.table_prefix == "_bizon_"
