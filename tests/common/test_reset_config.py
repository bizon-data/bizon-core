"""Tests for the `source.reset` guard on BizonConfig."""

import pytest
import yaml
from pydantic import ValidationError

from bizon.common.models import BizonConfig, SyncMetadata
from bizon.source.config import SourceSyncModes

CONFIG_TEMPLATE = """
name: test_reset_pipeline

source:
  name: dummy
  stream: creatures
  sync_mode: {sync_mode}
  cursor_field: updated_at
  reset: {reset}
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: {destination}
  config: {destination_config}
"""

DESTINATION_CONFIGS = {
    "logger": "{dummy: dummy}",
    "bigquery": "{project_id: p, dataset_id: d, gcs_buffer_bucket: b}",
    "file": "{format: json, destination_id: /tmp/out.json}",
    "bigquery_streaming_v2": "{project_id: p, dataset_id: d}",
    "bigquery_streaming": "{project_id: p, dataset_id: d}",
}


def build_config(destination: str, reset: bool = True, sync_mode: str = "incremental") -> BizonConfig:
    raw = CONFIG_TEMPLATE.format(
        reset=str(reset).lower(),
        sync_mode=sync_mode,
        destination=destination,
        destination_config=DESTINATION_CONFIGS[destination],
    )
    return BizonConfig.model_validate(obj=yaml.safe_load(raw))


@pytest.mark.parametrize("destination", ["bigquery", "logger", "file", "bigquery_streaming_v2"])
def test_reset_is_accepted_by_destinations_that_can_replace(destination):
    """A reset arrives as full_refresh, so any working full-refresh path supports it for free."""
    assert build_config(destination).source.reset is True


def test_reset_is_rejected_by_destinations_that_would_append():
    """bigquery_streaming has no finalize() and no staging table, so it appends even on full refresh."""
    with pytest.raises(ValidationError, match="source.reset is not supported"):
        build_config("bigquery_streaming")


def test_unsupported_destination_is_unaffected_without_reset():
    assert build_config("bigquery_streaming", reset=False).source.reset is False


@pytest.mark.parametrize("sync_mode", ["full_refresh", "stream"])
def test_non_incremental_runs_are_not_rejected(sync_mode):
    """The runner ignores reset outside incremental, so validation must not reject it either."""
    assert build_config("bigquery_streaming", sync_mode=sync_mode).source.reset is True


class TestSyncMetadataSyncMode:
    """A reset reaches destinations as a full refresh, which is how they replace the table."""

    def _sync_metadata(self, sync_mode: str, reset: bool) -> SyncMetadata:
        config = build_config("bigquery", reset=reset, sync_mode=sync_mode)
        return SyncMetadata.from_bizon_config(job_id="job_1", config=config)

    def test_reset_incremental_is_materialized_as_full_refresh(self):
        assert self._sync_metadata("incremental", reset=True).sync_mode == SourceSyncModes.FULL_REFRESH

    @pytest.mark.parametrize("sync_mode", ["full_refresh", "incremental", "stream"])
    def test_without_reset_the_sync_mode_is_passed_through(self, sync_mode):
        assert self._sync_metadata(sync_mode, reset=False).sync_mode == sync_mode

    @pytest.mark.parametrize("sync_mode", ["full_refresh", "stream"])
    def test_reset_does_not_divert_other_sync_modes(self, sync_mode):
        assert self._sync_metadata(sync_mode, reset=True).sync_mode == sync_mode

    @pytest.mark.parametrize("reset", [True, False])
    def test_reset_flag_is_always_carried_through(self, reset):
        """Destinations still need the raw flag, e.g. to drop a stale temp table exactly once."""
        assert self._sync_metadata("incremental", reset=reset).reset is reset
