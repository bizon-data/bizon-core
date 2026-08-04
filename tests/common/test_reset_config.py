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
}


def build_config(destination: str, reset: bool = True, sync_mode: str = "incremental") -> BizonConfig:
    raw = CONFIG_TEMPLATE.format(
        reset=str(reset).lower(),
        sync_mode=sync_mode,
        destination=destination,
        destination_config=DESTINATION_CONFIGS[destination],
    )
    return BizonConfig.model_validate(obj=yaml.safe_load(raw))


@pytest.mark.parametrize("destination", ["bigquery", "logger"])
def test_reset_is_accepted_by_supported_destinations(destination):
    assert build_config(destination).source.reset is True


@pytest.mark.parametrize("destination", ["file", "bigquery_streaming_v2"])
def test_reset_is_rejected_by_destinations_that_would_append(destination):
    """These still branch on sync_mode, so a reset there would duplicate the data instead of replacing it."""
    with pytest.raises(ValidationError, match="source.reset is not supported"):
        build_config(destination)


@pytest.mark.parametrize("destination", ["file", "bigquery_streaming_v2"])
def test_unsupported_destinations_are_unaffected_without_reset(destination):
    assert build_config(destination, reset=False).source.reset is False


@pytest.mark.parametrize("destination", ["file", "bigquery_streaming_v2"])
@pytest.mark.parametrize("sync_mode", ["full_refresh", "stream"])
def test_non_incremental_runs_are_not_rejected(destination, sync_mode):
    """The runner ignores reset outside incremental, so validation must not reject it either."""
    assert build_config(destination, sync_mode=sync_mode).source.reset is True


class TestDestinationSyncMode:
    """The sync mode destinations apply, which a reset diverts to full refresh."""

    def _sync_metadata(self, sync_mode: SourceSyncModes, reset: bool) -> SyncMetadata:
        return SyncMetadata(
            name="test",
            job_id="job_1",
            source_name="dummy",
            stream_name="creatures",
            destination_name="bigquery",
            destination_alias="bigquery",
            sync_mode=sync_mode,
            reset=reset,
        )

    def test_reset_incremental_is_applied_as_full_refresh(self):
        sync_metadata = self._sync_metadata(SourceSyncModes.INCREMENTAL, reset=True)

        assert sync_metadata.destination_sync_mode == SourceSyncModes.FULL_REFRESH
        # The job itself stays incremental so it becomes the next run's watermark.
        assert sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL

    @pytest.mark.parametrize("sync_mode", list(SourceSyncModes))
    def test_without_reset_the_sync_mode_is_passed_through(self, sync_mode):
        assert self._sync_metadata(sync_mode, reset=False).destination_sync_mode == sync_mode

    @pytest.mark.parametrize("sync_mode", [SourceSyncModes.FULL_REFRESH, SourceSyncModes.STREAM])
    def test_reset_does_not_divert_other_sync_modes(self, sync_mode):
        assert self._sync_metadata(sync_mode, reset=True).destination_sync_mode == sync_mode
