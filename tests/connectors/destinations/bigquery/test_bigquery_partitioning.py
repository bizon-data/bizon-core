"""Partitioning tests for the batch BigQuery destination (CI-safe, no live BigQuery).

Covers three things that used to be silently wrong:
  - `time_partitioning` was a bare window enum, so the partition column could not be configured;
  - an unknown partition column only failed mid-run, inside the load job;
  - a legacy unpartitioned destination table stayed unpartitioned forever, unreported, because
    BigQuery cannot change an existing table's partitioning spec.
"""

from unittest.mock import MagicMock, call, patch

import pytest
import yaml
from google.api_core.exceptions import BadRequest, Forbidden, NotFound, ServiceUnavailable
from pydantic import ValidationError

from bizon.common.models import BizonConfig
from bizon.connectors.destinations.bigquery.src.config import (
    BIZON_METADATA_COLUMN_TYPES,
    BIZON_METADATA_COLUMNS,
    PARTITIONABLE_COLUMN_TYPES,
    BigQueryConfigDetails,
    TimePartitioningWindow,
)

BASE_CONFIG = dict(project_id="p", dataset_id="d", gcs_buffer_bucket="b")

UNNEST_RECORD_SCHEMAS = [
    {
        "destination_id": "my_table",
        "record_schema": [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
    }
]


# ---------------------------------------------------------------------------
# Config model
# ---------------------------------------------------------------------------


def test_bare_window_string_still_validates():
    """Backwards compatibility: `time_partitioning: DAY` was the entire config before 0.5.3."""
    config = BigQueryConfigDetails(**BASE_CONFIG, time_partitioning="DAY")
    assert config.time_partitioning.type is TimePartitioningWindow.DAY
    assert config.time_partitioning.field == "_bizon_loaded_at"


@pytest.mark.parametrize("window", ["DAY", "HOUR", "MONTH", "YEAR"])
def test_every_bare_window_still_validates(window):
    assert BigQueryConfigDetails(**BASE_CONFIG, time_partitioning=window).time_partitioning.type.value == window


def test_bare_window_survives_a_full_config_round_trip():
    """The real guarantee: an existing production YAML keeps parsing through BizonConfig."""
    config = yaml.safe_load(
        """
        name: legacy pipeline
        source:
          name: dummy
          stream: creatures
          sync_mode: full_refresh
          authentication:
            type: api_key
            params:
              token: dummy_key
        destination:
          name: bigquery
          config:
            project_id: my_project
            dataset_id: bizon_test
            gcs_buffer_bucket: bizon-buffer
            time_partitioning: DAY
        """
    )
    bizon_config = BizonConfig.model_validate(config)
    assert bizon_config.destination.config.time_partitioning.type is TimePartitioningWindow.DAY
    assert bizon_config.destination.config.time_partitioning.field == "_bizon_loaded_at"


def test_mapping_form_validates():
    config = BigQueryConfigDetails(**BASE_CONFIG, time_partitioning={"type": "HOUR", "field": "_bizon_extracted_at"})
    assert config.time_partitioning.type is TimePartitioningWindow.HOUR
    assert config.time_partitioning.field == "_bizon_extracted_at"


def test_default_is_day_on_bizon_loaded_at():
    config = BigQueryConfigDetails(**BASE_CONFIG)
    assert config.time_partitioning.type is TimePartitioningWindow.DAY
    assert config.time_partitioning.field == "_bizon_loaded_at"
    assert config.enforce_partitioning is False


def test_null_field_selects_ingestion_time_partitioning():
    config = BigQueryConfigDetails(**BASE_CONFIG, time_partitioning={"type": "DAY", "field": None})
    assert config.time_partitioning.field is None


def test_mistyped_key_is_rejected():
    """extra=forbid: a silently-ignored `filed:` is exactly the failure mode this release is about."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BigQueryConfigDetails(**BASE_CONFIG, time_partitioning={"type": "DAY", "filed": "ts"})


def test_unknown_partition_field_rejected_without_unnest():
    with pytest.raises(ValidationError, match="does not declare it"):
        BigQueryConfigDetails(**BASE_CONFIG, time_partitioning={"field": "created_at"})


def test_unknown_partition_field_rejected_for_unnest_record_schema():
    """In unnest mode the table holds only record_schema columns, so an outside column is absent.

    The *implicit* `_bizon_loaded_at` no longer raises -- it resolves to ingestion-time partitioning,
    since the user never asked for it. See test_partition_field_validation.py. Writing a bad field
    yourself still raises, on this destination, because every batch load job applies it.
    """
    with pytest.raises(ValidationError, match="does not declare it"):
        BigQueryConfigDetails(
            **BASE_CONFIG, unnest=True, record_schemas=UNNEST_RECORD_SCHEMAS, time_partitioning={"field": "created_at"}
        )


def test_unnest_partition_field_from_record_schema_is_accepted():
    config = BigQueryConfigDetails(
        **BASE_CONFIG,
        unnest=True,
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
    )
    assert config.time_partitioning.field == "event_at"


def test_unnest_accepts_ingestion_time_partitioning():
    config = BigQueryConfigDetails(
        **BASE_CONFIG, unnest=True, record_schemas=UNNEST_RECORD_SCHEMAS, time_partitioning={"field": None}
    )
    assert config.time_partitioning.field is None


@pytest.mark.parametrize("variant", ["bigquery", "streaming", "streaming_v2"])
def test_metadata_columns_match_schema(build_bq_destination, variant):
    """BIZON_METADATA_COLUMN_TYPES is duplicated in config.py to keep google.cloud out of that module.

    Names, and the one property the copy exists to answer: which of those columns can be partitioned
    on. The check rejects `_bizon_id` for being STRING, which is only correct while this agrees with
    what the destinations actually declare.

    Exact types are deliberately not asserted -- `_source_data` is JSON on the streaming destinations
    and STRING on the batch one, which round-trips through Parquet. Both are unpartitionable, so the
    divergence cannot affect the check.
    """
    with build_bq_destination(variant) as destination:
        schema = destination.get_bigquery_schema()

    assert {field.name for field in schema} == BIZON_METADATA_COLUMNS
    assert {field.name for field in schema if field.field_type in PARTITIONABLE_COLUMN_TYPES} == {
        name for name, type_ in BIZON_METADATA_COLUMN_TYPES.items() if type_ in PARTITIONABLE_COLUMN_TYPES
    }


# ---------------------------------------------------------------------------
# Load job config
# ---------------------------------------------------------------------------


def test_load_job_defaults_to_bizon_loaded_at(build_bq_destination):
    with build_bq_destination("bigquery") as destination:
        job_config = destination._build_load_job_config()
        assert job_config.time_partitioning.field == "_bizon_loaded_at"
        assert job_config.time_partitioning.type_ == "DAY"


def test_load_job_uses_configured_field_and_window(build_bq_destination):
    with build_bq_destination(
        "bigquery",
        unnest=True,
        destination_id="my_table",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"type": "HOUR", "field": "event_at"},
    ) as destination:
        job_config = destination._build_load_job_config()
        assert job_config.time_partitioning.field == "event_at"
        assert job_config.time_partitioning.type_ == "HOUR"


def test_load_job_ingestion_time_partitioning(build_bq_destination):
    with build_bq_destination("bigquery", time_partitioning={"field": None}) as destination:
        job_config = destination._build_load_job_config()
        assert job_config.time_partitioning.field is None
        assert job_config.time_partitioning.type_ == "DAY"


def test_load_job_raises_when_field_missing_from_resolved_schema(build_bq_destination):
    """The stream runner assigns record_schemas after validation, so the static check can be bypassed."""
    with build_bq_destination(
        "bigquery",
        unnest=True,
        destination_id="my_table",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
    ) as destination:
        # Simulate post-validation injection: a schema that does not contain the partition field.
        destination.config.time_partitioning.field = "_bizon_loaded_at"
        with pytest.raises(ValueError, match="the schema written to .* does not declare it"):
            destination._build_load_job_config()


def test_check_connection_fails_fast_on_bad_partition_field(build_bq_destination):
    with build_bq_destination("bigquery") as destination:
        destination.config.time_partitioning.field = "nope"
        destination._ensure_dataset = MagicMock()
        with pytest.raises(ValueError, match="'nope'"):
            destination.check_connection()


# ---------------------------------------------------------------------------
# Legacy-table detection and enforce_partitioning
# ---------------------------------------------------------------------------


def test_warns_and_does_not_drop_when_flag_is_off(
    build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned
):
    """The 0.4.0-era symptom: a legacy unpartitioned table silently stays unpartitioned."""
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=None),
    }
    with build_bq_destination("bigquery", tables=tables) as destination:
        destination.finalize()

    warnings = "\n".join(str(message) for message in loguru_warnings)
    assert "Partitioning mismatch" in warnings
    assert "not partitioned" in warnings
    assert "partitioned by `_bizon_loaded_at` (DAY)" in warnings
    assert "enforce_partitioning: true" in warnings


def test_no_warning_when_specs_match(build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned):
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=partitioned()),
    }
    with build_bq_destination("bigquery", tables=tables) as destination:
        destination.finalize()

    assert not [m for m in loguru_warnings if "Partitioning mismatch" in str(m)]


def test_drops_destination_before_copy_when_enforcing_on_full_refresh(
    build_bq_destination, bq_ids, make_bq_table, partitioned
):
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=None),
    }
    with build_bq_destination("bigquery", tables=tables, enforce_partitioning=True) as destination:
        recorder = MagicMock()
        recorder.attach_mock(destination.bq_client.delete_table, "delete_table")
        recorder.attach_mock(destination.bq_client.copy_table, "copy_table")

        destination.finalize()

        ordered = [c for c in recorder.mock_calls if c[0] in ("delete_table", "copy_table")]
        assert ordered[0] == call.delete_table(bq_ids["table"], not_found_ok=True)
        assert ordered[1][0] == "copy_table"


def test_never_drops_on_incremental_even_when_enforcing(
    build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned
):
    """Incremental stages only its delta -- rebuilding here would discard history."""
    tables = {
        bq_ids["incremental"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=None),
    }
    with build_bq_destination(
        "bigquery", sync_mode="incremental", tables=tables, enforce_partitioning=True
    ) as destination:
        destination.finalize()
        # Only the temp table is deleted, never the destination table.
        deleted = [c.args[0] for c in destination.bq_client.delete_table.call_args_list]
        assert bq_ids["table"] not in deleted
        assert bq_ids["incremental"] in deleted

    warnings = "\n".join(str(message) for message in loguru_warnings)
    assert "bizon stream reset" in warnings


def test_no_drop_when_temp_table_missing(build_bq_destination, bq_ids, loguru_warnings, make_bq_table):
    """A run that produced no records never creates the temp table; it must not drop the destination."""
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=None)}
    with build_bq_destination("bigquery", tables=tables, enforce_partitioning=True) as destination:
        destination.bq_client.copy_table.side_effect = NotFound("no temp table")
        with pytest.raises(NotFound):
            destination.finalize()

        deleted = [c.args[0] for c in destination.bq_client.delete_table.call_args_list]
        assert bq_ids["table"] not in deleted

    assert not [m for m in loguru_warnings if "Partitioning mismatch" in str(m)]


def test_no_drop_when_destination_missing(build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned):
    """First run: the copy job creates the table with the temp table's partitioning."""
    tables = {bq_ids["temp"]: make_bq_table(time_partitioning=partitioned())}
    with build_bq_destination("bigquery", tables=tables, enforce_partitioning=True) as destination:
        destination._check_destination_partitioning()
        destination.bq_client.delete_table.assert_not_called()

    assert not [m for m in loguru_warnings if "Partitioning mismatch" in str(m)]


def test_clustering_difference_is_reported(build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned):
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned(), clustering_fields=["id"]),
        bq_ids["table"]: make_bq_table(time_partitioning=partitioned(), clustering_fields=None),
    }
    with build_bq_destination("bigquery", tables=tables) as destination:
        destination.finalize()

    warnings = "\n".join(str(message) for message in loguru_warnings)
    assert "clustered by (id)" in warnings


def test_lookup_failure_does_not_fail_the_run(build_bq_destination, bq_ids, loguru_warnings):
    """The check is diagnostic: a transient 5xx or a missing tables.get must not kill a publish."""
    with build_bq_destination("bigquery", tables={}) as destination:
        destination.bq_client.get_table.side_effect = ServiceUnavailable("backend error")

        # finalize() still publishes; only the post-copy existence check sees the error.
        destination._check_destination_partitioning()
        destination.bq_client.delete_table.assert_not_called()

    assert any("Could not check the partitioning" in str(m) for m in loguru_warnings)


def test_lookup_failure_does_not_drop_when_enforcing(build_bq_destination, bq_ids):
    """Above all, an unreadable spec must never be treated as a mismatch worth dropping over."""
    with build_bq_destination("bigquery", tables={}, enforce_partitioning=True) as destination:
        destination.bq_client.get_table.side_effect = Forbidden("no tables.get")
        destination._check_destination_partitioning()
        destination.bq_client.delete_table.assert_not_called()


def test_copy_job_partitioning_error_is_rewrapped(build_bq_destination, bq_ids, make_bq_table, partitioned):
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=partitioned()),
    }
    with build_bq_destination("bigquery", tables=tables) as destination:
        copy_job = MagicMock()
        copy_job.result.side_effect = BadRequest("Incompatible table partitioning specification")
        destination.bq_client.copy_table.return_value = copy_job

        with pytest.raises(RuntimeError, match="enforce_partitioning"):
            destination.finalize()


def test_copy_job_unrelated_error_is_not_rewrapped(build_bq_destination, bq_ids, make_bq_table, partitioned):
    tables = {
        bq_ids["temp"]: make_bq_table(time_partitioning=partitioned()),
        bq_ids["table"]: make_bq_table(time_partitioning=partitioned()),
    }
    with build_bq_destination("bigquery", tables=tables) as destination:
        copy_job = MagicMock()
        copy_job.result.side_effect = BadRequest("Some other problem")
        destination.bq_client.copy_table.return_value = copy_job

        with pytest.raises(BadRequest):
            destination.finalize()


# ---------------------------------------------------------------------------
# Cross-variant config parity -- the test that would have caught the hardcoded field
# ---------------------------------------------------------------------------

ALL_VARIANTS = ["bigquery", "streaming", "streaming_v2"]


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_all_variants_accept_a_bare_window(build_bq_destination, variant):
    with build_bq_destination(variant, time_partitioning="HOUR") as destination:
        assert destination.config.time_partitioning.type is TimePartitioningWindow.HOUR
        assert destination.config.time_partitioning.field == "_bizon_loaded_at"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_all_variants_accept_a_configured_field(build_bq_destination, variant):
    with build_bq_destination(
        variant, time_partitioning={"type": "MONTH", "field": "_bizon_extracted_at"}
    ) as destination:
        assert destination.config.time_partitioning.type is TimePartitioningWindow.MONTH
        assert destination.config.time_partitioning.field == "_bizon_extracted_at"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_all_variants_share_one_time_partitioning_model(variant):
    """The three configs used to declare byte-identical duplicates that drifted apart."""
    from bizon.connectors.destinations.bigquery.src.config import TimePartitioning as Canonical
    from bizon.connectors.destinations.bigquery_streaming.src.config import TimePartitioning as V1
    from bizon.connectors.destinations.bigquery_streaming_v2.src.config import TimePartitioning as V2

    assert Canonical is V1 is V2


def test_config_module_does_not_import_google_cloud():
    """Regression guard for 0.5.1: bizon.common.models imports this on every CLI invocation."""
    import ast
    import pathlib

    import bizon.connectors.destinations.bigquery.src.config as config_module

    source = pathlib.Path(config_module.__file__).read_text()
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    assert not [name for name in imported if name.startswith("google")]


def test_partitioning_helpers_are_importable_without_a_client():
    with patch("bizon.connectors.destinations.bigquery.src.partitioning.spec_from_table", create=True):
        pass  # Import-time only: partitioning.py must not need a live client to be imported.
