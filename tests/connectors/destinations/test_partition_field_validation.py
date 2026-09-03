"""One partition-field check across the three BigQuery destinations (CI-safe, no live BigQuery).

0.5.3 added a config-time check that `time_partitioning.field` exists in the schema the destination
writes -- and wired it to the batch destination only. These tests pin both halves of extending it:

  - the check now runs on all three, and covers the column's *type*, not just its presence;
  - it is only fatal where the spec actually reaches a table. On the two streaming destinations
    `create_table` is the only thing that ever applies partitioning, and it raises `Conflict` once
    the table exists, so BigQuery keeps the table's own spec and the run succeeds. Pipelines have
    been running that way for as long as their tables have existed; a hard failure at config load
    would stop every one of them on upgrade. Absent table -> raise (BigQuery would too). Present
    table -> warn and carry on.

The other half of not breaking anyone is the implicit default: `field` defaults to
`_bizon_loaded_at`, which `unnest: true` can never produce, so a config that never mentioned
partitioning would start failing purely by upgrading. An unwritten field falls back to ingestion-time
partitioning instead.
"""

from unittest.mock import MagicMock

import pytest
import yaml
from google.api_core.exceptions import Conflict
from pydantic import ValidationError

from bizon.common.models import BizonConfig
from bizon.connectors.destinations.bigquery.src.config import BigQueryConfigDetails
from bizon.connectors.destinations.bigquery_streaming.src.config import BigQueryStreamingConfigDetails
from bizon.connectors.destinations.bigquery_streaming_v2.src.config import BigQueryStreamingV2ConfigDetails

KAFKA_STREAMS_EXAMPLE = "bizon/connectors/sources/kafka/config/kafka_streams.example.yml"

RECORD_SCHEMAS = [
    {
        "destination_id": "my_project.bizon_test.cookie_test",
        "record_schema": [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "event_on", "type": "DATE", "mode": "NULLABLE"},
        ],
    }
]

# Each destination has its own required fields; everything else about them is irrelevant here.
CONFIG_CLASSES = {
    "bigquery": (BigQueryConfigDetails, dict(project_id="p", dataset_id="d", gcs_buffer_bucket="b")),
    "streaming": (BigQueryStreamingConfigDetails, dict(project_id="p", dataset_id="d")),
    "streaming_v2": (BigQueryStreamingV2ConfigDetails, dict(project_id="p", dataset_id="d")),
}

ALL_VARIANTS = list(CONFIG_CLASSES)
STREAMING_VARIANTS = ["streaming", "streaming_v2"]


def build_config(variant: str, **overrides):
    config_class, base = CONFIG_CLASSES[variant]
    return config_class(**base, **overrides)


def assert_rejected(variant: str, match: str, warnings: list, **overrides):
    """The batch destination raises; the streaming ones warn and keep the config usable.

    Asserting both in one helper keeps the split explicit at every call site: the severity is the
    point of these tests, not an incidental detail.
    """
    if variant == "bigquery":
        with pytest.raises(ValidationError, match=match):
            build_config(variant, **overrides)
        return None

    config = build_config(variant, **overrides)
    assert any(match in str(message) for message in warnings), f"expected a warning matching {match!r}"
    return config


# ---------------------------------------------------------------------------
# The implicit default can never break a config that stayed silent
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_unnest_without_time_partitioning_falls_back_to_ingestion_time(variant):
    """The trap: `_bizon_loaded_at` is a default the user never wrote and unnest cannot produce."""
    config = build_config(variant, unnest=True, record_schemas=RECORD_SCHEMAS)

    assert config.time_partitioning.field is None
    assert config.time_partitioning.type.value == "DAY"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_unnest_with_only_a_window_falls_back_too(variant):
    """`type` written, `field` not -- the field is still nobody's choice."""
    config = build_config(variant, unnest=True, record_schemas=RECORD_SCHEMAS, time_partitioning={"type": "HOUR"})

    assert config.time_partitioning.field is None
    assert config.time_partitioning.type.value == "HOUR"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_non_unnest_default_is_untouched(variant):
    config = build_config(variant)

    assert config.time_partitioning.field == "_bizon_loaded_at"
    assert config.time_partitioning.type.value == "DAY"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_bare_window_is_untouched(variant):
    """`time_partitioning: DAY` predates the mapping form and must keep resolving the same way."""
    config = build_config(variant, time_partitioning="DAY")

    assert config.time_partitioning.field == "_bizon_loaded_at"
    assert config.time_partitioning.type.value == "DAY"


@pytest.mark.parametrize("variant", STREAMING_VARIANTS)
def test_null_time_partitioning_disables_partitioning(variant):
    assert build_config(variant, time_partitioning=None).time_partitioning is None


@pytest.mark.parametrize("variant", STREAMING_VARIANTS)
def test_configs_do_not_share_a_time_partitioning_instance(variant):
    """`default=TimePartitioning(...)` is not copied by pydantic; the validator rewrites it."""
    first, second = build_config(variant), build_config(variant)

    assert first.time_partitioning is not second.time_partitioning


# ---------------------------------------------------------------------------
# An explicitly written field is checked -- presence and type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_explicit_field_in_record_schema_is_accepted(variant):
    config = build_config(variant, unnest=True, record_schemas=RECORD_SCHEMAS, time_partitioning={"field": "event_at"})

    assert config.time_partitioning.field == "event_at"


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_field_absent_from_record_schema(variant, loguru_warnings):
    assert_rejected(
        variant,
        "does not declare it",
        loguru_warnings,
        unnest=True,
        record_schemas=RECORD_SCHEMAS,
        time_partitioning={"field": "created_at"},
    )


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_field_present_but_not_temporal(variant, loguru_warnings):
    """New in 0.5.4: presence was never enough -- BigQuery cannot partition on a STRING."""
    assert_rejected(
        variant,
        "declares as STRING",
        loguru_warnings,
        unnest=True,
        record_schemas=RECORD_SCHEMAS,
        time_partitioning={"field": "id"},
    )


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_hour_window_on_a_date_column(variant, loguru_warnings):
    assert_rejected(
        variant,
        "HOUR partitioning on DATE",
        loguru_warnings,
        unnest=True,
        record_schemas=RECORD_SCHEMAS,
        time_partitioning={"type": "HOUR", "field": "event_on"},
    )


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_non_unnest_metadata_column_of_the_wrong_type(variant, loguru_warnings):
    """`_bizon_id` is a column bizon writes, so 0.5.3 accepted it. It is STRING."""
    assert_rejected(variant, "declares as STRING", loguru_warnings, time_partitioning={"field": "_bizon_id"})


@pytest.mark.parametrize("variant", ALL_VARIANTS)
def test_second_record_schema_naming_the_culprit(variant, loguru_warnings):
    """Every schema is checked: one stream added without the column is how the two drift apart."""
    schemas = RECORD_SCHEMAS + [
        {
            "destination_id": "my_project.bizon_test.orders",
            "record_schema": [{"name": "id", "type": "STRING", "mode": "NULLABLE"}],
        }
    ]

    assert_rejected(
        variant,
        "my_project.bizon_test.orders",
        loguru_warnings,
        unnest=True,
        record_schemas=schemas,
        time_partitioning={"field": "event_at"},
    )


# ---------------------------------------------------------------------------
# Runtime: severity follows whether the spec reaches a table
# ---------------------------------------------------------------------------


def apply_partitioning(destination, variant):
    """Drive whichever code path applies `time_partitioning` on this destination."""
    if variant == "streaming_v2":
        destination._ensure_table(destination.table_id)
    else:
        destination.load_to_bigquery_via_legacy_streaming(MagicMock())


@pytest.mark.parametrize("variant", STREAMING_VARIANTS)
def test_runtime_raises_when_the_table_does_not_exist(build_bq_destination, variant):
    """We are about to create it with this spec, and BigQuery would reject the request."""
    with build_bq_destination(
        variant, unnest=True, record_schemas=RECORD_SCHEMAS, time_partitioning={"field": "event_at"}, tables={}
    ) as destination:
        # Post-validation injection: the stream runner assigns record_schemas after config load, so
        # the config check can be bypassed entirely and the runtime check is the only one left.
        destination.config.time_partitioning.field = "_bizon_loaded_at"

        with pytest.raises(ValueError, match="does not declare it"):
            apply_partitioning(destination, variant)


@pytest.mark.parametrize("variant", STREAMING_VARIANTS)
def test_runtime_warns_and_continues_when_the_table_exists(
    build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned, variant
):
    """The backwards-compatibility guarantee: this shape has been running fine and must keep running.

    BigQuery keeps the existing table's spec, so the configured field is inert. The run completes and
    `create_table` is not asked to apply the bad spec.
    """
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=partitioned())}

    with build_bq_destination(
        variant,
        sync_mode="stream",
        unnest=True,
        record_schemas=RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
        tables=tables,
    ) as destination:
        destination.config.time_partitioning.field = "_bizon_loaded_at"

        apply_partitioning(destination, variant)

        submitted = destination.bq_client.create_table.call_args[0][0]
        assert submitted.time_partitioning is None

    assert any("already exists" in str(message) for message in loguru_warnings)


@pytest.mark.parametrize("variant", STREAMING_VARIANTS)
def test_runtime_probes_the_table_only_when_something_is_wrong(build_bq_destination, variant):
    """A healthy config must not pay an extra get_table on every flush."""
    with build_bq_destination(
        variant,
        sync_mode="stream",
        unnest=True,
        record_schemas=RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
        tables={},
    ) as destination:
        apply_partitioning(destination, variant)

        submitted = destination.bq_client.create_table.call_args[0][0]
        assert submitted.time_partitioning.field == "event_at"
        destination.bq_client.get_table.assert_not_called()


def test_legacy_streaming_warns_when_the_live_table_has_drifted(
    build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned
):
    """v1 swallows `Conflict` silently, which is how a table stays partitioned on the wrong column."""
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=partitioned(field="ingested_at"))}

    with build_bq_destination(
        "streaming", sync_mode="stream", time_partitioning={"field": "_bizon_loaded_at"}, tables=tables
    ) as destination:
        destination.bq_client.create_table.side_effect = Conflict("exists")
        destination.load_to_bigquery_via_legacy_streaming(MagicMock())

    assert any("`ingested_at`" in str(message) for message in loguru_warnings)


def test_legacy_streaming_stays_quiet_when_the_live_table_matches(
    build_bq_destination, bq_ids, loguru_warnings, make_bq_table, partitioned
):
    with build_bq_destination(
        "streaming", sync_mode="stream", time_partitioning={"field": "_bizon_loaded_at"}, tables={}
    ) as destination:
        table = make_bq_table(time_partitioning=partitioned(field="_bizon_loaded_at"))
        destination.bq_client.create_table.side_effect = Conflict("exists")
        destination.bq_client.get_table.side_effect = None
        destination.bq_client.get_table.return_value = table

        destination.load_to_bigquery_via_legacy_streaming(MagicMock())

    assert not [message for message in loguru_warnings if "cannot change an existing" in str(message)]


# ---------------------------------------------------------------------------
# The shipped multi-stream example is the production shape
# ---------------------------------------------------------------------------


def test_shipped_kafka_streams_example_validates():
    """`bigquery_streaming_v2` + `unnest: true` + `field: __inserted_at`, one schema per stream."""
    config = BizonConfig.model_validate(yaml.safe_load(open(KAFKA_STREAMS_EXAMPLE)))

    assert config.destination.config.time_partitioning.field == "__inserted_at"
    assert len(config.destination.config.record_schemas) == 2


def test_a_stream_that_drops_the_partition_column_is_reported(loguru_warnings):
    """Adding a stream whose record_schema lacks the partition column is the drift to catch."""
    raw = yaml.safe_load(open(KAFKA_STREAMS_EXAMPLE))
    orders = next(stream for stream in raw["streams"] if stream["name"] == "orders")
    orders["destination"]["record_schema"] = [
        column for column in orders["destination"]["record_schema"] if column["name"] != "__inserted_at"
    ]

    BizonConfig.model_validate(raw)

    assert any(orders["destination"]["table_id"] in str(message) for message in loguru_warnings)
