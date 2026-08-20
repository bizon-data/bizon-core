"""Partitioning tests for the BigQuery Storage Write API destination (CI-safe, no live BigQuery).

`finalize()` publishes with `CREATE OR REPLACE TABLE ... AS SELECT *`, which produces an
unpartitioned, unclustered table. BigQuery then refuses to change that spec ever again, so the
carefully partitioned staging table's layout never reached the published table.

The clauses can only be emitted when the destination table is absent or already matches -- BigQuery
rejects `CREATE OR REPLACE TABLE ... PARTITION BY` over a table with a different spec:

    Cannot replace a table with a different partitioning spec. Instead, DROP the table,
    and then recreate it.

so an existing legacy table falls back to the historical plain CTAS unless `enforce_partitioning`
lets the destination drop it.
"""

from unittest.mock import MagicMock

import pytest

UNNEST_RECORD_SCHEMAS = [
    {
        "destination_id": "my_project.bizon_test.cookie_test",
        "record_schema": [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "event_on", "type": "DATE", "mode": "NULLABLE"},
            {"name": "event_dt", "type": "DATETIME", "mode": "NULLABLE"},
        ],
        "clustering_keys": ["id"],
    }
]


def published_query(destination) -> str:
    assert destination.bq_client.query.call_count == 1
    return destination.bq_client.query.call_args[0][0]


def full_id(table) -> str:
    """`Table.full_table_id` is only populated by the server, so rebuild it from the reference."""
    return f"{table.project}.{table.dataset_id}.{table.table_id}"


# ---------------------------------------------------------------------------
# Full refresh: the DDL must carry the layout
# ---------------------------------------------------------------------------


def test_full_refresh_ddl_carries_partitioning(build_bq_destination, bq_ids, make_bq_table, partitioned):
    """New table (absent): emit the clauses so it is born partitioned."""
    tables = {bq_ids["temp"]: make_bq_table(time_partitioning=partitioned())}
    with build_bq_destination("streaming_v2", tables=tables) as destination:
        destination.finalize()
        query = published_query(destination)

    assert "CREATE OR REPLACE TABLE" in query
    assert "PARTITION BY TIMESTAMP_TRUNC(`_bizon_loaded_at`, DAY)" in query


def test_full_refresh_ddl_carries_clustering(build_bq_destination, bq_ids, make_bq_table, partitioned):
    with build_bq_destination(
        "streaming_v2",
        unnest=True,
        destination_id="my_project.bizon_test.cookie_test",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
        tables={},
    ) as destination:
        destination.finalize()
        query = published_query(destination)

    assert "PARTITION BY TIMESTAMP_TRUNC(`event_at`, DAY)" in query
    assert "CLUSTER BY `id`" in query


def test_full_refresh_ddl_custom_field_and_window(build_bq_destination):
    with build_bq_destination(
        "streaming_v2", time_partitioning={"type": "HOUR", "field": "_bizon_extracted_at"}, tables={}
    ) as destination:
        destination.finalize()
        assert "PARTITION BY TIMESTAMP_TRUNC(`_bizon_extracted_at`, HOUR)" in published_query(destination)


def test_full_refresh_ingestion_time_partitioning(build_bq_destination):
    with build_bq_destination("streaming_v2", time_partitioning={"field": None}, tables={}) as destination:
        destination.finalize()
        assert "PARTITION BY _PARTITIONDATE" in published_query(destination)


def test_full_refresh_keeps_clauses_when_spec_already_matches(build_bq_destination, bq_ids, make_bq_table, partitioned):
    """A matching spec is accepted by BigQuery, so the clauses stay."""
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=partitioned())}
    with build_bq_destination("streaming_v2", tables=tables) as destination:
        destination.finalize()
        assert "PARTITION BY TIMESTAMP_TRUNC(`_bizon_loaded_at`, DAY)" in published_query(destination)


# ---------------------------------------------------------------------------
# Full refresh against a legacy table: do not break the run
# ---------------------------------------------------------------------------


def test_legacy_table_falls_back_to_plain_ctas_and_warns(build_bq_destination, bq_ids, make_bq_table, loguru_warnings):
    """Emitting PARTITION BY here would fail the run outright; existing pipelines must keep working."""
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=None)}
    with build_bq_destination("streaming_v2", tables=tables) as destination:
        destination.finalize()
        query = published_query(destination)

    assert "PARTITION BY" not in query
    assert "CREATE OR REPLACE TABLE" in query

    warnings = "\n".join(str(message) for message in loguru_warnings)
    assert "Partitioning mismatch" in warnings
    assert "enforce_partitioning: true" in warnings


def test_legacy_table_is_dropped_when_enforcing(build_bq_destination, bq_ids, make_bq_table):
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=None)}
    with build_bq_destination("streaming_v2", tables=tables, enforce_partitioning=True) as destination:
        destination.finalize()
        query = published_query(destination)

        deleted = [c.args[0] for c in destination.bq_client.delete_table.call_args_list]
        assert bq_ids["table"] in deleted

    assert "PARTITION BY TIMESTAMP_TRUNC(`_bizon_loaded_at`, DAY)" in query


def test_no_drop_when_enforcing_and_spec_already_matches(build_bq_destination, bq_ids, make_bq_table, partitioned):
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=partitioned())}
    with build_bq_destination("streaming_v2", tables=tables, enforce_partitioning=True) as destination:
        destination.finalize()
        deleted = [c.args[0] for c in destination.bq_client.delete_table.call_args_list]
        assert deleted == [bq_ids["temp"]]


# ---------------------------------------------------------------------------
# Incremental: the first run used to 404
# ---------------------------------------------------------------------------


def test_incremental_creates_main_table_with_partitioning(build_bq_destination, bq_ids):
    """`INSERT INTO` 404s against a missing table, so it has to be created first -- partitioned."""
    with build_bq_destination("streaming_v2", sync_mode="incremental", tables={}) as destination:
        destination.finalize()

        created = [c.args[0] for c in destination.bq_client.create_table.call_args_list]
        main_table = [table for table in created if full_id(table) == bq_ids["table"]]
        assert main_table, f"main table not created; created: {[full_id(t) for t in created]}"
        table = main_table[0]
        assert table.time_partitioning.field == "_bizon_loaded_at"
        assert table.time_partitioning.type_ == "DAY"

        assert "INSERT INTO" in published_query(destination)


def test_incremental_creates_main_table_with_clustering(build_bq_destination):
    with build_bq_destination(
        "streaming_v2",
        sync_mode="incremental",
        unnest=True,
        destination_id="my_project.bizon_test.cookie_test",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"field": "event_at"},
        tables={},
    ) as destination:
        destination.finalize()
        created = [c.args[0] for c in destination.bq_client.create_table.call_args_list]
        assert any(table.clustering_fields == ["id"] for table in created)


def test_incremental_warns_on_legacy_table(build_bq_destination, bq_ids, make_bq_table, loguru_warnings):
    tables = {bq_ids["table"]: make_bq_table(time_partitioning=None)}
    with build_bq_destination("streaming_v2", sync_mode="incremental", tables=tables) as destination:
        destination.bq_client.create_table.side_effect = __import__(
            "google.api_core.exceptions", fromlist=["Conflict"]
        ).Conflict("exists")
        destination.finalize()

    assert "Partitioning mismatch" in "\n".join(str(message) for message in loguru_warnings)


def test_incremental_does_not_purge_main_table_from_ensured_cache(build_bq_destination, bq_ids):
    """The main table persists across the INSERT, unlike the temp table which is deleted."""
    with build_bq_destination("streaming_v2", sync_mode="incremental", tables={}) as destination:
        destination.finalize()
        cached = {key[0] for key in destination._ensured_tables}
        assert bq_ids["table"] in cached
        assert bq_ids["incremental"] not in cached


# ---------------------------------------------------------------------------
# Partition clause per column type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field, window, expected",
    [
        ("event_at", "DAY", "PARTITION BY TIMESTAMP_TRUNC(`event_at`, DAY)"),
        ("event_at", "HOUR", "PARTITION BY TIMESTAMP_TRUNC(`event_at`, HOUR)"),
        ("event_at", "MONTH", "PARTITION BY TIMESTAMP_TRUNC(`event_at`, MONTH)"),
        ("event_at", "YEAR", "PARTITION BY TIMESTAMP_TRUNC(`event_at`, YEAR)"),
        ("event_dt", "DAY", "PARTITION BY DATETIME_TRUNC(`event_dt`, DAY)"),
        ("event_dt", "HOUR", "PARTITION BY DATETIME_TRUNC(`event_dt`, HOUR)"),
        ("event_on", "DAY", "PARTITION BY `event_on`"),
        ("event_on", "MONTH", "PARTITION BY DATE_TRUNC(`event_on`, MONTH)"),
        ("event_on", "YEAR", "PARTITION BY DATE_TRUNC(`event_on`, YEAR)"),
    ],
)
def test_partition_clause_by_column_type(build_bq_destination, field, window, expected):
    with build_bq_destination(
        "streaming_v2",
        unnest=True,
        destination_id="my_project.bizon_test.cookie_test",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"type": window, "field": field},
        tables={},
    ) as destination:
        assert destination._partition_clause() == expected


def test_hour_partitioning_on_a_date_column_is_rejected(build_bq_destination):
    with build_bq_destination(
        "streaming_v2",
        unnest=True,
        destination_id="my_project.bizon_test.cookie_test",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"type": "HOUR", "field": "event_on"},
        tables={},
    ) as destination:
        with pytest.raises(ValueError, match="HOUR partitioning is not supported on DATE column"):
            destination._partition_clause()


def test_non_temporal_partition_column_is_rejected(build_bq_destination):
    with build_bq_destination(
        "streaming_v2",
        unnest=True,
        destination_id="my_project.bizon_test.cookie_test",
        record_schemas=UNNEST_RECORD_SCHEMAS,
        time_partitioning={"field": "id"},
        tables={},
    ) as destination:
        with pytest.raises(ValueError, match="not TIMESTAMP/DATE/DATETIME"):
            destination._partition_clause()


def test_stream_mode_finalize_is_still_a_noop(build_bq_destination):
    with build_bq_destination("streaming_v2", sync_mode="stream", tables={}) as destination:
        assert destination.finalize() is True
        destination.bq_client.query.assert_not_called()


def test_published_query_has_no_double_spaces(build_bq_destination):
    """An empty clause must not leave `TABLE  AS SELECT` in the DDL."""
    with build_bq_destination("streaming_v2", time_partitioning=None, tables={}) as destination:
        query = MagicMock()
        destination.bq_client.query.return_value = query
        destination.finalize()
        assert "  " not in published_query(destination)
