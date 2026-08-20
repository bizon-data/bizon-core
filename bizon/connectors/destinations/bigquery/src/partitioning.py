"""Helpers for reasoning about BigQuery table partitioning, shared by the BigQuery destinations.

Deliberately *not* in `config.py`: that module is imported by `bizon.common.models` on every CLI
invocation and must stay free of `google.cloud.bigquery` (see CHANGELOG 0.5.1). Only destinations,
which already need the client, import this module.

The central fact these helpers exist to work around: **BigQuery cannot change an existing table's
partitioning spec.** Verified against the API:

- a copy job (`WRITE_TRUNCATE` or `WRITE_APPEND`) into an existing table silently keeps the
  *destination's* spec, so publishing a partitioned staging table over a legacy unpartitioned table
  leaves it unpartitioned, forever;
- a copy job into a table that does *not* exist inherits the source's partitioning and clustering;
- `CREATE OR REPLACE TABLE` fails outright in either direction when the spec differs
  (`Cannot replace a table with a different partitioning spec. Instead, DROP the table, and then
  recreate it.`).

So the only way to repartition is to drop and recreate, which is why it is gated behind
`enforce_partitioning` and restricted to full refreshes (where every row is being republished
anyway).
"""

from typing import List, Optional, Tuple

# (window, field, clustering fields). `window is None` means the table is not time-partitioned;
# `field is None` with a window means ingestion-time partitioning (_PARTITIONTIME).
PartitioningSpec = Tuple[Optional[str], Optional[str], Tuple[str, ...]]

UNPARTITIONED: PartitioningSpec = (None, None, ())


def spec_from_table(table) -> PartitioningSpec:
    """Read the actual spec off a `google.cloud.bigquery.Table`."""
    if table is None:
        return UNPARTITIONED

    partitioning = getattr(table, "time_partitioning", None)
    clustering = tuple(getattr(table, "clustering_fields", None) or ())

    if partitioning is None:
        return (None, None, clustering)

    return (getattr(partitioning, "type_", None), getattr(partitioning, "field", None), clustering)


def spec_from_config(time_partitioning, clustering_fields: Optional[List[str]] = None) -> PartitioningSpec:
    """Build the intended spec from a `TimePartitioning` config model."""
    clustering = tuple(clustering_fields or ())

    if time_partitioning is None:
        return (None, None, clustering)

    return (time_partitioning.type.value, time_partitioning.field, clustering)


def describe(spec: PartitioningSpec) -> str:
    """Render a spec for a log line, e.g. ``partitioned by `_bizon_loaded_at` (DAY), clustered by (id)``."""
    window, field, clustering = spec

    if window is None:
        described = "not partitioned"
    elif field is None:
        described = f"partitioned by ingestion time ({window})"
    else:
        described = f"partitioned by `{field}` ({window})"

    if clustering:
        described += f", clustered by ({', '.join(clustering)})"

    return described
