from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger

BIZON_TABLE_PREFIX = "_bizon_"


def resolve_default_table_id(
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    base_name: str,
    prefix: str = BIZON_TABLE_PREFIX,
) -> str:
    """Resolve the fully-qualified table id for an AUTO-GENERATED name.

    Reuses an existing legacy (unprefixed) table if one is already present, so existing
    pipelines keep writing to their current table. Brand-new pipelines get the ``prefix``
    (``_bizon_`` by default) so bizon-managed tables are clearly namespaced.

    Args:
        bq_client: BigQuery client used to check for an existing legacy table.
        project_id: BigQuery project id.
        dataset_id: BigQuery dataset id.
        base_name: Auto-generated table name, e.g. ``f"{source_name}_{stream_name}"``.
        prefix: Prefix applied to brand-new tables. Empty string disables prefixing.

    Returns:
        Fully-qualified ``project.dataset.table`` id.
    """
    legacy_id = f"{project_id}.{dataset_id}.{base_name}"

    # No prefix configured -> keep the historical behavior.
    if not prefix:
        return legacy_id

    prefixed_id = f"{project_id}.{dataset_id}.{prefix}{base_name}"

    try:
        bq_client.get_table(legacy_id)
        logger.info(f"Reusing existing legacy table {legacy_id} (skipping '{prefix}' prefix)")
        return legacy_id
    except NotFound:
        return prefixed_id
    except Exception as e:
        # Any other error (e.g. permissions / transient): fall back to the legacy name,
        # which is exactly the previous behavior, so we never regress an existing run.
        logger.warning(f"Could not check existence of {legacy_id} ({e}); falling back to legacy name")
        return legacy_id
