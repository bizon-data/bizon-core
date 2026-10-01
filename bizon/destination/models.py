import secrets
from datetime import datetime

import polars as pl
from pytz import UTC

# Define the DataFrame DestinationRecord model
destination_record_schema = pl.Schema(
    [
        # Bizon system information
        ("bizon_id", str),
        ("bizon_extracted_at", pl.Datetime(time_unit="us", time_zone="UTC")),
        ("bizon_loaded_at", pl.Datetime(time_unit="us", time_zone="UTC")),
        # Source record information
        ("source_record_id", str),
        ("source_timestamp", pl.Datetime(time_unit="us", time_zone="UTC")),
        ("source_data", str),
    ]
)


_TIMESTAMP = pl.Datetime(time_unit="us", time_zone="UTC")


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def transform_to_df_destination_records(df_source_records: pl.DataFrame, extracted_at: datetime) -> pl.DataFrame:
    """Return a Polars DataFrame from a list of DestinationRecord objects"""
    height = df_source_records.height
    # One 128-bit draw for the whole frame: per-row uuid4() re-acquires the GIL on every urandom call.
    hex_ids = secrets.token_hex(16 * height)
    return df_source_records.select(
        pl.Series("bizon_id", [hex_ids[i : i + 32] for i in range(0, 32 * height, 32)], dtype=pl.String),
        pl.lit(_as_utc(extracted_at), dtype=_TIMESTAMP).alias("bizon_extracted_at"),
        pl.lit(datetime.now(tz=UTC), dtype=_TIMESTAMP).alias("bizon_loaded_at"),
        pl.col("id").alias("source_record_id"),
        pl.col("timestamp").alias("source_timestamp"),
        pl.col("data").alias("source_data"),
    )
