from datetime import datetime
from uuid import uuid4

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


def transform_to_df_destination_records(df_source_records: pl.DataFrame, extracted_at: datetime) -> pl.DataFrame:
    """Return a Polars DataFrame from a list of DestinationRecord objects"""
    return df_source_records.select(
        pl.Series([uuid4().hex for _ in range(df_source_records.height)]).alias("bizon_id"),
        pl.Series([extracted_at for _ in range(df_source_records.height)]).alias("bizon_extracted_at"),
        pl.Series([datetime.now(tz=UTC) for _ in range(df_source_records.height)]).alias("bizon_loaded_at"),
        pl.col("id").alias("source_record_id"),
        pl.col("timestamp").alias("source_timestamp"),
        pl.col("data").alias("source_data"),
    )
