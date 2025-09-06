from datetime import datetime
from typing import Dict, Any

import polars as pl
from pipeline.lib.asset_factories import build_large_socrata_asset

TAG_AUTO = {"domain": "auto", "source": "data.ny.gov"}

# If you ever want to rename raw Socrata fields → snake_case destination names, do it here.
mta_subway_hourly_ridership_rename_map: Dict[str, str] = {
    # Example:
    # "Station Complex": "station_complex",
    # "Station Complex ID": "station_complex_id",
    # Socrata GEOJSON properties usually arrive already snake-ish after generic_transform(),
    # so we keep this empty unless we find mismatches.
}

# Base schema config drives generic casting inside the factory's _auto_transform().
# _auto_transform() also ensures `date_column` exists (from the order field) and is first.
mta_subway_hourly_ridership_schema_cfg: Dict[str, Any] = dict(
    rename_map=mta_subway_hourly_ridership_rename_map,
    # Treat the order field as a date. The factory will make sure it's in date_cols
    # and add/overwrite `date_column` for us.
    date_cols=['transit_timestamp'],
    int_cols=['ridership'],
    float_cols=['latitude', 'longitude', 'transfers'],
    bool_cols=[],
)

def _mta_hourly_enhance(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Derive NYC-local calendar fields from the naive local transit_timestamp.
    Produces only DATE + INT fields (no timezone math, no DST ambiguity).

    Emits:
      - hour_of_day: INT 0..23 (from local timestamp)
      - dow: INT 0=Sun..6=Sat
      - day_date: DATE (local calendar day)
      - week_start_date: DATE (Sunday-anchored week start)
      - month_start_date: DATE (first of month)
      - year_num, month_num: INTs
    """
    t = pl.col("transit_timestamp").cast(pl.Datetime, strict=False)

    # PASS 1: base fields directly from the (NY-local, naive) timestamp
    lf = lf.with_columns([
        t.dt.hour().alias("hour_of_day"),
        ((t.dt.weekday() + 1) % 7).alias("dow"),                       # 0=Sun..6=Sat
        t.dt.date().alias("day_date"),                                 # DATE
        pl.date(t.dt.year(), t.dt.month(), pl.lit(1)).alias("month_start_date"),
        t.dt.year().alias("year_num"),
        t.dt.month().alias("month_num"),
    ])

    # PASS 2: week start date (Sunday anchor) using previously created columns (stays DATE)
    lf = lf.with_columns(
        (pl.col("day_date") - pl.duration(days=pl.col("dow"))).alias("week_start_date")
    )

    # Optional tidy ordering
    cols = lf.collect_schema().names()
    front = [c for c in [
        "day_date", "week_start_date", "month_start_date",
        "year_num", "month_num", "hour_of_day", "dow"
    ] if c in cols]
    rest = [c for c in cols if c not in front]
    return lf.select(front + rest)



# Build the RAW and CLEAN assets. The CLEAN output will include the mta_base fields.
mta_subway_hourly_ridership_raw, mta_subway_hourly_ridership = build_large_socrata_asset(
    asset_name="mta_subway_hourly_ridership",
    endpoint="https://data.ny.gov/resource/wujg-7c2s.geojson",
    schema_cfg=mta_subway_hourly_ridership_schema_cfg,
    # The Socrata ordering column; factory parses this and sets date_column accordingly.
    order_field="transit_timestamp ASC",
    start_date=datetime(2020, 7, 1),
    end_date=datetime(2025, 1, 1),
    partition_granularity="year",
    # This custom_fn adds the normalized, query-friendly columns directly into the clean parquet.
    custom_fn=_mta_hourly_enhance,
    tags=TAG_AUTO,
)
