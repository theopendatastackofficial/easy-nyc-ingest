from pipeline.lib.asset_factories import build_single_socrata_asset

from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.ny.gov"}

from datetime import datetime

from dagster import asset, MetadataValue

from pipeline.lib.asset_factories import build_single_socrata_asset
import polars as pl
from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.ny.gov"}

mta_daily_ridership_rename_map: Dict[str, str] = {}

mta_daily_ridership_schema_cfg: Dict[str, Any] = dict(
    rename_map = mta_daily_ridership_rename_map,
    date_cols  = ['date'],
    int_cols   = [],
    float_cols = ['access_a_ride_of_comparable_pre_pandemic_day', 'access_a_ride_total_scheduled_trips', 'bridges_and_tunnels_of_comparable_pre_pandemic_day', 'bridges_and_tunnels_total_traffic', 'buses_of_comparable_pre_pandemic_day', 'buses_total_estimated_ridersip', 'lirr_of_comparable_pre_pandemic_day', 'lirr_total_estimated_ridership', 'metro_north_of_comparable_pre_pandemic_day', 'metro_north_total_estimated_ridership', 'staten_island_railway_of_comparable_pre_pandemic_day', 'staten_island_railway_total_estimated_ridership', 'subways_of_comparable_pre_pandemic_day', 'subways_total_estimated_ridership'],
)

mta_daily_ridership_raw, mta_daily_ridership = build_single_socrata_asset(
    asset_name = "mta_daily_ridership",
    endpoint   = "https://data.ny.gov/resource/vxuj-8kew.json",
    schema_cfg = mta_daily_ridership_schema_cfg,
    order_field = "date ASC",
    tags = TAG_AUTO,
)