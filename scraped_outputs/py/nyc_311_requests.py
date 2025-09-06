from datetime import datetime
from pipeline.lib.asset_factories import build_large_socrata_asset

from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.cityofnewyork.us"}

nyc_311_requests_rename_map: Dict[str, str] = {}

nyc_311_requests_schema_cfg: Dict[str, Any] = dict(
    rename_map = nyc_311_requests_rename_map,
    date_cols  = ['closed_date', 'created_date', 'due_date', 'resolution_action_updated_date'],
    int_cols   = [],
    float_cols = ['latitude', 'longitude', 'x_coordinate_state_plane', 'y_coordinate_state_plane'],
    bool_cols  = [],
)

nyc_311_requests_raw, nyc_311_requests = build_large_socrata_asset(
    asset_name = "nyc_311_requests",
    endpoint   = "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
    schema_cfg = nyc_311_requests_schema_cfg,
    order_field = "closed_date ASC",
    # TODO: adjust date window
    start_date = datetime(2020, 1, 1),
    end_date   = datetime(2025, 1, 1),
    tags = TAG_AUTO,
)