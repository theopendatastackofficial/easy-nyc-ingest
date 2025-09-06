from pipeline.lib.asset_factories import build_medium_socrata_asset

from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.cityofnewyork.us"}

nyc_nypd_arrests_historical_rename_map: Dict[str, str] = {}

nyc_nypd_arrests_historical_schema_cfg: Dict[str, Any] = dict(
    rename_map = nyc_nypd_arrests_historical_rename_map,
    date_cols  = ['arrest_date'],
    int_cols   = ['jurisdiction_code','ky_cd', 'pd_cd' ],
    float_cols = ['arrest_precinct',  'latitude', 'longitude', 'x_coord_cd', 'y_coord_cd'],
    bool_cols  = [],
)


nyc_nypd_arrests_historical_raw, nyc_nypd_arrests_historical = build_medium_socrata_asset(
    asset_name = "nyc_nypd_arrests_historical",
    endpoint   = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json",
    schema_cfg = nyc_nypd_arrests_historical_schema_cfg,
    order_field = "arrest_date ASC",
    tags = TAG_AUTO,
)