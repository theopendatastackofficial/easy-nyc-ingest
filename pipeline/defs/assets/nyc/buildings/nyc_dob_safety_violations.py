from pipeline.lib.asset_factories import build_medium_socrata_asset

from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.cityofnewyork.us"}

nyc_dob_safety_violations_rename_map: Dict[str, str] = {}

nyc_dob_safety_violations_schema_cfg: Dict[str, Any] = dict(
    rename_map = nyc_dob_safety_violations_rename_map,
    date_cols  = ['cycle_end_date', 'violation_issue_date'],
    int_cols   = [],
    float_cols = ['bbl', 'block', 'census_tract_2020_', 'community_board', 'council_district', 'latitude', 'longitude', 'lot'],
)

nyc_dob_safety_violations_raw, nyc_dob_safety_violations = build_medium_socrata_asset(
    asset_name = "nyc_dob_safety_violations",
    endpoint   = "https://data.cityofnewyork.us/resource/855j-jady.json",
    schema_cfg = nyc_dob_safety_violations_schema_cfg,
    order_field = "violation_issue_date ASC",
    tags = TAG_AUTO,
)