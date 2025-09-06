from pipeline.lib.asset_factories import build_single_socrata_asset
import polars as pl
from typing import Dict, Any

TAG_AUTO = {"domain": "auto", "source": "data.ny.gov"}

mta_operations_statement_schema_cfg = {
    # a) one-off rename: 'month' → 'timestamp'
    "rename_map": {"month": "timestamp"},

    # b) tell generic_transform() which columns are what
    "date_cols":  ["timestamp"],
    "int_cols":   ["fiscal_year", "financial_plan_year"],   # ← cast to Int64
    "float_cols": ["amount"],
    # bool_cols:   []   # none for this dataset
}

# ──────────── 2 ▸ add-agency custom_fn ────────────
def _add_agency_full_name(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Append `agency_full_name` based on the short code in `agency`.
    Every scalar literal must be wrapped in pl.lit(...) so Polars
    doesn’t try to resolve it as a column.
    """
    return lf.with_columns(
        pl.when(pl.col("agency") == "LIRR").then(pl.lit("Long Island Rail Road"))
         .when(pl.col("agency") == "BT").then(pl.lit("Bridges and Tunnels"))
         .when(pl.col("agency") == "FMTAC").then(
             pl.lit("First Mutual Transportation Assurance Company")
         )
         .when(pl.col("agency") == "NYCT").then(pl.lit("New York City Transit"))
         .when(pl.col("agency") == "SIR").then(pl.lit("Staten Island Railway"))
         .when(pl.col("agency") == "MTABC").then(pl.lit("MTA Bus Company"))
         .when(pl.col("agency") == "GCMCOC").then(
             pl.lit("Grand Central Madison Concourse Operating Company")
         )
         .when(pl.col("agency") == "MNR").then(pl.lit("Metro-North Railroad"))
         .when(pl.col("agency") == "MTAHQ").then(
             pl.lit("Metropolitan Transportation Authority Headquarters")
         )
         .when(pl.col("agency") == "CD").then(pl.lit("MTA Construction and Development"))
         .when(pl.col("agency") == "CRR").then(pl.lit("Connecticut Railroads"))
         .otherwise(pl.lit("Unknown Agency"))
         .alias("agency_full_name")
    )

mta_operations_statement_raw, mta_operations_statement = build_single_socrata_asset(
    asset_name  = "mta_operations_statement",
    endpoint    = "https://data.ny.gov/resource/yg77-3tkj.json",
    schema_cfg  = mta_operations_statement_schema_cfg,  
    custom_fn   = _add_agency_full_name,                
    order_field = "month ASC",
    tags        = TAG_AUTO,
)