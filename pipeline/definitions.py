# pipeline/definitions.py
import dagster as dg
from dagster.components import load_defs

import pipeline.defs
from pipeline.constants import RAW_LAKE_PATH, CLEAN_LAKE_PATH
from pipeline.defs.resources.polars_parquet_io_manager import polars_parquet_io_manager
from pipeline.defs.resources.socrata_resource import SocrataResource

_shared_resources = {
    "socrata": SocrataResource(),

    # single-file assets
    "raw_single_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": RAW_LAKE_PATH, "mode": "single"}
    ),
    "clean_single_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": CLEAN_LAKE_PATH, "mode": "single"}
    ),

    # medium assets – chunked raws / single clean
    "raw_medium_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": RAW_LAKE_PATH, "mode": "flat_chunk"}
    ),
    "clean_medium_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": CLEAN_LAKE_PATH, "mode": "single"}
    ),

    # large assets – hive-partitioned raws *and* cleans
    "raw_large_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": RAW_LAKE_PATH, "mode": "hive_chunk"}
    ),
    "clean_large_io_manager": polars_parquet_io_manager.configured(
        {"base_dir": CLEAN_LAKE_PATH, "mode": "hive_chunk"}
    ),
}

_assets = load_defs(pipeline.defs).assets

defs = dg.Definitions(
    resources=_shared_resources,
    assets=_assets,
)
