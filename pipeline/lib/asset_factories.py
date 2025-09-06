# pipeline/lib/asset_factories.py
# ============================================================================
#  Dagster factory helpers for Socrata ingestion assets (single / medium / large).
#
#  Key behaviors:
#  --------------------------------------------------------------------------
#  • RAW parquet paths consistently use the *_raw* suffix across
#    *single*, *medium*, and *large* builders.
#  • The ordering column (token in `order_field`) is:
#        – automatically treated as a date column
#        – duplicated into a canonical `date_column`
#        – `date_column` is promoted to the first column in every clean file
#  • R2-related metadata/behavior removed.
#  • Option A (robust schema unification):
#      – CLEAN step transforms each chunk first, then uses diagonal_relaxed
#        concat to *union by name* and pad missing columns with nulls, while
#        also allowing dtype relaxation.
#  • Large builder:
#      – RAW is always fetched in monthly windows.
#      – CLEAN partitions selectable via `partition_granularity`: "year" or "month".
# ============================================================================

from __future__ import annotations

import os
import glob
import copy
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Mapping, Any, List, Tuple

import polars as pl
from dagster import AssetKey, asset, MetadataValue

from pipeline.constants import RAW_LAKE_PATH
from pipeline.lib.polars_helpers import generic_transform, to_snake_case


# ───────────────────────────── helpers ─────────────────────────────

def _portal_url(endpoint: str) -> str:
    if endpoint.endswith(".json"):
        return endpoint[:-5]
    if endpoint.endswith(".geojson"):
        return endpoint[:-8]
    return endpoint


def _default_tags(extra: Optional[dict[str, str]]) -> dict[str, str]:
    base = {"source": "socrata", "type": "ingestion"}
    base.update(extra or {})
    return base


def _meta(endpoint: str) -> dict[str, MetadataValue]:
    # keep existing key name for compatibility with dashboards
    return {"nyc_portal_url": MetadataValue.url(_portal_url(endpoint))}


def _extract_order_token(order_field: str) -> str:
    """
    Heuristic to extract the column token from an ORDER BY expression.
    Examples:
      "ts ASC" → "ts"
      "`timestamp` DESC" → "timestamp"
      "\"TimeStamp\" asc" → "TimeStamp"
    """
    token = (order_field or "").strip().split()[0]
    return token.strip("`\"")


# ──────────────────────── auto-transform wrapper ────────────────────────

def _auto_transform(
    *,
    schema_cfg: Mapping[str, Any],
    custom_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None,
    order_field: str,
) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    Build a transform function that:
      • runs generic_transform()
      • guarantees the ordering field is present as a Datetime
      • adds/overwrites `date_column`
      • promotes `date_column` to first place
    """
    raw_token   = _extract_order_token(order_field)
    token_snake = to_snake_case(pl.DataFrame({raw_token: []})).columns[0]
    rename_map  = schema_cfg.get("rename_map", {}) or {}
    final_field = rename_map.get(token_snake, token_snake)

    # ensure the ordering field is treated as a date
    cfg = copy.deepcopy(schema_cfg)
    date_cols = list(cfg.get("date_cols", []))  # allow mutation even if tuple
    if raw_token in date_cols:
        date_cols = [final_field if c == raw_token else c for c in date_cols]
    elif final_field not in date_cols:
        date_cols.append(final_field)
    cfg["date_cols"] = date_cols

    def _wrapped(lf: pl.LazyFrame) -> pl.LazyFrame:
        lf2 = generic_transform(lf, cfg, custom=custom_fn)

        # safeguard: if the final_field never appeared (e.g., all-null), inject as null datetime
        cols_after = lf2.collect_schema().names()
        if final_field not in cols_after:
            lf2 = lf2.with_columns(
                pl.lit(None).cast(pl.Datetime).alias(final_field)
            )

        # now add or reorder date_column
        cols = lf2.collect_schema().names()
        if final_field in cols:
            if "date_column" not in cols:
                lf2 = lf2.with_columns(pl.col(final_field).alias("date_column"))
                cols.insert(0, "date_column")
            else:
                cols = ["date_column"] + [c for c in cols if c != "date_column"]
            lf2 = lf2.select(cols)

        return lf2

    return _wrapped


def _resolve_transform(
    *,
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None,
    schema_cfg: Mapping[str, Any] | None,
    custom_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None,
    order_field: str | None,
) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    if transform_fn:
        return transform_fn
    if schema_cfg is None:
        raise ValueError("Either 'transform_fn' or 'schema_cfg' must be supplied.")
    if not order_field:
        raise ValueError("order_field must be supplied when using schema_cfg.")
    return _auto_transform(schema_cfg=schema_cfg, custom_fn=custom_fn, order_field=order_field)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  SMALL  (single-shot)                                                     ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def build_single_socrata_asset(
    *,
    asset_name: str,
    endpoint: str,
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    schema_cfg : Mapping[str, Any] | None = None,
    custom_fn  : Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    order_field: str | None = None,
    where_clause: str | None = None,
    additional_params: dict[str, str] | None = None,
    limit: int = 500_000,
    tags: Optional[dict[str, str]] = None,
    description_raw: str = "Raw dataframe – single parquet.",
    description_clean: str = "Cleaned dataframe – single parquet.",
):
    raw_key    = f"{asset_name}_raw"
    asset_tags = _default_tags(tags)
    meta       = _meta(endpoint)

    # RAW --------------------------------------------------------------------
    @asset(
        name                   = raw_key,
        io_manager_key         = "raw_single_io_manager",
        group_name             = "single_file_assets",
        description            = description_raw,
        required_resource_keys = {"socrata", "raw_single_io_manager"},
        tags                   = asset_tags,
        metadata               = meta,
    )
    def raw_asset(context):
        params: Dict[str, Any] = {"$limit": limit}
        if order_field:  params["$order"] = order_field
        if where_clause: params["$where"] = where_clause
        if additional_params:
            params.update(additional_params)

        recs = context.resources.socrata.fetch_data(endpoint, params)
        df   = pl.DataFrame(recs)
        context.log.info(f"[{raw_key}] downloaded {df.height:,} rows")
        context.add_output_metadata({"row_count": df.height})
        return df

    # CLEAN ------------------------------------------------------------------
    _transform = _resolve_transform(
        transform_fn=transform_fn,
        schema_cfg =schema_cfg,
        custom_fn  =custom_fn,
        order_field=order_field,
    )

    @asset(
        name           = asset_name,
        deps           = [AssetKey(raw_key)],
        io_manager_key = "clean_single_io_manager",
        group_name     = "single_file_assets",
        description    = description_clean,
        tags           = asset_tags,
        metadata       = meta,
    )
    def clean_asset(context):
        raw_pq = os.path.join(RAW_LAKE_PATH, raw_key, f"{raw_key}.parquet")
        if not os.path.exists(raw_pq):
            raise FileNotFoundError(raw_pq)

        raw_lf   = pl.scan_parquet(raw_pq)
        clean_lf = _transform(raw_lf)
        return clean_lf  # IO manager can stream LazyFrame

    return raw_asset, clean_asset


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  MEDIUM  (offset-paging)                                                  ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def build_medium_socrata_asset(
    *,
    asset_name: str,
    endpoint: str,
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    schema_cfg : Mapping[str, Any] | None = None,
    custom_fn  : Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    order_field: str,
    where_clause: str | None = None,
    additional_params: dict[str, str] | None = None,
    limit: int = 500_000,
    tags: Optional[dict[str, str]] = None,
    description_raw: str = "Unprocessed chunked parquet files (medium).",
    description_clean: str = "Consolidated & transformed single parquet (medium).",
):
    raw_key    = f"{asset_name}_raw"
    asset_tags = _default_tags(tags)
    meta       = _meta(endpoint)
    _transform = _resolve_transform(
        transform_fn=transform_fn,
        schema_cfg =schema_cfg,
        custom_fn  =custom_fn,
        order_field=order_field,
    )

    # RAW --------------------------------------------------------------------
    @asset(
        name                   = raw_key,
        group_name             = "medium_assets",
        description            = description_raw,
        required_resource_keys = {"socrata", "raw_medium_io_manager"},
        tags                   = asset_tags,
        metadata               = meta,
    )
    def raw_asset(context) -> str:
        mgr    = context.resources.raw_medium_io_manager
        offset = batch = total = 0
        while True:
            query: Dict[str, Any] = {"$limit": limit, "$offset": offset, "$order": order_field}
            if where_clause:      query["$where"] = where_clause
            if additional_params: query.update(additional_params)

            recs = context.resources.socrata.fetch_data(endpoint, query)
            if not recs:
                break
            df = pl.DataFrame(recs)
            batch += 1
            total += df.height
            mgr.write_chunk(raw_key, batch, df)   # *_raw* folder
            offset += limit
        return f"{total:,} rows"

    # CLEAN ------------------------------------------------------------------
    @asset(
        name                   = asset_name,
        deps                   = [AssetKey(raw_key)],
        group_name             = "medium_assets",
        description            = description_clean,
        required_resource_keys = {"clean_medium_io_manager"},
        tags                   = asset_tags,
        metadata               = meta,
    )
    def clean_asset(context) -> str:
        raw_dir = os.path.join(RAW_LAKE_PATH, raw_key)
        files   = sorted(glob.glob(os.path.join(raw_dir, "*.parquet")))
        if not files:
            return "skip"

        # Option A: transform each chunk FIRST, then relaxed *union-by-name* concat
        chunk_lfs = [_transform(pl.scan_parquet(f)) for f in files]
        merged_lf = pl.concat(chunk_lfs, how="diagonal_relaxed")

        # Streamed write via IO manager (no big collect)
        context.resources.clean_medium_io_manager.write_single(asset_name, merged_lf)
        context.log.info(f"[{asset_name}] wrote consolidated parquet")
        return "done"

    return raw_asset, clean_asset


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  LARGE  (RAW monthly; CLEAN yearly OR monthly)                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def build_large_socrata_asset(
    *,
    asset_name: str,
    endpoint: str,
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    schema_cfg: Mapping[str, Any] | None = None,
    custom_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    order_field: str,
    start_date: datetime,
    end_date: datetime,
    # CLEAN granularity (RAW is always monthly)
    partition_granularity: str = "month",   # 'month' or 'year'
    additional_params: dict[str, str] | None = None,
    limit: int = 500_000,
    tags: Optional[dict[str, str]] = None,
    description_raw: str = "Unprocessed chunked parquet files (partitioned monthly).",
    description_clean: str = "Partitioned parquet w/ transformations (large).",
) -> Tuple[Callable, Callable]:
    if partition_granularity not in {"month", "year"}:
        raise ValueError("partition_granularity must be 'month' or 'year'")

    raw_key = f"{asset_name}_raw"
    asset_tags = {"source": "socrata", "type": "ingestion", **(tags or {})}
    meta = {"nyc_portal_url": MetadataValue.url(endpoint.rstrip(".geojson").rstrip(".json"))}

    _transform = _resolve_transform(
        transform_fn=transform_fn,
        schema_cfg=schema_cfg,
        custom_fn=custom_fn,
        order_field=order_field,
    )

    def _monthly_windows() -> List[Tuple[datetime, datetime]]:
        cur = start_date.replace(day=1)
        windows: List[Tuple[datetime, datetime]] = []
        while cur < end_date:
            nxt = (cur + timedelta(days=32)).replace(day=1)
            windows.append((cur, nxt))
            cur = nxt
        return windows

    # ───────────────────────────── RAW (always monthly) ───────────────────────────── #
    @asset(
        name=raw_key,
        group_name="large_assets",
        description=description_raw,
        required_resource_keys={"socrata", "raw_large_io_manager"},
        tags=asset_tags,
        metadata=meta,
    )
    def raw_asset(context) -> str:
        mgr = context.resources.raw_large_io_manager
        chunks = 0
        raw_token = _extract_order_token(order_field)

        for win_start, win_end in _monthly_windows():
            offset = batch = 0
            where = (
                f"{raw_token} >= '{win_start:%Y-%m-%dT00:00:00}' AND "
                f"{raw_token} < '{win_end:%Y-%m-%dT00:00:00}'"
            )
            while True:
                query = {"$limit": limit, "$offset": offset, "$order": order_field, "$where": where}
                if additional_params:
                    query.update(additional_params)

                recs = context.resources.socrata.fetch_data(endpoint, query)
                if not recs:
                    break

                df = pl.DataFrame(recs)
                batch += 1
                chunks += 1

                mgr.write_chunk(
                    raw_key,
                    batch,
                    df,
                    year=win_start.year,
                    month=win_start.month,
                )
                offset += limit

        return f"{chunks} chunks"

    # ───────────────────────────── CLEAN (yearly OR monthly) ───────────────────────── #
    @asset(
        name=asset_name,
        deps=[AssetKey(raw_key)],
        group_name="large_assets",
        description=description_clean,
        required_resource_keys={"clean_large_io_manager"},
        tags=asset_tags,
        metadata=meta,
    )
    def clean_asset(context) -> str:
        raw_root = os.path.join(RAW_LAKE_PATH, raw_key)
        mgr = context.resources.clean_large_io_manager
        processed = 0

        # Iterate by year directory
        for y_path in sorted(glob.glob(os.path.join(raw_root, "year=*"))):
            yr = int(os.path.basename(y_path).split("=", 1)[1])

            if partition_granularity == "month":
                # Process each month separately → monthly clean partitions
                month_dirs = sorted(glob.glob(os.path.join(y_path, "month=*")))
                for m_path in month_dirs:
                    mo = int(os.path.basename(m_path).split("=", 1)[1])
                    files = sorted(glob.glob(os.path.join(m_path, "*.parquet")))
                    if not files:
                        continue

                    # Option A: transform each file first, then relaxed union-by-name concat
                    chunk_lfs = [_transform(pl.scan_parquet(f)) for f in files]
                    merged    = pl.concat(chunk_lfs, how="diagonal_relaxed")
                    clean_df  = merged.collect()

                    mgr.write_chunk(
                        asset_name,
                        1,
                        clean_df,
                        year=yr,
                        month=mo,
                    )
                    processed += 1

            else:  # yearly clean
                files = sorted(glob.glob(os.path.join(y_path, "**", "*.parquet"), recursive=True))
                if not files:
                    continue

                # Option A: transform each file first, then relaxed union-by-name concat
                chunk_lfs = [_transform(pl.scan_parquet(f)) for f in files]
                merged    = pl.concat(chunk_lfs, how="diagonal_relaxed")
                clean_df  = merged.collect()

                # One yearly partition (month=None)
                mgr.write_chunk(
                    asset_name,
                    1,
                    clean_df,
                    year=yr,
                    month=None,
                )
                processed += 1

        return f"{processed} {'monthly' if partition_granularity=='month' else 'yearly'} partitions"

    return raw_asset, clean_asset
