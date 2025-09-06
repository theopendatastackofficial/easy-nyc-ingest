# pipeline/lib/polars_helpers.py
from __future__ import annotations

import polars as pl
import re
from typing import Mapping, Callable, Any


# ----------------- basic helpers -----------------

def _colnames(df: pl.DataFrame | pl.LazyFrame) -> list[str]:
    """Return column names whether DF or LazyFrame."""
    return df.collect_schema().names() if isinstance(df, pl.LazyFrame) else list(df.columns)


def to_snake_case(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Convert all column names to snake_case:
      • Lowercase
      • Replace spaces and hyphens with underscores
      • Strip invalid characters
    """
    mapping = {
        c: re.sub(r"[^0-9a-z_]+", "", c.lower().replace(" ", "_").replace("-", "_"))
        for c in _colnames(df)
    }
    return df.rename(mapping)


# ----------------- generic transform -----------------

def generic_transform(
    df: pl.DataFrame | pl.LazyFrame,
    schema_cfg: Mapping[str, Any],
    *,
    custom: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
) -> pl.LazyFrame:
    """
    Apply generic schema-based transformations:

      - Rename columns (schema_cfg["rename_map"])
      - Ensure type conversions (schema_cfg["dtype_overrides"])
      - Normalize column names to snake_case
      - Apply optional custom function

    Returns a LazyFrame.
    """
    # Always work in LazyFrame
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df

    # Snake case renaming
    lf = to_snake_case(lf)

    # Column renames from config
    rename_map: dict[str, str] = schema_cfg.get("rename_map", {}) or {}
    if rename_map:
        lf = lf.rename(rename_map)

    # Dtype overrides
    dtype_overrides: dict[str, Any] = schema_cfg.get("dtype_overrides", {}) or {}
    for col, dtype in dtype_overrides.items():
        if col in lf.collect_schema().names():
            lf = lf.with_columns(pl.col(col).cast(dtype, strict=False))

    # Custom user-supplied function
    if custom:
        lf = custom(lf)

    return lf
