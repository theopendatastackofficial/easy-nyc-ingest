import os
from functools import cached_property
from typing import Literal, Optional

import polars as pl
from dagster import (
    IOManager,
    io_manager,
    ConfigurableResource,
    InputContext,
    OutputContext,
    get_dagster_logger,
)


class PolarsParquetIOManager(IOManager, ConfigurableResource):
    """
    Universal Polars ↔ Parquet IO-manager.

    ────────────────────────────────────────────────────────────────
    mode = "single"
        <base_dir>/<asset>/<asset>.parquet
        (read + write using streaming when LazyFrame)

    mode = "flat_chunk"
        <base_dir>/<asset>/<asset>_<batch>.parquet
        (write_chunk only)

    mode = "hive_chunk"
        <base_dir>/<asset>/year=YYYY[/month=MM]/<asset>_YYYY[MM]_<batch>.parquet
        (write_chunk only)
    """

    # Resource configuration
    base_dir: str
    mode: Literal["single", "flat_chunk", "hive_chunk"] = "single"

    # Dagster logger
    @cached_property
    def _log(self):
        return get_dagster_logger()

    # ------------------------------------------------------------------ #
    @staticmethod
    def _drop_all_null_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Remove columns that are entirely NULL (saves space & avoids dict-0 bug)."""
        keep = [c for c in df.columns if df[c].null_count() < df.height]
        return df.select(keep)

    # ------------------------------------------------------------------ #
    def _path(
        self,
        *,
        asset: str,
        batch: Optional[int] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> str:
        if self.mode == "single":
            return os.path.join(self.base_dir, asset, f"{asset}.parquet")

        if self.mode == "flat_chunk":
            assert batch is not None, "batch required for flat_chunk mode"
            return os.path.join(self.base_dir, asset, f"{asset}_{batch}.parquet")

        # hive_chunk
        assert year is not None, "year required for hive_chunk mode"
        parts = [self.base_dir, asset, f"year={year:04d}"]
        token = f"{year:04d}"
        if month is not None:
            parts.append(f"month={month:02d}")
            token += f"{month:02d}"
        assert batch is not None, "batch required for hive_chunk mode"
        fname = f"{asset}_{token}_{batch}.parquet"
        return os.path.join(*(parts + [fname]))

    # ------------------------------------------------------------------ #
    def write_chunk(
        self,
        asset_name: str,
        batch_num: int,
        df: pl.DataFrame,
        *,
        year: int | None = None,
        month: int | None = None,
    ) -> None:
        if self.mode == "single":
            raise RuntimeError("write_chunk() not available in 'single' mode")

        path = self._path(asset=asset_name, batch=batch_num, year=year, month=month)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        cleaned = self._drop_all_null_columns(df)
        cleaned.write_parquet(path, compression="zstd", use_pyarrow=True)

        self._log.info(f"[{self.mode}] wrote {cleaned.shape[0]:,} rows → {path}")

    # ------------------------------------------------------------------ #
    def write_single(self, asset_name: str, obj: pl.DataFrame | pl.LazyFrame) -> None:
        if self.mode != "single":
            raise RuntimeError("write_single() only valid in 'single' mode")

        path = self._path(asset=asset_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pl.LazyFrame):
            # STREAM write directly from LazyFrame → constant memory footprint
            obj.sink_parquet(path, compression="zstd")
            self._log.info(f"[single] streamed {path}")
            return

        if isinstance(obj, pl.DataFrame):
            cleaned = self._drop_all_null_columns(obj)
            cleaned.write_parquet(path, compression="zstd", use_pyarrow=True)

            removed = set(obj.columns) - set(cleaned.columns)
            if removed:
                self._log.info(f"[single] dropped {len(removed)} all-NULL columns: {sorted(removed)}")
            self._log.info(f"[single] wrote {path}")
            return

        raise TypeError("Only Polars (Lazy)DataFrame objects are supported")

    # ------------------------------------------------------------------ #
    # Dagster glue
    def handle_output(self, context: OutputContext, obj):
        if self.mode == "single":
            self.write_single(context.asset_key.to_python_identifier(), obj)
        else:
            context.log.debug(f"[{self.mode}] chunks already on disk – no-op")

    def load_input(self, context: InputContext):
        if self.mode != "single":
            raise NotImplementedError("load_input() only implemented for 'single' mode")

        path = self._path(asset=context.asset_key.to_python_identifier())
        if not os.path.exists(path):
            raise FileNotFoundError(path)

        df = pl.read_parquet(path)
        context.log.info(f"[single] loaded {len(df):,} rows ← {path}")
        return df


# ---------------------------------------------------------------------- #
@io_manager(
    config_schema={
        "base_dir": str,
        "mode": str,   # single | flat_chunk | hive_chunk
    }
)
def polars_parquet_io_manager(init_context):
    cfg = init_context.resource_config
    return PolarsParquetIOManager(
        base_dir=cfg["base_dir"],
        mode=cfg.get("mode", "single"),
    )
