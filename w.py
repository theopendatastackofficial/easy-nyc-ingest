# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb", "pyyaml"]
# ///
"""
Build (or rebuild) a DuckDB warehouse from a minimal YAML:

  paths:
    clean:      data/opendata/clean
    raw:        data/opendata/raw
    analytics:  data/opendata/analytics
    warehouse:  data/duckdb/data.duckdb
  search_order: [analytics, clean, raw]   # optional
  assets:
    - asset_a
    - asset_b

Autodetects flat vs hive (year=/month=) layouts; user does NOT need
to specify partitioning. Creates 1 VIEW per asset (or TABLE with --as-tables).
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Dict

import duckdb
import yaml


# ------------------------------ helpers ------------------------------

def _expand_relative(base_dir: Path, p: str | Path) -> Path:
    """
    Expand env vars and ~, then resolve relative to `base_dir` if still relative.
    This makes YAML paths intuitive: they are relative to the YAML file location.
    """
    s = str(p)
    s = os.path.expandvars(os.path.expanduser(s))
    candidate = Path(s)
    if not candidate.is_absolute():
        candidate = (base_dir / candidate)
    return candidate.resolve()


@dataclass
class Config:
    paths: Dict[str, Path]        # keys: clean, raw, analytics, warehouse
    search_order: List[str]       # e.g., ["analytics", "clean", "raw"]
    assets: List[str]


def load_config(yaml_path: Path) -> Config:
    data = yaml.safe_load(yaml_path.read_text())

    if not isinstance(data, dict):
        raise ValueError(f"{yaml_path} must be a YAML mapping")

    # Resolve relative paths with respect to the YAML file’s directory
    yaml_dir = yaml_path.parent

    paths = data.get("paths", {})
    required = ["clean", "raw", "analytics", "warehouse"]
    missing = [k for k in required if k not in paths]
    if missing:
        raise ValueError(f"paths: missing keys {missing} in {yaml_path}")

    cfg_paths = {k: _expand_relative(yaml_dir, paths[k]) for k in required}
    cfg_paths["warehouse"].parent.mkdir(parents=True, exist_ok=True)

    search_order = data.get("search_order") or ["analytics", "clean", "raw"]
    search_order = [s.lower() for s in search_order]
    for s in search_order:
        if s not in ("analytics", "clean", "raw"):
            raise ValueError(f"search_order contains invalid entry: {s}")

    assets = data.get("assets") or []
    if not assets or not isinstance(assets, list) or not all(isinstance(a, str) for a in assets):
        raise ValueError("assets: must be a non-empty list of asset names (strings)")

    return Config(paths=cfg_paths, search_order=search_order, assets=assets)


def find_first_glob(base: Path, asset: str) -> Tuple[str, str] | None:
    """
    Try a sequence of patterns under base/asset and return (pattern, kind).
    kind is for logging: "hive_monthly", "hive_yearly", "flat", "deep".
    """
    root = base / asset
    candidates: List[Tuple[Path, str]] = [
        (root / "year=*" / "month=*" / "*.parquet", "hive_monthly"),
        (root / "year=*" / "*.parquet",            "hive_yearly"),
        (root / "*.parquet",                       "flat"),
        (root / "**" / "*.parquet",                "deep"),
    ]
    for pat, kind in candidates:
        pat_str = str(pat.as_posix())
        if glob.glob(pat_str, recursive=True):
            return (pat_str, kind)
    return None


def autodetect_glob(cfg: Config, asset: str) -> Tuple[str, str] | None:
    """Search layers in cfg.search_order and return the first matching (glob, kind)."""
    for layer in cfg.search_order:
        base = cfg.paths[layer]
        got = find_first_glob(base, asset)
        if got:
            return got
    return None


def ensure_httpfs(con: duckdb.DuckDBPyConnection):
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")


def create_views_or_tables(
    con: duckdb.DuckDBPyConnection,
    items: List[Tuple[str, str, str]],   # (asset, glob, kind)
    *,
    as_tables: bool,
    verbose: bool,
):
    """
    Create OR REPLACE VIEW/TABLE for each asset.
    Always sets hive_partitioning=true + union_by_name=true.
    Also writes a registry table with (asset_name, parquet_glob, kind).
    """
    rows = []
    for asset, pat, kind in items:
        stmt = "TABLE" if as_tables else "VIEW"
        sql = f"""
        CREATE OR REPLACE {stmt} {asset} AS
        SELECT *
        FROM read_parquet(
          '{pat}',
          hive_partitioning = true,
          union_by_name     = true
        );
        """
        con.execute(sql)
        rows.append((asset, pat, kind))
        if verbose:
            print(f"[+] {stmt:<5} {asset:30s}  ←  {kind:12s}  ({pat})")

    con.execute("DROP TABLE IF EXISTS registry__assets;")
    con.execute(
        "CREATE TABLE registry__assets (asset_name TEXT, parquet_glob TEXT, detected_kind TEXT);"
    )
    con.executemany("INSERT INTO registry__assets VALUES (?, ?, ?);", rows)
    if verbose:
        print(f"[✓] Registered {len(rows)} {'tables' if as_tables else 'views'}")


# ------------------------------ CLI ------------------------------

def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build a DuckDB warehouse from a simple YAML (paths + assets)."
    )
    p.add_argument(
        "-c", "--config",
        type=str,
        default="datasets.yaml",
        help="Path to YAML config (default: datasets.yaml at repo root)",
    )

    g = p.add_mutually_exclusive_group()
    g.add_argument("--wipe", dest="wipe", action="store_true", help="Remove existing warehouse first (default)")
    g.add_argument("--no-wipe", dest="wipe", action="store_false", help="Keep existing file if present")
    p.set_defaults(wipe=True)

    p.add_argument("--as-tables", action="store_true", help="Create TABLEs instead of VIEWs")
    p.add_argument("--only", type=str, default="", help="Comma-separated exact asset names to include")
    p.add_argument("--exclude", type=str, default="", help="Comma-separated substrings to exclude")
    p.add_argument("--list", action="store_true", help="List what would be registered; do not write DB")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    return p.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    cfg = load_config(Path(args.config).resolve())

    only = {s.strip() for s in args.only.split(",") if s.strip()} or None
    exclude = {s.strip() for s in args.exclude.split(",") if s.strip()}

    selected: List[Tuple[str, str, str]] = []
    for a in cfg.assets:
        if only and a not in only:
            continue
        if exclude and any(tok in a for tok in exclude):
            continue
        sel = autodetect_glob(cfg, a)
        if sel is None:
            if args.verbose:
                print(f"[-] Skip {a:30s}: no parquet files found in any layer")
            continue
        pat, kind = sel
        selected.append((a, pat, kind))

    if args.list:
        if not selected:
            print("No assets matched (or no data present).")
            return 0
        print("\nAssets that would be registered:")
        for a, pat, kind in selected:
            print(f"  - {a:30s} [{kind:12s}] ← {pat}")
        return 0

    wh = cfg.paths["warehouse"]
    if wh.exists() and args.wipe:
        wh.unlink()
        if args.verbose:
            print(f"[•] Removed previous warehouse → {wh}")

    # atomic write: build to tmp then replace
    tmp = wh.with_suffix(wh.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink(missing_ok=True)

    con = duckdb.connect(str(tmp), read_only=False)
    try:
        ensure_httpfs(con)
        create_views_or_tables(con, selected, as_tables=args.as_tables, verbose=args.verbose)
    finally:
        con.close()

    tmp.replace(wh)
    print(f"[✓] Warehouse ready → {wh}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
