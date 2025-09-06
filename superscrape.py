# /// script
# requires-python = ">=3.10"
# dependencies = ["beautifulsoup4", "requests"]
# ///

from __future__ import annotations

import argparse, csv, json, logging, random, re, sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)
log = logging.getLogger(__name__)

# ───────────────────────── regexes / const ────────────────────────────────
DATASET_ID_RE = re.compile(r"/([A-Za-z0-9]{4}-[A-Za-z0-9]{4})(?:[/?]|$)")
ROWS_RE       = re.compile(r"([\d,.]+)\s*([KkMm]?)")

BOOL_HINTS = ("flag", "indicator", "boolean", "bool", "is_", "has_")
INT_HINTS  = (
    "id", "code", "num", "number", "count", "total", "units", "year",
    "zip", "bbl", "tract", "block", "lot", "value", "area", "front", "depth"
)
FLOAT_HINTS = ("ratio", "percent", "rate", "far", "latitude", "longitude", "coord")

SIZE_LIMIT = {"single": 500_000, "medium": 20_000_000}    # >20 M → large

USAGE_TEXT = """
Usage:

CSV mode (positional):
  uv run superscrape.py test.csv
  uv run superscrape.py test.csv out_dir

CSV mode (flags, backward compatible):
  uv run superscrape.py -i test.csv -o out_dir

Single asset mode (asset name + URL):
  uv run superscrape.py mta_daily_ridership https://data.ny.gov/.../vxuj-8kew

Default output directory (when not provided): scraped_outputs
"""

# ─────────────────────────── helpers ──────────────────────────────────────
def _strip_html(text: str | None) -> str:
    return BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)

def _dataset_id(url: str) -> str | None:
    m = DATASET_ID_RE.search(url)
    return m.group(1).lower() if m else None

def _canonical(host: str, dsid: str) -> str:
    return f"https://{host}/resource/{dsid}.json"

def _views_meta_url(host: str, dsid: str) -> str:
    return f"https://{host}/api/views/{dsid}.json"

def _views_meta_v1_url(host: str, dsid: str) -> str:
    return f"https://{host}/api/views/metadata/v1/{dsid}"

def _get_json(url: str, *, timeout: int = 30) -> dict:
    log.debug("→ %s", url)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _get_html(url: str) -> str | None:
    try:
        log.debug("→ %s (html)", url)
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as exc:                      # noqa: BLE001
        log.debug("html fetch failed: %s", exc)
        return None

def _parse_rows_token(token: str) -> int | None:
    m = ROWS_RE.search(token.replace(" ", ""))
    if not m:
        return None
    num, suffix = m.groups()
    value = float(num.replace(",", ""))
    if suffix.upper() == "M":
        value *= 1_000_000
    elif suffix.upper() == "K":
        value *= 1_000
    return int(value)

def _looks_like_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return u.scheme in {"http", "https"} and bool(u.netloc)
    except Exception:
        return False

# ────────── fast row-count helpers ────────────────────────────────────────
def _rows_from_header(endpoint: str) -> int | None:
    try:
        r = requests.head(endpoint, timeout=10)
        r.raise_for_status()
        for k, v in r.headers.items():
            if "row-count" in k.lower():
                return int(v)
    except Exception:
        pass
    return None

def _rows_from_meta_v1(host: str, dsid: str) -> int | None:
    try:
        data = _get_json(_views_meta_v1_url(host, dsid), timeout=20)
        val = data.get("rowCount") or data.get("rows")
        return int(val) if val is not None else None
    except Exception:
        return None

def _rows_from_meta_v2(meta: dict) -> int | None:
    view = meta.get("view", {}) or meta
    direct = view.get("count") or view.get("metrics", {}).get("result_count")
    if isinstance(direct, (int, float, str)):
        return int(direct)
    best = 0
    for c in meta.get("columns", []) or []:
        val = c.get("cachedContents", {}).get("non_null")
        if val:
            best = max(best, int(val))
    return best or None

def _rows_from_catalog(host: str, dsid: str) -> int | None:
    try:
        url = f"https://{host}/api/catalog/v1?ids={dsid}&limit=1"
        res = (_get_json(url, timeout=20).get("results") or [])[0]
        rows = res.get("resource", {}).get("rows") or res.get("resource", {}).get("row_count")
        return int(rows) if rows else None
    except Exception:
        return None

def _rows_from_count_query(endpoint: str) -> int | None:
    try:
        url = f"{endpoint}?$query=SELECT%20count(*)"
        log.info("⚠️  issuing count(*) query: %s", url)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        raw = (data[0].get("count") if isinstance(data, list) and data else None)
        return int(raw) if raw is not None else None
    except Exception:
        return None

# ────────── heuristics for number-typed columns ───────────────────────────
def _classify_number(field: str, desc: str) -> str:
    name = field.lower()
    text = desc.lower()
    if any(k in name for k in BOOL_HINTS) or "boolean" in text:
        return "bool_cols"
    if any(k in name for k in INT_HINTS):
        return "int_cols"
    if any(k in name for k in FLOAT_HINTS):
        return "float_cols"
    return "float_cols"

# ────────── build data-dictionary + buckets ───────────────────────────────
def _bucket_columns(cols: List[dict]) -> Tuple[List[dict], Dict[str, List[str]]]:
    dd: List[dict] = []
    buckets: Dict[str, List[str]] = {
        "date_cols": [], "int_cols": [], "float_cols": [], "bool_cols": []
    }

    for c in cols:
        field = c.get("fieldName", "")
        if field.startswith(":@computed_region_"):
            continue

        dt  = (c.get("dataTypeName") or "").lower()
        desc = _strip_html(c.get("description") or "")

        if dt in ("calendar_date", "floating_timestamp", "fixed_timestamp"):
            buckets["date_cols"].append(field)
        elif dt == "number":
            buckets[_classify_number(field, desc)].append(field)
        elif dt == "checkbox":
            buckets["bool_cols"].append(field)

        dd.append({
            "column_name":    c.get("name"),
            "description":    desc,
            "api_field_name": field,
            "data_type":      dt,
        })

    for k in buckets:
        buckets[k] = sorted(set(buckets[k]))
    return dd, buckets

# ────────── misc helpers ──────────────────────────────────────────────────
def _asset_size(row_count: int | None) -> str:
    if row_count is None:
        return "medium"
    if row_count <= SIZE_LIMIT["single"]:
        return "single"
    if row_count <= SIZE_LIMIT["medium"]:
        return "medium"
    return "large"

def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")

# ────────── CLI parsing with positional support ───────────────────────────
def parse_args(argv: List[str]):
    """
    Supports:
      - flags:       -i INPUT.csv -o out_dir
      - positional:  INPUT.csv [out_dir]
      - single row:  ASSET_NAME URL
    """
    p = argparse.ArgumentParser(
        description="Generate asset stubs from Socrata datasets.\n\n" + USAGE_TEXT,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # backwards-compatible flags
    p.add_argument("-i", "--input", help="CSV with headers: asset_name,url")
    p.add_argument("-o", "--output", help="Output directory (default: scraped_outputs)")
    # optional positionals
    p.add_argument("arg1", nargs="?", help="CSV path OR asset_name")
    p.add_argument("arg2", nargs="?", help="Output dir (CSV mode) OR URL (single-asset mode)")
    return p.parse_args(argv)

def resolve_mode(args) -> tuple[str, dict]:
    """
    Returns (mode, params)
      mode == "csv"    → params: {"csv_path": Path, "out_dir": Path}
      mode == "single" → params: {"asset": str, "url": str, "out_dir": Path}
    """
    DEFAULT_OUT = Path("scraped_outputs")

    # Flag-driven first
    if args.input:
        csv_path = Path(args.input)
        out_dir  = Path(args.output) if args.output else DEFAULT_OUT
        return "csv", {"csv_path": csv_path, "out_dir": out_dir}

    # Positional-driven
    if args.arg1:
        a1 = args.arg1
        a2 = args.arg2

        # a1 is a CSV path on disk
        p1 = Path(a1)
        if p1.exists() and p1.is_file():
            out_dir = Path(a2) if (a2 and not _looks_like_url(a2)) else (Path(args.output) if args.output else DEFAULT_OUT)
            return "csv", {"csv_path": p1, "out_dir": out_dir}

        # a1 is asset_name, a2 must be URL
        if a2 and _looks_like_url(a2):
            out_dir = Path(args.output) if args.output else DEFAULT_OUT
            return "single", {"asset": a1, "url": a2, "out_dir": out_dir}

        # if a1 looks like CSV by name but not found on disk
        if a1.lower().endswith(".csv"):
            raise SystemExit(f"Input CSV not found: {a1}")

    # If we got here, insufficient args
    raise SystemExit(USAGE_TEXT)

# ─────────────────────────────── main ─────────────────────────────────────
def main() -> None:
    args = parse_args(sys.argv[1:])
    mode, params = resolve_mode(args)

    out_dir: Path = params["out_dir"]
    (out_dir / "json").mkdir(parents=True, exist_ok=True)
    (out_dir / "py").mkdir(parents=True, exist_ok=True)

    rows_iter: List[Dict[str, str]]

    if mode == "csv":
        in_csv: Path = params["csv_path"]
        with in_csv.open(newline="", encoding="utf-8") as fh:
            rdr = csv.DictReader(fh)
            if {"asset_name", "url"} - set(rdr.fieldnames or []):
                log.error("CSV must have headers: asset_name,url")
                return
            rows_iter = list(rdr)
    else:
        # single asset mode
        rows_iter = [{"asset_name": params["asset"], "url": params["url"]}]

    for row in rows_iter:
        asset, orig_url = row["asset_name"].strip(), row["url"].strip()
        if not asset or not orig_url:
            log.error("Skipping row with missing asset_name or url: %s", row)
            continue

        dsid = _dataset_id(orig_url)
        if not dsid:
            log.error("⚠️  cannot extract dataset id from %s", orig_url)
            continue

        host     = urlparse(orig_url).netloc.lower()
        endpoint = _canonical(host, dsid)
        meta     = _get_json(_views_meta_url(host, dsid))
        html     = _get_html(f"https://{host}/d/{dsid}")  # noqa: F841 (html kept for potential future use)

        rows = (
            _rows_from_header(endpoint)
            or _rows_from_meta_v1(host, dsid)
            or _rows_from_meta_v2(meta)
            or _rows_from_catalog(host, dsid)
            or _rows_from_count_query(endpoint)
        )
        if rows is None:
            log.warning("⚠️  could not determine row count for %s", dsid)

        size, (cols, buckets) = _asset_size(rows), _bucket_columns(meta.get("columns", []) or [])
        order_field = f"{random.choice(buckets['date_cols'])} ASC" if buckets['date_cols'] else ""

        # ── write JSON payload ─────────────────────────────────────────
        payload = {
            "title":            meta.get("name"),
            "description":      _strip_html(meta.get("description")),
            "update_frequency": meta.get("metadata", {}).get("updateFrequency"),
            "endpoint":         endpoint[:-5],  # strip .json
            "data_dictionary":  cols,
            "total_columns":    len(cols),
            "rows":             rows,
        }
        (out_dir / "json" / f"{_slug(payload['title'] or asset)}.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False)
        )

        # ── generate stub .py asset file ───────────────────────────────
        rename_var, cfg_var = f"{asset}_rename_map", f"{asset}_schema_cfg"
        imports: List[str]  = []
        if size == "single":
            imports.append("from pipeline.lib.asset_factories import build_single_socrata_asset")
        elif size == "medium":
            imports.append("from pipeline.lib.assets.asset_factories import build_medium_socrata_asset")
        else:
            imports += [
                "from datetime import datetime",
                "from pipeline.lib.asset_factories import build_large_socrata_asset",
            ]

        lines: List[str] = [
            *imports,
            "",
            "from typing import Dict, Any",
            "",
            f'TAG_AUTO = {{"domain": "auto", "source": "{host}"}}',
            "",
            f"{rename_var}: Dict[str, str] = {{}}",
            "",
            f"{cfg_var}: Dict[str, Any] = dict(",
            f"    rename_map = {rename_var},",
            f"    date_cols  = {buckets['date_cols']},",
            f"    int_cols   = {buckets['int_cols']},",
            f"    float_cols = {buckets['float_cols']},",
            f"    bool_cols  = {buckets['bool_cols']},",
            ")",
            "",
        ]

        builder_fn = {
            "single": "build_single_socrata_asset",
            "medium": "build_medium_socrata_asset",
            "large":  "build_large_socrata_asset",
        }[size]

        call = [
            f"{asset}_raw, {asset} = {builder_fn}(",
            f'    asset_name = "{asset}",',
            f'    endpoint   = "{endpoint}",',
            f'    schema_cfg = {cfg_var},',
        ]
        if order_field:
            call.append(f'    order_field = "{order_field}",')
        if size == "large":
            call += [
                "    # TODO: adjust date window",
                "    start_date = datetime(2020, 1, 1),",
                "    end_date   = datetime(2025, 1, 1),",
            ]
        call += [
            "    tags = TAG_AUTO,",
            ")",
        ]
        lines += call

        (out_dir / "py" / f"{asset}.py").write_text("\n".join(lines))
        log.info("✅  generated asset '%s' (size=%s, rows=%s)", asset, size, rows)

if __name__ == "__main__":
    main()
